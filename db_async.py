"""Async MariaDB helpers for the Pappaliiga stats pipeline.

This module provides a single async connection pool built on top of ``asyncmy``.
The helpers are intentionally small and explicit so the sync pipeline can share them
across modules without pulling in ORM magic.

Key guarantees:
- autocommit is disabled; callers must commit explicitly (or use
  ``transaction()`` which handles commit/rollback automatically)
- connections always use ``utf8mb4``
- schema creation and reset helpers live here to keep bootstrapping in
  one place

Connection Pool Sizing:
- DEFAULT_POOL_MAX_SIZE = 30 (increased from 10 to prevent starvation)
- Max concurrency in sync.py is 10 championships in parallel
- Pool size must be >= 3x max concurrency to avoid deadlock during
  complex nested transactions with retry logic
- Each worker may hold multiple connections during transaction retries

Environment:
  DATABASE_URL = mariadb://user:pass@host:3306/database?param=value
Only secrets (user/password) are read from the environment; everything
else is kept in source control.
"""

from __future__ import annotations

import asyncio
import re
import logging
import os
import time
import traceback
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Collection, Dict, Iterable, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, urlparse

import asyncmy
from asyncmy import auth as asyncmy_auth
from asyncmy import cursors, errors as asyncmy_errors

try:
    import faceit_config
except Exception:  # pragma: no cover - config import is optional for tooling
    faceit_config = None

LOGGER = logging.getLogger(__name__)
POOL_LOGGER = logging.getLogger("pappaliiga.db.pool")


def _coerce_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, int):
            return value
        raw = str(value).strip()
        if not raw:
            return default
        return int(raw)
    except (TypeError, ValueError):
        return default

DEFAULT_POOL_MIN_SIZE = 2
DEFAULT_POOL_MAX_SIZE = 30
DEFAULT_POOL_RECYCLE_SECONDS = 300

SCHEMA_PATH = Path(__file__).with_name("mariadb_schema.sql")
DEFAULT_TEAM_AVATAR = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"

_pool: Optional[asyncmy.Pool] = None
_pool_lock = asyncio.Lock()

_POOL_WAIT_WARN_SECONDS = float(os.environ.get("DB_POOL_WAIT_WARN_SECONDS", "2.0"))
_POOL_HOLD_WARN_SECONDS = float(os.environ.get("DB_POOL_HOLD_WARN_SECONDS", "5.0"))
_POOL_WAIT_WARN_THRESHOLD = int(os.environ.get("DB_POOL_WAIT_WARN_THRESHOLD", "5"))


@dataclass(slots=True)
class _PoolLease:
    conn_id: int
    label: str | None
    task: str | None
    acquired_at: float
    stack: str


class _PoolTracker:
    """Track pool usage for temporary diagnostics."""

    # TODO(pipeline-diagnostics): drop this tracker once hangs are resolved.

    def __init__(self) -> None:
        self.enabled = os.environ.get("DB_POOL_DEBUG", "0") != "0"
        self.active: Dict[int, _PoolLease] = {}
        self.events: deque[Dict[str, Any]] = deque(maxlen=500)
        self.waiting = 0

    def _task_name(self) -> str | None:
        task = asyncio.current_task()
        if not task:
            return None
        name = task.get_name()
        return name or f"task-{id(task)}"

    def _stack_snippet(self) -> str:
        snippet = traceback.format_stack(limit=6)
        return "".join(snippet[-5:]).strip()

    def on_wait_start(self, label: str | None) -> None:
        if not self.enabled:
            return
        self.waiting += 1
        evt = {
            "ts": time.time(),
            "event": "wait",
            "label": label,
            "waiting": self.waiting,
            "task": self._task_name(),
        }
        self.events.append(evt)
        POOL_LOGGER.info("POOL wait label=%s waiting=%d task=%s", label, self.waiting, evt["task"])
        if self.waiting >= _POOL_WAIT_WARN_THRESHOLD:
            POOL_LOGGER.warning(
                "POOL wait backlog=%d label=%s task=%s",
                self.waiting,
                label,
                evt["task"],
            )

    def on_acquired(self, conn: asyncmy.Connection, label: str | None) -> None:
        if not self.enabled:
            return
        conn_id = id(conn)
        self.waiting = max(0, self.waiting - 1)
        lease = _PoolLease(
            conn_id=conn_id,
            label=label,
            task=self._task_name(),
            acquired_at=time.time(),
            stack=self._stack_snippet(),
        )
        self.active[conn_id] = lease
        self.events.append(
            {
                "ts": lease.acquired_at,
                "event": "acquire",
                "conn_id": conn_id,
                "label": label,
                "task": lease.task,
                "waiting": self.waiting,
            }
        )
        POOL_LOGGER.info(
            "POOL acquire conn=%s label=%s task=%s active=%d waiting=%d",
            conn_id,
            label,
            lease.task,
            len(self.active),
            self.waiting,
        )

    def on_release(self, conn: asyncmy.Connection, label: str | None) -> None:
        if not self.enabled:
            return
        conn_id = id(conn)
        lease = self.active.pop(conn_id, None)
        released_at = time.time()
        held = released_at - (lease.acquired_at if lease else released_at)
        self.events.append(
            {
                "ts": released_at,
                "event": "release",
                "conn_id": conn_id,
                "label": label,
                "task": lease.task if lease else None,
                "held_seconds": round(held, 3),
                "active": len(self.active),
                "waiting": self.waiting,
            }
        )
        POOL_LOGGER.info(
            "POOL release conn=%s label=%s task=%s held=%.3fs active=%d waiting=%d",
            conn_id,
            label,
            lease.task if lease else None,
            held,
            len(self.active),
            self.waiting,
        )
        if held >= _POOL_HOLD_WARN_SECONDS:
            POOL_LOGGER.warning(
                "POOL long-held conn=%s label=%s task=%s held=%.3fs",
                conn_id,
                label,
                lease.task if lease else None,
                held,
            )

    def snapshot(self) -> Dict[str, Any]:
        if not self.enabled:
            return {"enabled": False}
        now = time.time()
        active = [
            {
                "conn_id": lease.conn_id,
                "label": lease.label,
                "task": lease.task,
                "held_seconds": round(now - lease.acquired_at, 3),
                "stack": lease.stack,
            }
            for lease in self.active.values()
        ]
        return {
            "enabled": True,
            "active": active,
            "waiting": self.waiting,
            "recent_events": list(self.events),
        }


_POOL_TRACKER = _PoolTracker()


@dataclass(slots=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"
    minsize: int = DEFAULT_POOL_MIN_SIZE
    maxsize: int = DEFAULT_POOL_MAX_SIZE
    autocommit: bool = False
    pool_recycle: int = DEFAULT_POOL_RECYCLE_SECONDS
    unix_socket: Optional[str] = None
    ssl: Optional[Dict[str, Any]] = None

    @classmethod
    def from_env(cls, url: Optional[str] = None) -> "DBConfig":
        raw = url or os.environ.get("DATABASE_URL", "").strip()
        if not raw:
            raise RuntimeError("DATABASE_URL environment variable must be set")

        parsed = urlparse(raw)
        scheme = (parsed.scheme or "").lower()
        if scheme not in {"mysql", "mariadb", "mysql+asyncmy", "mariadb+asyncmy"}:
            raise ValueError(f"Unsupported database scheme '{scheme}' in DATABASE_URL")

        if parsed.hostname:
            host = parsed.hostname
        elif parsed.path and parsed.path.startswith("/") and parsed.netloc == "":
            # allow unix socket form: mariadb:///dbname?unix_socket=/path
            host = "localhost"
        else:
            raise ValueError("Database host could not be determined from DATABASE_URL")

        port = parsed.port or 3306
        database = (parsed.path or "/")[1:]  # strip leading slash
        if not database:
            raise ValueError("Database name missing from DATABASE_URL")

        user = parsed.username or ""
        password = parsed.password or ""

        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        charset = query.get("charset", "utf8mb4")
        config_minsize = _coerce_int(
            getattr(faceit_config, "DB_POOL_MIN_SIZE", DEFAULT_POOL_MIN_SIZE)
            if faceit_config
            else DEFAULT_POOL_MIN_SIZE,
            DEFAULT_POOL_MIN_SIZE,
        )
        config_maxsize = _coerce_int(
            getattr(faceit_config, "DB_POOL_MAX_SIZE", DEFAULT_POOL_MAX_SIZE)
            if faceit_config
            else DEFAULT_POOL_MAX_SIZE,
            DEFAULT_POOL_MAX_SIZE,
        )
        minsize = _coerce_int(query.get("minsize"), config_minsize)
        maxsize = _coerce_int(query.get("maxsize"), config_maxsize)
        minsize = _coerce_int(os.environ.get("DB_POOL_MIN_SIZE"), minsize)
        maxsize = _coerce_int(os.environ.get("DB_POOL_MAX_SIZE"), maxsize)
        if minsize <= 0 or maxsize <= 0:
            raise ValueError("Pool sizes must be positive integers")
        if maxsize < minsize:
            maxsize = minsize

        unix_socket = query.get("unix_socket")
        ssl = None
        if "ssl" in query:
            ssl_mode = query["ssl"].lower()
            if ssl_mode in {"1", "true", "require"}:
                ssl = {"cert_reqs": "CERT_REQUIRED"}

        return cls(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset,
            minsize=minsize,
            maxsize=maxsize,
            autocommit=False,
            pool_recycle=int(query.get("pool_recycle", DEFAULT_POOL_RECYCLE_SECONDS)),
            unix_socket=unix_socket,
            ssl=ssl,
        )


async def _native_password_from_gssapi(conn: Any, pkt: Any):
    """Handle servers requesting auth_gssapi_client by falling back to mysql_native_password.

    Many MariaDB installations on Windows default to auth_gssapi_client even when the
    user is configured with a password. Asyncmy does not ship a GSSAPI implementation,
    but we can re-use the mysql_native_password scramble if the server is willing to
    accept it. This mirrors how libmariadb falls back automatically when the password
    plugin is available.
    """

    # If the server sends an auth switch request, capture the salt payload so the
    # scramble below has the correct nonce. Otherwise, assume the connection already
    # populated ``conn.salt`` during the initial handshake.
    try:
        if hasattr(pkt, "is_auth_switch_request") and pkt.is_auth_switch_request():
            conn.salt = pkt.read_all()
    except AttributeError:
        pass

    password = getattr(conn, "_password", None)
    if not password:
        return await asyncmy_auth._roundtrip(conn, b"")

    salt = getattr(conn, "salt", b"")
    scramble = asyncmy_auth.scramble_native_password(password, salt)
    return await asyncmy_auth._roundtrip(conn, scramble)


async def get_pool() -> asyncmy.Pool:
    """Return the global asyncmy pool, creating it lazily."""
    global _pool
    if _pool:
        return _pool

    async with _pool_lock:
        if _pool:
            return _pool
        cfg = DBConfig.from_env()
        LOGGER.info(
            "Creating MariaDB pool host=%s port=%s db=%s min=%s max=%s",
            cfg.host,
            cfg.port,
            cfg.database,
            cfg.minsize,
            cfg.maxsize,
        )
        pool_kwargs = dict(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            db=cfg.database,
            charset=cfg.charset,
            minsize=cfg.minsize,
            maxsize=cfg.maxsize,
            autocommit=cfg.autocommit,
            pool_recycle=cfg.pool_recycle,
            unix_socket=cfg.unix_socket,
            ssl=cfg.ssl,
            auth_plugin_map={"auth_gssapi_client": _native_password_from_gssapi},
        )

        try:
            _pool = await asyncmy.create_pool(**pool_kwargs)
        except asyncmy_errors.OperationalError as exc:
            msg = str(exc)
            if "Authentication plugin" not in msg:
                raise
            LOGGER.warning(
                "Authentication plugin negotiation failed (%s); ensure the MariaDB user is configured with mysql_native_password",
                msg,
            )
            LOGGER.warning(
                "Run: ALTER USER '%s'@'%s' IDENTIFIED WITH mysql_native_password BY '<password>';",
                cfg.user or "<user>",
                cfg.host or "localhost",
            )
            raise
        return _pool


async def close_pool() -> None:
    """Close the global pool (mainly for tests)."""
    global _pool
    if not _pool:
        return
    _pool.close()
    await _pool.wait_closed()
    _pool = None


async def get_pool_snapshot() -> Dict[str, Any]:
    """Return lightweight diagnostics about the asyncmy pool."""
    snapshot = _POOL_TRACKER.snapshot()
    pool = _pool
    if pool:
        snapshot["config"] = {
            "minsize": getattr(pool, "minsize", None),
            "maxsize": getattr(pool, "maxsize", None),
            "size": getattr(pool, "size", None),
            "free": getattr(pool, "free", None),
        }
    else:
        snapshot["config"] = None
    return snapshot


@asynccontextmanager
async def connection(*, label: str | None = None) -> AsyncIterator[asyncmy.Connection]:
    """Acquire a transactional connection with temporary diagnostics logging."""
    pool = await get_pool()
    _POOL_TRACKER.on_wait_start(label)
    conn = await pool.acquire()
    _POOL_TRACKER.on_acquired(conn, label)
    try:
        await conn.begin()
        yield conn
    except Exception:
        await conn.rollback()
        raise
    else:
        await conn.commit()
    finally:
        pool.release(conn)
        _POOL_TRACKER.on_release(conn, label)


@asynccontextmanager
async def readonly_connection(*, label: str | None = None) -> AsyncIterator[asyncmy.Connection]:
    """Acquire a read-only connection (no implicit transaction)."""
    pool = await get_pool()
    _POOL_TRACKER.on_wait_start(label)
    conn = await pool.acquire()
    _POOL_TRACKER.on_acquired(conn, label)
    try:
        yield conn
    finally:
        pool.release(conn)
        _POOL_TRACKER.on_release(conn, label)


async def execute(sql: str, params: Sequence[Any] | Dict[str, Any] | None = None) -> int:
    """Execute a single statement and return the affected-row count."""
    sql_conv, params_conv = _translate_sql(sql, params) if params is not None else (sql, params)
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql_conv, params_conv)
            return cur.rowcount


async def fetch_all(sql: str, params: Sequence[Any] | Dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async with readonly_connection() as conn:
        async with conn.cursor(asyncmy.cursors.DictCursor) as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
            return list(rows)


async def fetch_one(sql: str, params: Sequence[Any] | Dict[str, Any] | None = None) -> Optional[dict[str, Any]]:
    async with readonly_connection() as conn:
        async with conn.cursor(asyncmy.cursors.DictCursor) as cur:
            await cur.execute(sql, params)
            row = await cur.fetchone()
            return dict(row) if row else None


async def fetch_val(sql: str, params: Sequence[Any] | Dict[str, Any] | None = None, default: Any = None) -> Any:
    row = await fetch_one(sql, params)
    if not row:
        return default
    return next(iter(row.values())) if row else default


async def _run_script(conn: asyncmy.Connection, sql: str) -> None:
    statements = [chunk.strip() for chunk in sql.split(";") if chunk.strip()]
    async with conn.cursor() as cur:
        for stmt in statements:
            await cur.execute(stmt)


async def _ensure_table_columns_async(
    conn: Any,
    table: str,
    columns: dict[str, str],
) -> None:
    async def _kill_metadata_lock_blockers() -> int:
        killed = 0
        current_id = conn.thread_id() if hasattr(conn, "thread_id") else None
        def _field(proc: Any, idx: int, key: str, default: Any = None) -> Any:
            if isinstance(proc, Mapping):
                return proc.get(key, default)
            try:
                return proc[idx]
            except Exception:
                return default

        async with conn.cursor() as kill_cur:
            await kill_cur.execute("SELECT DATABASE()")
            dbname = (await kill_cur.fetchone() or ("",))[0]
            await kill_cur.execute("SHOW FULL PROCESSLIST")
            processes = await kill_cur.fetchall()
            candidates: list[tuple[Any, str, str, str, str, str]] = []
            for proc in processes:
                proc_id = _field(proc, 0, "Id")
                user = str(_field(proc, 1, "User", "") or "")
                db = _field(proc, 3, "db")
                command = str(_field(proc, 4, "Command", "") or "")
                state = str(_field(proc, 6, "State", "") or "").lower()
                info = str(_field(proc, 7, "Info", "") or "").lower()
                if current_id is not None and proc_id == current_id:
                    continue
                if dbname and db != dbname:
                    continue
                if command in {"Binlog Dump", "Daemon"}:
                    continue
                if user.lower() in {"system user", "event_scheduler"}:
                    continue
                looks_like_blocker = (
                    "metadata lock" in state
                    or (f"`{table}`".lower() in info)
                    or (f" {table} " in f" {info} ")
                )
                candidates.append((proc_id, user, str(db), command, state, info))
                if looks_like_blocker:
                    try:
                        await kill_cur.execute(f"KILL {int(proc_id)}")
                        killed += 1
                        LOGGER.warning("Killed blocking process %s while migrating %s", proc_id, table)
                    except Exception as kill_exc:
                        LOGGER.warning("Failed to kill blocking process %s: %s", proc_id, kill_exc)
            # If we still did not kill anything, fall back to aggressive mode:
            # terminate all non-system sessions in this DB and retry ALTER.
            if killed == 0 and candidates:
                LOGGER.warning(
                    "No explicit metadata-lock blocker found for %s; killing %d candidate session(s) in db %s",
                    table,
                    len(candidates),
                    dbname,
                )
                for proc_id, _user, _db, _cmd, _state, _info in candidates:
                    try:
                        await kill_cur.execute(f"KILL {int(proc_id)}")
                        killed += 1
                        LOGGER.warning("Killed candidate process %s while migrating %s", proc_id, table)
                    except Exception as kill_exc:
                        LOGGER.warning("Failed to kill candidate process %s: %s", proc_id, kill_exc)
        if killed:
            await conn.commit()
        return killed

    async with conn.cursor() as cur:
        # Fail fast instead of hanging on metadata locks during ALTER TABLE.
        try:
            await cur.execute("SET SESSION lock_wait_timeout = 5")
            await cur.execute("SET SESSION innodb_lock_wait_timeout = 5")
        except Exception:
            pass
        await cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (table,),
        )
        existing = {row[0] for row in await cur.fetchall()}
        missing = [(name, ddl) for name, ddl in columns.items() if name not in existing]
        for name, ddl in missing:
            LOGGER.info("Adding missing column %s.%s", table, name)
            started = time.perf_counter()
            try:
                await cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{name}` {ddl}")
            except Exception as exc:
                message = str(exc).lower()
                is_lock = ("lock wait timeout" in message) or ("metadata lock" in message)
                if is_lock:
                    LOGGER.warning(
                        "Metadata lock while adding %s.%s; attempting to kill blockers and retry once",
                        table,
                        name,
                    )
                    killed = await _kill_metadata_lock_blockers()
                    if killed > 0:
                        try:
                            await cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{name}` {ddl}")
                        except Exception as retry_exc:
                            elapsed = time.perf_counter() - started
                            LOGGER.error(
                                "Retry failed adding column %s.%s after %.2fs: %s",
                                table,
                                name,
                                elapsed,
                                retry_exc,
                            )
                            raise
                        else:
                            elapsed = time.perf_counter() - started
                            LOGGER.info("Added column %s.%s in %.2fs after killing blockers", table, name, elapsed)
                            continue
                elapsed = time.perf_counter() - started
                LOGGER.error(
                    "Failed adding column %s.%s after %.2fs (likely metadata lock or insufficient privileges): %s",
                    table,
                    name,
                    elapsed,
                    exc,
                )
                raise
            else:
                elapsed = time.perf_counter() - started
                LOGGER.info("Added column %s.%s in %.2fs", table, name, elapsed)
    if missing:
        await conn.commit()


async def _ensure_table_indexes_async(
    conn: Any,
    table: str,
    indexes: dict[str, str],
) -> None:
    async def _kill_metadata_lock_blockers() -> int:
        killed = 0
        current_id = conn.thread_id() if hasattr(conn, "thread_id") else None
        def _field(proc: Any, idx: int, key: str, default: Any = None) -> Any:
            if isinstance(proc, Mapping):
                return proc.get(key, default)
            try:
                return proc[idx]
            except Exception:
                return default

        async with conn.cursor() as kill_cur:
            await kill_cur.execute("SELECT DATABASE()")
            dbname = (await kill_cur.fetchone() or ("",))[0]
            await kill_cur.execute("SHOW FULL PROCESSLIST")
            processes = await kill_cur.fetchall()
            candidates: list[tuple[Any, str, str, str, str, str]] = []
            for proc in processes:
                proc_id = _field(proc, 0, "Id")
                user = str(_field(proc, 1, "User", "") or "")
                db = _field(proc, 3, "db")
                command = str(_field(proc, 4, "Command", "") or "")
                state = str(_field(proc, 6, "State", "") or "").lower()
                info = str(_field(proc, 7, "Info", "") or "").lower()
                if current_id is not None and proc_id == current_id:
                    continue
                if dbname and db != dbname:
                    continue
                if command in {"Binlog Dump", "Daemon"}:
                    continue
                if user.lower() in {"system user", "event_scheduler"}:
                    continue
                looks_like_blocker = (
                    "metadata lock" in state
                    or (f"`{table}`".lower() in info)
                    or (f" {table} " in f" {info} ")
                )
                candidates.append((proc_id, user, str(db), command, state, info))
                if looks_like_blocker:
                    try:
                        await kill_cur.execute(f"KILL {int(proc_id)}")
                        killed += 1
                        LOGGER.warning("Killed blocking process %s while indexing %s", proc_id, table)
                    except Exception as kill_exc:
                        LOGGER.warning("Failed to kill blocking process %s: %s", proc_id, kill_exc)
            if killed == 0 and candidates:
                LOGGER.warning(
                    "No explicit metadata-lock blocker found for %s indexes; killing %d candidate session(s) in db %s",
                    table,
                    len(candidates),
                    dbname,
                )
                for proc_id, _user, _db, _cmd, _state, _info in candidates:
                    try:
                        await kill_cur.execute(f"KILL {int(proc_id)}")
                        killed += 1
                        LOGGER.warning("Killed candidate process %s while indexing %s", proc_id, table)
                    except Exception as kill_exc:
                        LOGGER.warning("Failed to kill candidate process %s: %s", proc_id, kill_exc)
        if killed:
            await conn.commit()
        return killed

    async with conn.cursor() as cur:
        try:
            await cur.execute("SET SESSION lock_wait_timeout = 5")
            await cur.execute("SET SESSION innodb_lock_wait_timeout = 5")
        except Exception:
            pass
        await cur.execute(
            """
            SELECT DISTINCT index_name
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = %s
            """,
            (table,),
        )
        existing = {row[0] for row in await cur.fetchall()}
        missing = [(name, ddl) for name, ddl in indexes.items() if name not in existing]
        for name, ddl in missing:
            LOGGER.info("Adding missing index %s.%s", table, name)
            started = time.perf_counter()
            try:
                await cur.execute(f"ALTER TABLE `{table}` ADD INDEX `{name}` {ddl}")
            except Exception as exc:
                message = str(exc).lower()
                is_lock = ("lock wait timeout" in message) or ("metadata lock" in message)
                if is_lock:
                    LOGGER.warning(
                        "Metadata lock while adding index %s.%s; attempting to kill blockers and retry once",
                        table,
                        name,
                    )
                    killed = await _kill_metadata_lock_blockers()
                    if killed > 0:
                        try:
                            await cur.execute(f"ALTER TABLE `{table}` ADD INDEX `{name}` {ddl}")
                        except Exception as retry_exc:
                            elapsed = time.perf_counter() - started
                            LOGGER.error(
                                "Retry failed adding index %s.%s after %.2fs: %s",
                                table,
                                name,
                                elapsed,
                                retry_exc,
                            )
                            raise
                        else:
                            elapsed = time.perf_counter() - started
                            LOGGER.info("Added index %s.%s in %.2fs after killing blockers", table, name, elapsed)
                            continue
                elapsed = time.perf_counter() - started
                LOGGER.error(
                    "Failed adding index %s.%s after %.2fs (likely metadata lock): %s",
                    table,
                    name,
                    elapsed,
                    exc,
                )
                raise
            else:
                elapsed = time.perf_counter() - started
                LOGGER.info("Added index %s.%s in %.2fs", table, name, elapsed)
    if missing:
        await conn.commit()


async def create_schema_async(force: bool = False) -> None:
    """Create tables if they are missing.

    When ``force`` is True the schema file is executed even if tables already
    exist (useful when adding new tables mid-stream).
    """
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Schema file missing: {SCHEMA_PATH}")

    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = 'championships'
                """
            )
            existing = (await cur.fetchone() or (0,))[0]
        if existing and not force:
            LOGGER.info("Schema already present; skipping creation")
        else:
            LOGGER.info("Creating database schema from %s", SCHEMA_PATH)
            sql = SCHEMA_PATH.read_text(encoding="utf-8")
            await _run_script(conn, sql)

        # New tables may be added after the initial bootstrap; ensure they exist
        # even when the base schema creation is skipped.
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS player_championships (
                    player_id VARCHAR(64) NOT NULL,
                    championship_id VARCHAR(64) NOT NULL,
                    player_name VARCHAR(255) NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (player_id, championship_id),
                    KEY idx_player_championships_championship (championship_id),
                    CONSTRAINT fk_player_championships_player FOREIGN KEY (player_id)
                        REFERENCES players (player_id) ON DELETE CASCADE ON UPDATE CASCADE,
                    CONSTRAINT fk_player_championships_championship FOREIGN KEY (championship_id)
                        REFERENCES championships (championship_id) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS championship_team_statuses (
                    championship_id VARCHAR(64) NOT NULL,
                    team_id VARCHAR(64) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    effective_at BIGINT(20) NULL,
                    reason VARCHAR(255) NULL,
                    note TEXT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (championship_id, team_id),
                    KEY idx_championship_team_statuses_team (team_id),
                    KEY idx_championship_team_statuses_status (status),
                    KEY idx_championship_team_statuses_effective (effective_at),
                    CONSTRAINT fk_championship_team_statuses_championship FOREIGN KEY (championship_id)
                        REFERENCES championships (championship_id) ON DELETE CASCADE ON UPDATE CASCADE,
                    CONSTRAINT fk_championship_team_statuses_team FOREIGN KEY (team_id)
                        REFERENCES teams (team_id) ON DELETE CASCADE ON UPDATE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
            )
        await conn.commit()

        player_totals_columns = {
            "headshots": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "utility_count": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "utility_successes": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "utility_enemies": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "knife_kills": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "zeus_kills": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
            "first_kills": "INT(10) UNSIGNED NOT NULL DEFAULT 0",
        }
        player_columns = {
            "avatar": "VARCHAR(512) NULL",
            "faceit_url": "VARCHAR(512) NULL",
        }
        match_columns = {
            "payload_hash": "CHAR(64) NULL",
        }
        await _ensure_table_columns_async(conn, "matches", match_columns)
        try:
            await _ensure_table_columns_async(conn, "players", player_columns)
        except asyncmy_errors.OperationalError as exc:
            code = exc.args[0] if exc.args else 0
            if code == 1205:
                LOGGER.warning(
                    "Skipping players column migration due to lock timeout. Sync can continue with fallback SQL."
                )
            else:
                raise
        await _ensure_table_columns_async(conn, "player_season_totals", player_totals_columns)

        match_indexes = {
            "idx_matches_season_division_team1_finished": "(season, division_num, team1_id, finished_at)",
            "idx_matches_season_division_team2_finished": "(season, division_num, team2_id, finished_at)",
        }
        player_stats_indexes = {
            "idx_player_stats_player_match_round": "(player_id, match_id, round_index)",
        }
        team_stats_indexes = {
            "idx_team_stats_team_match_round": "(team_id, match_id, round_index)",
        }
        await _ensure_table_indexes_async(conn, "matches", match_indexes)
        await _ensure_table_indexes_async(conn, "player_stats", player_stats_indexes)
        await _ensure_table_indexes_async(conn, "team_stats", team_stats_indexes)


async def reset_db_async(confirm: bool = False) -> None:
    """Drop all tables in the current database (does NOT recreate - call create_schema_async separately)."""
    if not confirm:
        raise RuntimeError("reset_db_async called without confirm=True; aborting to keep data safe")

    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT DATABASE()")
            dbname = (await cur.fetchone() or ("",))[0]
            LOGGER.warning("Dropping all tables from %s", dbname)
            
            # Kill any other connections that might be holding locks
            await cur.execute("SHOW PROCESSLIST")
            processes = await cur.fetchall()
            current_id = conn.thread_id()
            for proc_id, user, host, db, *_ in processes:
                if proc_id != current_id and db == dbname:
                    try:
                        await cur.execute(f"KILL {proc_id}")
                        LOGGER.info("Killed process %s to release locks", proc_id)
                    except Exception as e:
                        LOGGER.warning("Could not kill process %s: %s", proc_id, e)
            
            await cur.execute("SET FOREIGN_KEY_CHECKS=0")
            await cur.execute("SET lock_wait_timeout=5")
            await cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
            )
            tables = [row[0] for row in await cur.fetchall()]
            if tables:
                LOGGER.info("Found %d tables to drop: %s", len(tables), ", ".join(tables))
                LOGGER.info("Dropping tables...")
                # Drop tables one by one with explicit error handling to avoid hangs
                for table in tables:
                    try:
                        LOGGER.debug("Dropping table %s", table)
                        await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
                        LOGGER.debug("Successfully dropped table %s", table)
                    except Exception as e:
                        LOGGER.error("Failed to drop table %s: %s", table, e)
                        raise
                LOGGER.info("Successfully dropped %d tables", len(tables))
            else:
                LOGGER.info("No tables found to drop")
            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
        await conn.commit()
        LOGGER.info("All tables dropped from %s", dbname)


# ---------------------------------------------------------------------------
# Lightweight query helpers (consolidated from async_db.py)
# ---------------------------------------------------------------------------

def _translate_sql(sql: str, params: Any) -> tuple[str, Any]:
    """Convert named parameters to MariaDB placeholders."""
    if isinstance(params, dict):
        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in params:
                raise KeyError(f"SQL parameter '{key}' missing from params")
            return f"%({key})s"
        return re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", repl, sql), params

    if isinstance(params, (list, tuple)):
        return sql.replace("?", "%s"), params

    return sql, params


async def query_async(sql: str, params: Any = None) -> list[dict]:
    """Execute a read-only query and return dictionaries."""
    sql_conv, params_conv = _translate_sql(sql, params)
    rows = await fetch_all(sql_conv, params_conv)
    return [dict(row) for row in rows]


def _prepare_excluded(
    excluded: Collection[str] | None,
    ignore: Iterable[str] = (),
) -> list[str]:
    """Normalise a collection of team IDs for exclusion clauses."""

    ignore_set = {str(tid) for tid in ignore if tid}
    seen: set[str] = set()
    prepared: list[str] = []
    for tid in excluded or []:
        if not tid:
            continue
        tid_str = str(tid)
        if tid_str in ignore_set or tid_str in seen:
            continue
        seen.add(tid_str)
        prepared.append(tid_str)
    return prepared


def _build_exclusion_clause(
    excluded: Collection[str] | None,
    alias: str = "m",
    param_prefix: str = "ex",
) -> tuple[str, dict[str, str]]:
    """Return a SQL snippet that excludes matches involving ``excluded`` team IDs."""

    ids = _prepare_excluded(excluded)
    if not ids:
        return "", {}

    placeholders = ", ".join(f":{param_prefix}{idx}" for idx in range(len(ids)))
    clause = (
        f" AND {alias}.team1_id NOT IN ({placeholders})"
        f" AND {alias}.team2_id NOT IN ({placeholders})"
    )
    params = {f"{param_prefix}{idx}": tid for idx, tid in enumerate(ids)}
    return clause, params


def _build_allmaps_cte(all_maps: Sequence[str]) -> tuple[str, dict[str, str]]:
    """Return SQL and parameters for a CTE enumerating map identifiers."""

    selects: list[str] = []
    params: dict[str, str] = {}
    for idx, map_name in enumerate(all_maps):
        alias = " AS map" if idx == 0 else ""
        selects.append(f"SELECT :map{idx}{alias}")
        params[f"map{idx}"] = map_name
    return " UNION ALL ".join(selects), params


def build_played_match_condition(
    *,
    alias: str = "m",
    include_forfeits: bool = True,
    include_ignored: bool = True,
) -> str:
    """Return SQL condition for matches that should count as played."""
    conditions = [f"NULLIF({alias}.finished_at, 0) IS NOT NULL"]
    if not include_forfeits:
        conditions.append(f"COALESCE({alias}.is_forfeit, 0) = 0")
    if not include_ignored:
        conditions.append(f"COALESCE({alias}.ignored_due_ban, 0) = 0")
    return " AND ".join(conditions)


def build_match_scope_clause(
    *,
    alias: str = "m",
    season: int | None = None,
    division_num: int | None = None,
    championship_id: str | None = None,
    team_id: str | None = None,
    param_prefix: str = "",
) -> tuple[str, dict[str, Any]]:
    """Return SQL/params for scoping matches to season/division/championship/team."""
    conditions: list[str] = []
    params: dict[str, Any] = {}

    def _param(name: str) -> str:
        return f"{param_prefix}{name}" if param_prefix else name

    if season is not None:
        key = _param("season")
        conditions.append(f"{alias}.season = :{key}")
        params[key] = season
    if division_num is not None:
        key = _param("division_num")
        conditions.append(f"{alias}.division_num = :{key}")
        params[key] = division_num
    if championship_id is not None:
        key = _param("championship_id")
        conditions.append(f"{alias}.championship_id = :{key}")
        params[key] = championship_id
    if team_id is not None:
        key = _param("team_id")
        conditions.append(f"({alias}.team1_id = :{key} OR {alias}.team2_id = :{key})")
        params[key] = team_id

    return " AND ".join(conditions), params


async def list_championship_team_statuses(
    championship_id: str,
    *,
    include_active: bool = False,
) -> list[dict[str, Any]]:
    where_clauses = ["cts.championship_id = :championship_id"]
    params: dict[str, Any] = {"championship_id": championship_id}
    if not include_active:
        where_clauses.append("LOWER(COALESCE(cts.status, 'active')) <> 'active'")

    return await fetch_all(
        f"""
        SELECT
            cts.championship_id,
            cts.team_id,
            LOWER(COALESCE(cts.status, 'active')) AS status,
            cts.effective_at,
            cts.reason,
            cts.note,
            COALESCE(tc.team_name, t.name) AS team_name,
            t.avatar,
            cts.created_at,
            cts.updated_at
        FROM championship_team_statuses cts
        LEFT JOIN teams t ON t.team_id = cts.team_id
        LEFT JOIN team_championships tc
            ON tc.team_id = cts.team_id
           AND tc.championship_id = cts.championship_id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY COALESCE(cts.effective_at, 0) DESC, cts.team_id ASC
        """,
        params,
    )


async def upsert_championship_team_status(
    championship_id: str,
    team_id: str,
    *,
    status: str,
    effective_at: int | None = None,
    reason: str | None = None,
    note: str | None = None,
) -> int:
    normalized_status = (status or "active").strip().lower() or "active"
    return await execute(
        """
        INSERT INTO championship_team_statuses (
            championship_id,
            team_id,
            status,
            effective_at,
            reason,
            note
        )
        VALUES (
            :championship_id,
            :team_id,
            :status,
            :effective_at,
            :reason,
            :note
        )
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            effective_at = VALUES(effective_at),
            reason = VALUES(reason),
            note = VALUES(note)
        """,
        {
            "championship_id": championship_id,
            "team_id": team_id,
            "status": normalized_status,
            "effective_at": effective_at,
            "reason": reason,
            "note": note,
        },
    )


async def delete_championship_team_status(championship_id: str, team_id: str) -> int:
    return await execute(
        """
        DELETE FROM championship_team_statuses
        WHERE championship_id = :championship_id
          AND team_id = :team_id
        """,
        {"championship_id": championship_id, "team_id": team_id},
    )


async def count_played_matches(
    *,
    season: int | None = None,
    division_num: int | None = None,
    championship_id: str | None = None,
    team_id: str | None = None,
    player_id: str | None = None,
    is_playoff: bool | None = None,
    parent_championship_id: str | None = None,
    include_forfeits: bool = True,
    include_ignored: bool = True,
    include_forfeit_maps: bool = False,
) -> int:
    """Count played matches with optional season/division/championship/team/player/playoff scope."""
    where = [build_played_match_condition(
        alias="m",
        include_forfeits=include_forfeits,
        include_ignored=include_ignored,
    )]
    scope_clause, params = build_match_scope_clause(
        alias="m",
        season=season,
        division_num=division_num,
        championship_id=championship_id,
        team_id=team_id,
    )
    if scope_clause:
        where.append(scope_clause)

    join_parts: list[str] = []
    if is_playoff is not None or parent_championship_id is not None:
        join_parts.append("JOIN championships c ON c.championship_id = m.championship_id")
        if is_playoff is not None:
            where.append("c.is_playoffs = :is_playoff")
            params["is_playoff"] = int(is_playoff)
        if parent_championship_id is not None:
            where.append("c.parent_championship_id = :parent_championship_id")
            params["parent_championship_id"] = parent_championship_id

    if player_id is not None:
        join_clause = "JOIN player_stats ps ON ps.match_id = m.match_id"
        if not include_forfeit_maps:
            join_clause += " AND COALESCE(ps.is_forfeit_map, 0) = 0"
        join_parts.append(join_clause)
        where.append("ps.player_id = :player_id")
        params["player_id"] = player_id

    join_clause = " ".join(join_parts)

    sql = (
        "SELECT COUNT(DISTINCT m.match_id) AS matches_played "
        "FROM matches m "
        f"{join_clause} "
        "WHERE "
        + " AND ".join(where)
    )
    rows = await query_async(sql, params)
    return int(rows[0].get("matches_played") or 0) if rows else 0


async def count_played_matches_by_season(
    *,
    seasons: Sequence[int] | None = None,
    include_forfeits: bool = True,
    include_ignored: bool = True,
) -> dict[int, int]:
    """Return played match counts per season."""
    condition = build_played_match_condition(
        alias="m",
        include_forfeits=include_forfeits,
        include_ignored=include_ignored,
    )
    params: dict[str, Any] = {}
    where = [condition]

    season_list: list[int] = []
    if seasons is not None:
        seen: set[int] = set()
        for s in seasons:
            try:
                season_int = int(s)
            except (TypeError, ValueError):
                continue
            if season_int in seen:
                continue
            seen.add(season_int)
            season_list.append(season_int)
        if not season_list:
            return {}
        placeholders = ", ".join(f":season{i}" for i in range(len(season_list)))
        where.append(f"m.season IN ({placeholders})")
        params.update({f"season{i}": s for i, s in enumerate(season_list)})

    sql = (
        "SELECT m.season, COUNT(DISTINCT m.match_id) AS matches_played "
        "FROM matches m "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY m.season"
    )
    rows = await query_async(sql, params)
    results = {int(row["season"]): int(row.get("matches_played") or 0) for row in rows}
    if seasons is not None:
        return {s: results.get(s, 0) for s in season_list}
    return results


async def count_played_matches_by_championship_ids(
    *,
    championship_ids: Sequence[str],
    include_forfeits: bool = True,
    include_ignored: bool = True,
) -> dict[str, int]:
    """Return played match counts per championship ID."""
    ids: list[str] = []
    seen: set[str] = set()
    for cid in championship_ids or []:
        if not cid:
            continue
        cid_str = str(cid)
        if cid_str in seen:
            continue
        seen.add(cid_str)
        ids.append(cid_str)

    if not ids:
        return {}

    condition = build_played_match_condition(
        alias="m",
        include_forfeits=include_forfeits,
        include_ignored=include_ignored,
    )
    placeholders = ", ".join(f":cid{i}" for i in range(len(ids)))
    params = {f"cid{i}": cid for i, cid in enumerate(ids)}

    sql = (
        "SELECT m.championship_id, COUNT(DISTINCT m.match_id) AS matches_played "
        "FROM matches m "
        f"WHERE {condition} AND m.championship_id IN ({placeholders}) "
        "GROUP BY m.championship_id"
    )
    rows = await query_async(sql, params)
    results = {str(row["championship_id"]): int(row.get("matches_played") or 0) for row in rows}
    return {cid: results.get(cid, 0) for cid in ids}


async def count_total_matches_by_championship_ids(
    *,
    championship_ids: Sequence[str],
) -> dict[str, int]:
    """Return total match counts per championship ID (includes all matches)."""
    ids: list[str] = []
    seen: set[str] = set()
    for cid in championship_ids or []:
        if not cid:
            continue
        cid_str = str(cid)
        if cid_str in seen:
            continue
        seen.add(cid_str)
        ids.append(cid_str)

    if not ids:
        return {}

    placeholders = ", ".join(f":cid{i}" for i in range(len(ids)))
    params = {f"cid{i}": cid for i, cid in enumerate(ids)}
    sql = (
        "SELECT m.championship_id, COUNT(*) AS matches_total "
        "FROM matches m "
        f"WHERE m.championship_id IN ({placeholders}) "
        "GROUP BY m.championship_id"
    )
    rows = await query_async(sql, params)
    results = {str(row["championship_id"]): int(row.get("matches_total") or 0) for row in rows}
    return {cid: results.get(cid, 0) for cid in ids}


async def count_played_matches_by_season_and_playoff(
    *,
    seasons: Sequence[int] | None = None,
    include_forfeits: bool = True,
    include_ignored: bool = True,
) -> dict[tuple[int, int], int]:
    """Return played match counts per (season, is_playoff)."""
    condition = build_played_match_condition(
        alias="m",
        include_forfeits=include_forfeits,
        include_ignored=include_ignored,
    )
    params: dict[str, Any] = {}
    where = [condition]

    season_list: list[int] = []
    if seasons is not None:
        seen: set[int] = set()
        for s in seasons:
            try:
                season_int = int(s)
            except (TypeError, ValueError):
                continue
            if season_int in seen:
                continue
            seen.add(season_int)
            season_list.append(season_int)
        if not season_list:
            return {}
        placeholders = ", ".join(f":season{i}" for i in range(len(season_list)))
        where.append(f"m.season IN ({placeholders})")
        params.update({f"season{i}": s for i, s in enumerate(season_list)})

    sql = (
        "SELECT m.season, c.is_playoffs AS is_playoff, "
        "COUNT(DISTINCT m.match_id) AS matches_played "
        "FROM matches m "
        "JOIN championships c ON c.championship_id = m.championship_id "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY m.season, c.is_playoffs"
    )
    rows = await query_async(sql, params)
    return {
        (int(row["season"]), int(row.get("is_playoff") or 0)): int(row.get("matches_played") or 0)
        for row in rows
    }


# ---------------------------------------------------------------------------
# Former db_ops_async helpers and operations
# ---------------------------------------------------------------------------

Row = Mapping[str, Any]


@asynccontextmanager
async def _write_connection(
    conn: asyncmy.Connection | None,
    *,
    label: str,
) -> AsyncIterator[asyncmy.Connection]:
    """Return provided connection or acquire a new transactional connection."""
    if conn is not None:
        yield conn
        return
    async with connection(label=label) as owned_conn:
        yield owned_conn


def _is_retryable_error(exc: Exception) -> bool:
    """Check if database exception is retryable (deadlock or lock timeout)."""
    if isinstance(exc, asyncmy_errors.OperationalError):
        error_code = exc.args[0] if exc.args else 0
        # 1213 = Deadlock, 1205 = Lock wait timeout, 1020 = record changed since last read
        return error_code in (1213, 1205, 1020)
    return False


async def _retry_on_deadlock(
    operation,
    *,
    label: str,
    max_attempts: int = 3,
    base_delay: float = 0.1,
) -> Any:
    """
    Retry a coroutine function if it encounters a deadlock or lock timeout.
    Uses exponential backoff with jitter and logs each attempt for diagnostics.
    """
    import random

    LOGGER.debug("retry[%s] start max_attempts=%d", label, max_attempts)
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = await operation()
            LOGGER.debug("retry[%s] attempt %d succeeded", label, attempt)
            return result
        except Exception as exc:
            last_exc = exc
            retryable = _is_retryable_error(exc)
            if attempt < max_attempts and retryable:
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                LOGGER.warning(
                    "retry[%s] attempt %d/%d hit %s (%s); sleeping %.2fs",
                    label,
                    attempt,
                    max_attempts,
                    exc.__class__.__name__,
                    getattr(exc, "args", exc),
                    delay,
                )
                LOGGER.debug("retry[%s] sleep-start attempt=%d delay=%.2fs", label, attempt, delay)
                await asyncio.sleep(delay)
                LOGGER.debug("retry[%s] sleep-end attempt=%d", label, attempt)
                continue
            LOGGER.error(
                "retry[%s] giving up after attempt %d/%d (retryable=%s): %s",
                label,
                attempt,
                max_attempts,
                retryable,
                exc,
            )
            raise
    if last_exc:
        raise last_exc
    return None


def _normalize_id_list(values: Sequence[Any]) -> list[str]:
    """Return de-duplicated non-empty IDs as strings while preserving order."""
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value is None:
            continue
        item = str(value).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out

_TS_EXPR = (
    "COALESCE(m.finished_at, m.started_at, m.scheduled_at, m.configured_at, m.last_seen_at, 0)"
)

_CHAMPIONSHIP_UPSERT_SQL = """
    INSERT INTO championships (championship_id, season, division_num, name, is_playoffs, slug, parent_championship_id)
    VALUES (%(championship_id)s, %(season)s, %(division_num)s, %(name)s, %(is_playoffs)s, %(slug)s, %(parent_championship_id)s)
    ON DUPLICATE KEY UPDATE
      season = VALUES(season),
      division_num = VALUES(division_num),
      name = CASE WHEN championships.name = '' THEN VALUES(name) ELSE championships.name END,
      is_playoffs = VALUES(is_playoffs),
      slug = CASE WHEN championships.slug = '' THEN VALUES(slug) ELSE championships.slug END,
      parent_championship_id = VALUES(parent_championship_id)
"""

_TEAM_UPSERT_SQL = """
    INSERT INTO teams (team_id, name, avatar)
    VALUES (%(team_id)s, %(name)s, %(avatar)s)
    ON DUPLICATE KEY UPDATE
      name = CASE WHEN VALUES(name) <> '' THEN VALUES(name) ELSE teams.name END,
      avatar = CASE WHEN VALUES(avatar) <> '' THEN VALUES(avatar) ELSE teams.avatar END
"""

_MAP_UPSERT_SQL = """
    INSERT INTO maps (
      match_id, season, division_num, round_index, map_name,
      score_team1, score_team2, winner_team_id, is_forfeit
    )
    VALUES (
      %(match_id)s, %(season)s, %(division_num)s, %(round_index)s, %(map_name)s,
      %(score_team1)s, %(score_team2)s, %(winner_team_id)s, %(is_forfeit)s
    )
    ON DUPLICATE KEY UPDATE
      map_name = VALUES(map_name),
      score_team1 = VALUES(score_team1),
      score_team2 = VALUES(score_team2),
      winner_team_id = VALUES(winner_team_id),
      is_forfeit = VALUES(is_forfeit)
"""

_PLAYER_STAT_UPSERT_SQL = """
        INSERT INTO player_stats (
            season, division_num, match_id, round_index, map_id, player_id, team_id, opponent_team_id,
            is_forfeit_map, kills, deaths, assists, mvps, headshots, damage,
            sniper_kills, pistol_kills, knife_kills, zeus_kills, first_kills,
            enemies_flashed, flash_count, flash_successes, utility_damage,
            utility_count, utility_successes, utility_enemies,
            mk_2k, mk_3k, mk_4k, mk_5k,
            clutch_kills, cl_1v1_attempts, cl_1v1_wins, cl_1v2_attempts, cl_1v2_wins,
            entry_count, entry_wins,
            kd, kr, adr, hs_pct, result
        )
        VALUES (
            %(season)s, %(division_num)s, %(match_id)s, %(round_index)s, %(map_id)s, %(player_id)s, %(team_id)s, %(opponent_team_id)s,
            %(is_forfeit_map)s, %(kills)s, %(deaths)s, %(assists)s, %(mvps)s, %(headshots)s, %(damage)s,
            %(sniper_kills)s, %(pistol_kills)s, %(knife_kills)s, %(zeus_kills)s, %(first_kills)s,
            %(enemies_flashed)s, %(flash_count)s, %(flash_successes)s, %(utility_damage)s,
            %(utility_count)s, %(utility_successes)s, %(utility_enemies)s,
            %(mk_2k)s, %(mk_3k)s, %(mk_4k)s, %(mk_5k)s,
            %(clutch_kills)s, %(cl_1v1_attempts)s, %(cl_1v1_wins)s, %(cl_1v2_attempts)s, %(cl_1v2_wins)s,
            %(entry_count)s, %(entry_wins)s,
            %(kd)s, %(kr)s, %(adr)s, %(hs_pct)s, %(result)s
        )
        ON DUPLICATE KEY UPDATE
            team_id = VALUES(team_id),
            opponent_team_id = VALUES(opponent_team_id),
            is_forfeit_map = VALUES(is_forfeit_map),
            kills = VALUES(kills),
            deaths = VALUES(deaths),
            assists = VALUES(assists),
            mvps = VALUES(mvps),
            headshots = VALUES(headshots),
            damage = VALUES(damage),
            sniper_kills = VALUES(sniper_kills),
            pistol_kills = VALUES(pistol_kills),
            knife_kills = VALUES(knife_kills),
            zeus_kills = VALUES(zeus_kills),
            first_kills = VALUES(first_kills),
            enemies_flashed = VALUES(enemies_flashed),
            flash_count = VALUES(flash_count),
            flash_successes = VALUES(flash_successes),
            utility_damage = VALUES(utility_damage),
            utility_count = VALUES(utility_count),
            utility_successes = VALUES(utility_successes),
            utility_enemies = VALUES(utility_enemies),
            mk_2k = VALUES(mk_2k),
            mk_3k = VALUES(mk_3k),
            mk_4k = VALUES(mk_4k),
            mk_5k = VALUES(mk_5k),
            clutch_kills = VALUES(clutch_kills),
            cl_1v1_attempts = VALUES(cl_1v1_attempts),
            cl_1v1_wins = VALUES(cl_1v1_wins),
            cl_1v2_attempts = VALUES(cl_1v2_attempts),
            cl_1v2_wins = VALUES(cl_1v2_wins),
            entry_count = VALUES(entry_count),
            entry_wins = VALUES(entry_wins),
            kd = VALUES(kd),
            kr = VALUES(kr),
            adr = VALUES(adr),
            hs_pct = VALUES(hs_pct),
            result = VALUES(result)
"""

_TEAM_STAT_UPSERT_SQL = """
    INSERT INTO team_stats (
      season, division_num, match_id, round_index, team_id, opponent_team_id,
      map_id, is_forfeit_map, final_score, first_half_score, second_half_score,
      overtime_score, headshot_pct, win
    )
    VALUES (
      %(season)s, %(division_num)s, %(match_id)s, %(round_index)s, %(team_id)s, %(opponent_team_id)s,
      %(map_id)s, %(is_forfeit_map)s, %(final_score)s, %(first_half_score)s, %(second_half_score)s,
      %(overtime_score)s, %(headshot_pct)s, %(win)s
    )
    ON DUPLICATE KEY UPDATE
      opponent_team_id = VALUES(opponent_team_id),
      map_id = VALUES(map_id),
      is_forfeit_map = VALUES(is_forfeit_map),
      final_score = VALUES(final_score),
      first_half_score = VALUES(first_half_score),
      second_half_score = VALUES(second_half_score),
      overtime_score = VALUES(overtime_score),
      headshot_pct = VALUES(headshot_pct),
      win = VALUES(win)
"""

def _champ_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalise championship row keys to prevent missing placeholders."""
    return {
        "championship_id": row.get("championship_id"),
        "season": row.get("season"),
        "division_num": row.get("division_num"),
        "name": row.get("name"),
        "is_playoffs": row.get("is_playoffs", 0),
        "slug": row.get("slug"),
        "parent_championship_id": row.get("parent_championship_id"),
    }


async def upsert_championships_async(conn: asyncmy.Connection, rows: Sequence[Row]) -> None:
    """Upsert multiple championships."""
    prepared = [_champ_row(row) for row in rows]
    async with conn.cursor() as cur:
        await cur.executemany(_CHAMPIONSHIP_UPSERT_SQL, prepared)
        await conn.commit()


async def upsert_championship_async(
    conn: asyncmy.Connection | None,
    row: Row,
) -> None:
    """Upsert a single championship."""

    async def _op():
        async with _write_connection(conn, label="upsert_championship") as target_conn:
            async with target_conn.cursor() as cur:
                await cur.execute(_CHAMPIONSHIP_UPSERT_SQL, _champ_row(row))
                await target_conn.commit()

    await _retry_on_deadlock(_op, label="upsert_championship")


async def upsert_map_catalog_async(
    conn: asyncmy.Connection,
    row: Row,
    *,
    commit: bool = True,
) -> None:
    sql = """
        INSERT INTO maps_catalog (map_id, pretty_name, image_sm, image_lg)
        VALUES (%(map_id)s, %(pretty_name)s, %(image_sm)s, %(image_lg)s)
        ON DUPLICATE KEY UPDATE
          pretty_name = VALUES(pretty_name),
          image_sm = CASE WHEN VALUES(image_sm) <> '' THEN VALUES(image_sm) ELSE maps_catalog.image_sm END,
          image_lg = CASE WHEN VALUES(image_lg) <> '' THEN VALUES(image_lg) ELSE maps_catalog.image_lg END
    """
    async with conn.cursor() as cur:
        await cur.execute(sql, row)
        if commit:
            await conn.commit()


async def upsert_match_async(
    conn: asyncmy.Connection,
    row: Row,
    *,
    commit: bool = True,
) -> None:
    match_id = row["match_id"]
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO matches (
              match_id, championship_id, season, division_num, best_of,
              configured_at, started_at, finished_at, scheduled_at, status,
              last_seen_at, activity_ts, team1_id, team2_id, winner_team_id,
              is_forfeit, ignored_due_ban, payload_hash
            )
            VALUES (
              %(match_id)s, %(championship_id)s, %(season)s, %(division_num)s, %(best_of)s,
              %(configured_at)s, %(started_at)s, %(finished_at)s, %(scheduled_at)s, %(status)s,
              %(last_seen_at)s, %(activity_ts)s, %(team1_id)s, %(team2_id)s, %(winner_team_id)s,
              %(is_forfeit)s, %(ignored_due_ban)s, %(payload_hash)s
            )
            ON DUPLICATE KEY UPDATE
              championship_id = VALUES(championship_id),
              season = VALUES(season),
              division_num = VALUES(division_num),
              best_of = VALUES(best_of),
              configured_at = VALUES(configured_at),
              started_at = VALUES(started_at),
              finished_at = VALUES(finished_at),
              scheduled_at = VALUES(scheduled_at),
              status = VALUES(status),
              last_seen_at = VALUES(last_seen_at),
              activity_ts = GREATEST(matches.activity_ts, VALUES(activity_ts)),
              team1_id = VALUES(team1_id),
              team2_id = VALUES(team2_id),
              winner_team_id = VALUES(winner_team_id),
              is_forfeit = VALUES(is_forfeit),
              ignored_due_ban = VALUES(ignored_due_ban),
              payload_hash = VALUES(payload_hash)
            """,
            row,
        )
        if commit:
            await conn.commit()
    LOGGER.debug("upserted match %s", match_id)


async def upsert_teams_bulk_async(
    rows: Sequence[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "teams",
) -> None:
    """Upsert multiple teams (optionally in an existing transaction)."""
    if not rows:
        return

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(_TEAM_UPSERT_SQL, rows)

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=f"upsert-{label}") as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=f"upsert-{label}")


async def upsert_team_championships_bulk_async(
    rows: Sequence[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "team_championships",
) -> None:
    """Upsert team-championship associations with historical team names."""
    if not rows:
        return

    sql = """
    INSERT INTO team_championships (team_id, championship_id, team_name)
    VALUES (%(team_id)s, %(championship_id)s, %(team_name)s)
    ON DUPLICATE KEY UPDATE
      team_name = CASE WHEN VALUES(team_name) <> '' THEN VALUES(team_name) ELSE team_championships.team_name END
    """

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(sql, rows)

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=f"upsert-{label}") as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=f"upsert-{label}")


async def upsert_players_bulk_async(
    rows: Sequence[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "players",
) -> None:
    """Upsert multiple players."""
    if not rows:
        return

    full_sql = """
    INSERT INTO players (player_id, nickname, avatar, faceit_url)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      nickname = CASE
        WHEN COALESCE(players.nickname, '') = '' THEN VALUES(nickname)
        WHEN players.nickname LIKE 'deleted-user-%' THEN VALUES(nickname)
        ELSE players.nickname
      END,
      avatar = CASE
        WHEN COALESCE(VALUES(avatar), '') <> '' THEN VALUES(avatar)
        ELSE players.avatar
      END,
      faceit_url = CASE
        WHEN COALESCE(VALUES(faceit_url), '') <> '' THEN VALUES(faceit_url)
        ELSE players.faceit_url
      END
    """
    fallback_sql = """
    INSERT INTO players (player_id, nickname)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
      nickname = CASE
        WHEN COALESCE(players.nickname, '') = '' THEN VALUES(nickname)
        WHEN players.nickname LIKE 'deleted-user-%' THEN VALUES(nickname)
        ELSE players.nickname
      END
    """
    full_params = [
        (
            row.get("player_id"),
            row.get("nickname") or row.get("name") or "",
            row.get("avatar"),
            row.get("faceit_url"),
        )
        for row in rows
        if row.get("player_id")
    ]
    fallback_params = [
        (player_id, nickname)
        for player_id, nickname, *_ in full_params
    ]
    if not full_params:
        return

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            try:
                await cur.executemany(full_sql, full_params)
            except Exception:
                await cur.executemany(fallback_sql, fallback_params)

    if conn is not None:
        await _retry_on_deadlock(lambda: _op(conn), label=f"upsert-{label}")
        return

    async def _owned_op():
        async with connection(label=f"upsert-{label}") as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=f"upsert-{label}")


async def upsert_player_championships_bulk_async(
    rows: Sequence[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "player_championships",
) -> None:
    """Upsert player-championship associations with historical player names."""
    if not rows:
        return

    sql = """
    INSERT INTO player_championships (player_id, championship_id, player_name)
    VALUES (%(player_id)s, %(championship_id)s, %(player_name)s)
    ON DUPLICATE KEY UPDATE
      player_name = CASE
        WHEN VALUES(player_name) <> '' THEN VALUES(player_name)
        ELSE player_championships.player_name
      END
    """

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(sql, rows)

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=f"upsert-{label}") as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=f"upsert-{label}")


async def upsert_maps_bulk_async(
    match_id: str,
    season: int,
    division_num: int,
    maps: Sequence[Row],
    *,
    allow_shrink: bool = True,
    conn: asyncmy.Connection | None = None,
    label: str = "maps",
) -> None:
    """Upsert maps for a match; deletes stale rows not present in payload."""
    if not maps:
        return
    match_id = str(match_id)

    rows = [
        {
            "match_id": match_id,
            "season": season,
            "division_num": division_num,
            **m,
        }
        for m in maps
    ]
    keep_rounds = [int(item["round_index"]) for item in rows if item.get("round_index") is not None]
    keep_rounds_clause = ", ".join(str(r) for r in keep_rounds) if keep_rounds else ""

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(_MAP_UPSERT_SQL, rows)

            existing_count = 0
            if not allow_shrink:
                await cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM maps
                    WHERE match_id = %s
                      AND round_index IS NOT NULL
                    """,
                    (match_id,),
                )
                row = await cur.fetchone()
                existing_count = int((row or [0])[0] or 0)

            incoming_count = len(set(keep_rounds))
            should_delete_obsolete = allow_shrink or incoming_count >= existing_count

            if should_delete_obsolete:
                # Delete maps no longer present (round index mismatch)
                delete_sql = """
                    DELETE FROM maps
                    WHERE match_id = %s
                      AND round_index IS NOT NULL
                """
                params: list[Any] = [match_id]
                if keep_rounds_clause:
                    delete_sql += f" AND round_index NOT IN ({keep_rounds_clause})"
                await cur.execute(delete_sql, params)
            else:
                LOGGER.warning(
                    "Skipping map shrink for match %s: existing=%d incoming=%d",
                    match_id,
                    existing_count,
                    incoming_count,
                )

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=f"upsert-{label}") as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=f"upsert-{label}")


async def clear_obsolete_maps_async(
    match_id: str,
    keep_round_indexes: Iterable[int],
    *,
    label: str = "clear-obsolete-maps",
) -> None:
    """Delete maps no longer present for a match (round index mismatch)."""
    keep_clause = ", ".join(str(r) for r in keep_round_indexes)
    async with connection(label=label) as conn:
        async with conn.cursor() as cur:
            sql = """
                DELETE FROM maps
                WHERE match_id = %s
                  AND round_index IS NOT NULL
            """
            params: list[Any] = [match_id]
            if keep_clause:
                sql += f" AND round_index NOT IN ({keep_clause})"
            await cur.execute(sql, params)
        await conn.commit()


async def get_map_id_lookup_async(
    conn: asyncmy.Connection,
    match_id: str,
) -> dict[int, int]:
    """Return mapping of round_index -> map_id for a match."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT map_id, round_index FROM maps WHERE match_id = %s",
            (match_id,),
        )
        rows = await cur.fetchall()
    return {int(r[1]): int(r[0]) for r in rows}


async def replace_map_votes_async(
    match_id: str,
    season: int,
    division_num: int,
    votes: Sequence[Row],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "map-votes",
) -> None:
    if not votes:
        if conn is not None:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM map_votes WHERE match_id = %s", (match_id,))
            return
        async with connection(label=label) as owned_conn:
            async with owned_conn.cursor() as cur:
                await cur.execute("DELETE FROM map_votes WHERE match_id = %s", (match_id,))
            await owned_conn.commit()
        return

    rows = [
        {
            "match_id": match_id,
            "season": season,
            "division_num": division_num,
            **vote,
        }
        for vote in votes
    ]

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.execute("DELETE FROM map_votes WHERE match_id = %s", (match_id,))
            await cur.executemany(
                """
                INSERT INTO map_votes (
                  match_id, season, division_num, map_name, status,
                  selected_by_faction, selected_by_team_id, round_num
                )
                VALUES (
                  %(match_id)s, %(season)s, %(division_num)s, %(map_name)s, %(status)s,
                  %(selected_by_faction)s, %(selected_by_team_id)s, %(round_num)s
                )
                """,
                rows,
            )

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=label) as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=label)


async def delete_stats_for_match_async(
    match_id: str,
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "delete-stats",
) -> None:
    """Delete stats rows for a match with deadlock-safe retries."""

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.execute("DELETE FROM player_stats WHERE match_id = %s", (match_id,))
            await cur.execute("DELETE FROM team_stats WHERE match_id = %s", (match_id,))

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=label) as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=label)


async def upsert_player_stats_bulk_async(
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    player_rows: Sequence[Row],
    forfeit_lookup: Mapping[int, bool],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "player-stats",
) -> None:
    """Upsert player stats for a match."""
    if not player_rows:
        return

    def _get_stat(stats: Mapping[str, Any], key: str, default: Any = 0) -> Any:
        """Extract stat value with safe defaults."""
        val = stats.get(key, default)
        if val is None or val == "":
            return default
        return val

    def _normalize_result(value: Any) -> int:
        if value is None:
            return 0
        text = str(value).strip().lower()
        if text in {"1", "w", "win", "won", "true"}:
            return 1
        return 0

    rows: list[dict[str, Any]] = []
    for row in player_rows:
        round_index = int(row.get("round_index") or 0)
        map_id = map_lookup.get(round_index)
        stats = row.get("stats", {}) or {}
        
        rows.append(
            {
                "season": season,
                "division_num": division_num,
                "match_id": match_id,
                "map_id": map_id,
                "round_index": round_index,
                "player_id": row.get("player_id"),
                "team_id": row.get("team_id"),
                "opponent_team_id": row.get("opponent_team_id"),
                "is_forfeit_map": 1 if forfeit_lookup.get(round_index) else 0,
                # Core stats
                "kills": _get_stat(stats, "Kills"),
                "deaths": _get_stat(stats, "Deaths"),
                "assists": _get_stat(stats, "Assists"),
                "mvps": _get_stat(stats, "MVPs"),
                "headshots": _get_stat(stats, "Headshots"),
                "damage": _get_stat(stats, "Damage"),
                # Weapon-specific
                "sniper_kills": _get_stat(stats, "Sniper Kills"),
                "pistol_kills": _get_stat(stats, "Pistol Kills"),
                "knife_kills": _get_stat(stats, "Knife Kills"),
                "zeus_kills": _get_stat(stats, "Zeus Kills"),
                "first_kills": _get_stat(stats, "First Kills"),
                # Utility
                "enemies_flashed": _get_stat(stats, "Enemies Flashed"),
                "flash_count": _get_stat(stats, "Flash Count"),
                "flash_successes": _get_stat(stats, "Flash Successes"),
                "utility_damage": _get_stat(stats, "Utility Damage"),
                "utility_count": _get_stat(stats, "Utility Count"),
                "utility_successes": _get_stat(stats, "Utility Successes"),
                "utility_enemies": _get_stat(stats, "Utility Enemies"),
                # Multikills
                "mk_2k": _get_stat(stats, "Double Kills"),
                "mk_3k": _get_stat(stats, "Triple Kills"),
                "mk_4k": _get_stat(stats, "Quadro Kills"),
                "mk_5k": _get_stat(stats, "Penta Kills"),
                # Clutch
                "clutch_kills": _get_stat(stats, "Clutch Kills"),
                "cl_1v1_attempts": _get_stat(stats, "1v1Count"),
                "cl_1v1_wins": _get_stat(stats, "1v1Wins"),
                "cl_1v2_attempts": _get_stat(stats, "1v2Count"),
                "cl_1v2_wins": _get_stat(stats, "1v2Wins"),
                # Entry
                "entry_count": _get_stat(stats, "Entry Count"),
                "entry_wins": _get_stat(stats, "Entry Wins"),
                # Ratios
                "kd": _get_stat(stats, "K/D Ratio", 0.0),
                "kr": _get_stat(stats, "K/R Ratio", 0.0),
                "adr": _get_stat(stats, "ADR", 0.0),
                "hs_pct": _get_stat(stats, "Headshots %", 0.0),
                "result": _normalize_result(_get_stat(stats, "Result")),
            }
        )

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(_PLAYER_STAT_UPSERT_SQL, rows)

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=label) as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=label)


async def upsert_team_stats_bulk_async(
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    team_rows: Sequence[Row],
    forfeit_lookup: Mapping[int, bool],
    *,
    conn: asyncmy.Connection | None = None,
    label: str = "team-stats",
) -> None:
    if not team_rows:
        return

    rows = []
    for row in team_rows:
        round_index = int(row.get("round_index") or 0)
        rows.append(
            {
                "season": season,
                "division_num": division_num,
                "match_id": match_id,
                "map_id": map_lookup.get(round_index),
                "is_forfeit_map": 1 if forfeit_lookup.get(round_index) else 0,
                **row,
            }
        )

    async def _op(target_conn: asyncmy.Connection):
        async with target_conn.cursor() as cur:
            await cur.executemany(_TEAM_STAT_UPSERT_SQL, rows)

    if conn is not None:
        await _op(conn)
        return

    async def _owned_op():
        async with connection(label=label) as owned_conn:
            await _op(owned_conn)
            await owned_conn.commit()

    await _retry_on_deadlock(_owned_op, label=label)


async def upsert_team_season_totals_bulk_async(
    season: int,
    division_num: int,
    team_ids: Sequence[str],
    *,
    snapshot_ts: int | None = None,
    label: str = "team-season-totals-bulk",
) -> None:
    """Upsert aggregated season totals for multiple teams in one query."""
    team_id_list = _normalize_id_list(team_ids)
    if not team_id_list:
        return

    placeholders = ", ".join(["%s"] * len(team_id_list))

    async def _op():
        async with connection(label=label) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO team_season_totals (
                      season, division_num, team_id,
                      matches_played, matches_won, maps_played, maps_won,
                      rounds_won, rounds_lost
                    )
                    SELECT
                      %s AS season, %s AS division_num, mt.team_id,
                      COUNT(DISTINCT mt.match_id) AS matches_played,
                      COUNT(DISTINCT CASE WHEN mt.winner_team_id = mt.team_id THEN mt.match_id END) AS matches_won,
                      COUNT(mp.map_id) AS maps_played,
                      SUM(CASE WHEN mp.winner_team_id = mt.team_id THEN 1 ELSE 0 END) AS maps_won,
                      SUM(
                        CASE WHEN mt.team_slot = 1 THEN COALESCE(mp.score_team1,0)
                             ELSE COALESCE(mp.score_team2,0) END
                      ) AS rounds_won,
                      SUM(
                        CASE WHEN mt.team_slot = 1 THEN COALESCE(mp.score_team2,0)
                             ELSE COALESCE(mp.score_team1,0) END
                      ) AS rounds_lost
                    FROM (
                      SELECT m.match_id, m.winner_team_id, m.team1_id AS team_id, 1 AS team_slot
                      FROM matches m
                      JOIN championships c ON m.championship_id = c.championship_id
                      WHERE m.season = %s
                        AND m.division_num = %s
                        AND m.team1_id IN ({placeholders})
                        AND c.is_playoffs = 0
                        AND NULLIF(m.finished_at, 0) IS NOT NULL
                                                AND COALESCE(m.ignored_due_ban, 0) = 0
                      UNION ALL
                      SELECT m.match_id, m.winner_team_id, m.team2_id AS team_id, 2 AS team_slot
                      FROM matches m
                      JOIN championships c ON m.championship_id = c.championship_id
                      WHERE m.season = %s
                        AND m.division_num = %s
                        AND m.team2_id IN ({placeholders})
                        AND c.is_playoffs = 0
                        AND NULLIF(m.finished_at, 0) IS NOT NULL
                                                AND COALESCE(m.ignored_due_ban, 0) = 0
                    ) mt
                    LEFT JOIN maps mp ON mp.match_id = mt.match_id
                    GROUP BY mt.team_id
                    ON DUPLICATE KEY UPDATE
                      matches_played = VALUES(matches_played),
                      matches_won = VALUES(matches_won),
                      maps_played = VALUES(maps_played),
                      maps_won = VALUES(maps_won),
                      rounds_won = VALUES(rounds_won),
                      rounds_lost = VALUES(rounds_lost)
                    """,
                    (
                        season,
                        division_num,
                        season,
                        division_num,
                        *team_id_list,
                        season,
                        division_num,
                        *team_id_list,
                    ),
                )
                if snapshot_ts is not None:
                    await cur.execute(
                        f"""
                        INSERT IGNORE INTO team_season_totals_prev (
                          season, division_num, team_id,
                          matches_played, matches_won, maps_played, maps_won,
                          rounds_won, rounds_lost, snapshot_ts
                        )
                        SELECT
                          season, division_num, team_id,
                          matches_played, matches_won, maps_played, maps_won,
                          rounds_won, rounds_lost, %s AS snapshot_ts
                        FROM team_season_totals
                        WHERE season = %s
                          AND division_num = %s
                          AND team_id IN ({placeholders})
                        """,
                        (snapshot_ts, season, division_num, *team_id_list),
                    )
            await conn.commit()

    await _retry_on_deadlock(_op, label=label)


async def upsert_player_season_totals_bulk_async(
    season: int,
    division_num: int,
    player_ids: Sequence[str],
    *,
    label: str = "player-season-totals-bulk",
) -> None:
    """Upsert aggregated season totals for multiple players in one query."""
    player_id_list = _normalize_id_list(player_ids)
    if not player_id_list:
        return

    placeholders = ", ".join(["%s"] * len(player_id_list))

    async def _op():
        async with connection(label=label) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO player_season_totals (
                      season, division_num, player_id, team_id,
                      maps_played, rounds_played, kills, deaths, assists,
                      mvps, headshots, sniper_kills, pistol_kills, knife_kills, zeus_kills, first_kills,
                      utility_damage, enemies_flashed, flash_count, flash_successes,
                      utility_count, utility_successes, utility_enemies,
                      mk_2k, mk_3k, mk_4k, mk_5k,
                      clutch_kills, cl_1v1_attempts, cl_1v1_wins,
                      cl_1v2_attempts, cl_1v2_wins, entry_count, entry_wins,
                      adr, kr, kd, hs_pct, damage
                    )
                    SELECT
                      %s AS season, %s AS division_num, ps.player_id,
                      MAX(ps.team_id) AS team_id,
                      COUNT(*) AS maps_played,
                      SUM(COALESCE(mp.score_team1,0) + COALESCE(mp.score_team2,0)) AS rounds_played,
                      SUM(ps.kills) AS kills,
                      SUM(ps.deaths) AS deaths,
                      SUM(ps.assists) AS assists,
                      SUM(ps.mvps) AS mvps,
                      SUM(ps.headshots) AS headshots,
                      SUM(ps.sniper_kills) AS sniper_kills,
                      SUM(ps.pistol_kills) AS pistol_kills,
                      SUM(ps.knife_kills) AS knife_kills,
                      SUM(ps.zeus_kills) AS zeus_kills,
                      SUM(ps.first_kills) AS first_kills,
                      SUM(ps.utility_damage) AS utility_damage,
                      SUM(ps.enemies_flashed) AS enemies_flashed,
                      SUM(ps.flash_count) AS flash_count,
                      SUM(ps.flash_successes) AS flash_successes,
                      SUM(ps.utility_count) AS utility_count,
                      SUM(ps.utility_successes) AS utility_successes,
                      SUM(ps.utility_enemies) AS utility_enemies,
                      SUM(ps.mk_2k) AS mk_2k,
                      SUM(ps.mk_3k) AS mk_3k,
                      SUM(ps.mk_4k) AS mk_4k,
                      SUM(ps.mk_5k) AS mk_5k,
                      SUM(ps.clutch_kills) AS clutch_kills,
                      SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
                      SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
                      SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
                      SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
                      SUM(ps.entry_count) AS entry_count,
                      SUM(ps.entry_wins) AS entry_wins,
                      COALESCE(SUM(ps.damage) / NULLIF(SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)),0), 0) AS adr,
                      COALESCE(SUM(ps.kills) / NULLIF(SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)),0), 0) AS kr,
                      COALESCE(SUM(ps.kills) / NULLIF(SUM(ps.deaths),0), SUM(ps.kills)) AS kd,
                      COALESCE(SUM(ps.headshots) / NULLIF(SUM(ps.kills),0) * 100, 0) AS hs_pct,
                      SUM(ps.damage) AS damage
                    FROM player_stats ps
                    JOIN matches m ON m.match_id = ps.match_id
                    LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
                    WHERE m.season = %s
                      AND m.division_num = %s
                      AND ps.player_id IN ({placeholders})
                      AND COALESCE(ps.is_forfeit_map, 0) = 0
                      AND COALESCE(mp.is_forfeit, 0) = 0
                    GROUP BY ps.player_id
                    ON DUPLICATE KEY UPDATE
                      team_id = VALUES(team_id),
                      maps_played = VALUES(maps_played),
                      rounds_played = VALUES(rounds_played),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      assists = VALUES(assists),
                      mvps = VALUES(mvps),
                      headshots = VALUES(headshots),
                      sniper_kills = VALUES(sniper_kills),
                      pistol_kills = VALUES(pistol_kills),
                      knife_kills = VALUES(knife_kills),
                      zeus_kills = VALUES(zeus_kills),
                      first_kills = VALUES(first_kills),
                      utility_damage = VALUES(utility_damage),
                      enemies_flashed = VALUES(enemies_flashed),
                      flash_count = VALUES(flash_count),
                      flash_successes = VALUES(flash_successes),
                      utility_count = VALUES(utility_count),
                      utility_successes = VALUES(utility_successes),
                      utility_enemies = VALUES(utility_enemies),
                      mk_2k = VALUES(mk_2k),
                      mk_3k = VALUES(mk_3k),
                      mk_4k = VALUES(mk_4k),
                      mk_5k = VALUES(mk_5k),
                      clutch_kills = VALUES(clutch_kills),
                      cl_1v1_attempts = VALUES(cl_1v1_attempts),
                      cl_1v1_wins = VALUES(cl_1v1_wins),
                      cl_1v2_attempts = VALUES(cl_1v2_attempts),
                      cl_1v2_wins = VALUES(cl_1v2_wins),
                      entry_count = VALUES(entry_count),
                      entry_wins = VALUES(entry_wins),
                      adr = VALUES(adr),
                      kr = VALUES(kr),
                      kd = VALUES(kd),
                      hs_pct = VALUES(hs_pct),
                      damage = VALUES(damage)
                    """,
                    (
                        season,
                        division_num,
                        season,
                        division_num,
                        *player_id_list,
                    ),
                )
            await conn.commit()

    await _retry_on_deadlock(_op, label=label)


async def upsert_team_map_season_totals_bulk_async(
    season: int,
    division_num: int,
    team_ids: Sequence[str],
    *,
    map_names: Sequence[str] | None = None,
    label: str = "team-map-season-totals-bulk",
) -> None:
    """Upsert aggregated map totals for multiple teams in one query."""
    team_id_list = _normalize_id_list(team_ids)
    if not team_id_list:
        return
    map_name_list = _normalize_id_list(map_names or [])

    placeholders = ", ".join(["%s"] * len(team_id_list))
    map_filter_sql = ""
    map_filter_params: tuple[Any, ...] = ()
    if map_name_list:
        map_placeholders = ", ".join(["%s"] * len(map_name_list))
        map_filter_sql = f" AND mp.map_name IN ({map_placeholders})"
        map_filter_params = tuple(map_name_list)

    async def _op():
        async with connection(label=label) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO team_map_season_totals (
                      season, division_num, team_id, map_name,
                      played, picks, opp_picks, wins, games,
                      ban1, ban2, opp_ban, total_own_ban, decov,
                      kills, deaths, mvps, rd, kd, adr, damage, utility_damage
                    )
                    SELECT
                      %s AS season, %s AS division_num, ts.team_id, mp.map_name,
                      COUNT(*) AS played,
                      0 AS picks,
                      0 AS opp_picks,
                      SUM(CASE WHEN ts.win = 1 THEN 1 ELSE 0 END) AS wins,
                      COUNT(*) AS games,
                      0, 0, 0, 0, 0,
                      SUM(COALESCE(ts.final_score,0)) AS kills,
                      SUM(COALESCE(opp.final_score,0)) AS deaths,
                      SUM(COALESCE(ps.mvps,0)) AS mvps,
                      SUM(COALESCE(ts.final_score,0) - COALESCE(opp.final_score,0)) AS rd,
                      COALESCE(SUM(COALESCE(ts.final_score,0)) / NULLIF(SUM(COALESCE(opp.final_score,0)),0), SUM(COALESCE(ts.final_score,0))) AS kd,
                      0 AS adr,
                      SUM(COALESCE(ps.damage,0)) AS damage,
                      SUM(COALESCE(ps.utility_damage,0)) AS utility_damage
                    FROM team_stats ts
                    JOIN matches m ON m.match_id = ts.match_id
                    JOIN maps mp ON mp.match_id = ts.match_id AND mp.round_index = ts.round_index
                    LEFT JOIN team_stats opp
                      ON opp.match_id = ts.match_id
                     AND opp.team_id <> ts.team_id
                     AND opp.round_index = ts.round_index
                    LEFT JOIN player_stats ps
                      ON ps.match_id = ts.match_id
                     AND ps.round_index = ts.round_index
                     AND ps.team_id = ts.team_id
                    WHERE m.season = %s
                      AND m.division_num = %s
                      AND ts.team_id IN ({placeholders})
                      AND ts.is_forfeit_map = 0
                      AND mp.is_forfeit = 0
                      {map_filter_sql}
                    GROUP BY ts.team_id, mp.map_name
                    ON DUPLICATE KEY UPDATE
                      played = VALUES(played),
                      wins = VALUES(wins),
                      games = VALUES(games),
                      rd = VALUES(rd),
                      kd = VALUES(kd),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      mvps = VALUES(mvps),
                      damage = VALUES(damage),
                      utility_damage = VALUES(utility_damage)
                    """,
                    (
                        season,
                        division_num,
                        season,
                        division_num,
                        *team_id_list,
                        *map_filter_params,
                    ),
                )
            await conn.commit()

    await _retry_on_deadlock(_op, label=label)


async def upsert_player_map_season_totals_bulk_async(
    season: int,
    division_num: int,
    player_ids: Sequence[str],
    *,
    label: str = "player-map-season-totals-bulk",
) -> None:
    """Upsert aggregated map totals for multiple players in one query."""
    player_id_list = _normalize_id_list(player_ids)
    if not player_id_list:
        return

    placeholders = ", ".join(["%s"] * len(player_id_list))

    async def _op():
        async with connection(label=label) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    INSERT INTO player_map_season_totals (
                      season, division_num, player_id, team_id, map_name,
                      maps_played, rounds_played, kills, deaths, assists,
                      headshots, sniper_kills, pistol_kills, knife_kills, zeus_kills, first_kills,
                      utility_damage, enemies_flashed, flash_count, flash_successes,
                      utility_count, utility_successes, utility_enemies,
                      mk_2k, mk_3k, mk_4k, mk_5k,
                      entry_count, entry_wins, clutch_kills,
                      cl_1v1_attempts, cl_1v1_wins, cl_1v2_attempts, cl_1v2_wins,
                      adr, kr, kd, hs_pct, mvps, damage
                    )
                    SELECT
                      %s AS season, %s AS division_num, ps.player_id,
                      MAX(ps.team_id) AS team_id,
                      COALESCE(mp.map_name, CONCAT('map_', ps.map_id)) AS map_name,
                      COUNT(*) AS maps_played,
                      SUM(COALESCE(mp.score_team1,0) + COALESCE(mp.score_team2,0)) AS rounds_played,
                      SUM(ps.kills) AS kills,
                      SUM(ps.deaths) AS deaths,
                      SUM(ps.assists) AS assists,
                      SUM(ps.headshots) AS headshots,
                      SUM(ps.sniper_kills) AS sniper_kills,
                      SUM(ps.pistol_kills) AS pistol_kills,
                      SUM(ps.knife_kills) AS knife_kills,
                      SUM(ps.zeus_kills) AS zeus_kills,
                      SUM(ps.first_kills) AS first_kills,
                      SUM(ps.utility_damage) AS utility_damage,
                      SUM(ps.enemies_flashed) AS enemies_flashed,
                      SUM(ps.flash_count) AS flash_count,
                      SUM(ps.flash_successes) AS flash_successes,
                      SUM(ps.utility_count) AS utility_count,
                      SUM(ps.utility_successes) AS utility_successes,
                      SUM(ps.utility_enemies) AS utility_enemies,
                      SUM(ps.mk_2k) AS mk_2k,
                      SUM(ps.mk_3k) AS mk_3k,
                      SUM(ps.mk_4k) AS mk_4k,
                      SUM(ps.mk_5k) AS mk_5k,
                      SUM(ps.entry_count) AS entry_count,
                      SUM(ps.entry_wins) AS entry_wins,
                      SUM(ps.clutch_kills) AS clutch_kills,
                      SUM(ps.cl_1v1_attempts) AS cl_1v1_attempts,
                      SUM(ps.cl_1v1_wins) AS cl_1v1_wins,
                      SUM(ps.cl_1v2_attempts) AS cl_1v2_attempts,
                      SUM(ps.cl_1v2_wins) AS cl_1v2_wins,
                      COALESCE(SUM(ps.damage) / NULLIF(SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)),0), 0) AS adr,
                      COALESCE(SUM(ps.kills) / NULLIF(SUM(COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)),0), 0) AS kr,
                      COALESCE(SUM(ps.kills) / NULLIF(SUM(ps.deaths),0), SUM(ps.kills)) AS kd,
                      COALESCE(SUM(ps.headshots) / NULLIF(SUM(ps.kills),0) * 100, 0) AS hs_pct,
                      SUM(ps.mvps) AS mvps,
                      SUM(ps.damage) AS damage
                    FROM player_stats ps
                    JOIN matches m ON m.match_id = ps.match_id
                    LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
                    WHERE m.season = %s
                      AND m.division_num = %s
                      AND ps.player_id IN ({placeholders})
                      AND COALESCE(ps.is_forfeit_map, 0) = 0
                      AND COALESCE(mp.is_forfeit, 0) = 0
                    GROUP BY ps.player_id, COALESCE(mp.map_name, CONCAT('map_', ps.map_id))
                    ON DUPLICATE KEY UPDATE
                      team_id = VALUES(team_id),
                      maps_played = VALUES(maps_played),
                      rounds_played = VALUES(rounds_played),
                      kills = VALUES(kills),
                      deaths = VALUES(deaths),
                      assists = VALUES(assists),
                      headshots = VALUES(headshots),
                      sniper_kills = VALUES(sniper_kills),
                      pistol_kills = VALUES(pistol_kills),
                      knife_kills = VALUES(knife_kills),
                      zeus_kills = VALUES(zeus_kills),
                      first_kills = VALUES(first_kills),
                      utility_damage = VALUES(utility_damage),
                      enemies_flashed = VALUES(enemies_flashed),
                      flash_count = VALUES(flash_count),
                      flash_successes = VALUES(flash_successes),
                      utility_count = VALUES(utility_count),
                      utility_successes = VALUES(utility_successes),
                      utility_enemies = VALUES(utility_enemies),
                      mk_2k = VALUES(mk_2k),
                      mk_3k = VALUES(mk_3k),
                      mk_4k = VALUES(mk_4k),
                      mk_5k = VALUES(mk_5k),
                      entry_count = VALUES(entry_count),
                      entry_wins = VALUES(entry_wins),
                      clutch_kills = VALUES(clutch_kills),
                      cl_1v1_attempts = VALUES(cl_1v1_attempts),
                      cl_1v1_wins = VALUES(cl_1v1_wins),
                      cl_1v2_attempts = VALUES(cl_1v2_attempts),
                      cl_1v2_wins = VALUES(cl_1v2_wins),
                      adr = VALUES(adr),
                      kr = VALUES(kr),
                      kd = VALUES(kd),
                      hs_pct = VALUES(hs_pct),
                      mvps = VALUES(mvps),
                      damage = VALUES(damage)
                    """,
                    (
                        season,
                        division_num,
                        season,
                        division_num,
                        *player_id_list,
                    ),
                )
            await conn.commit()

    await _retry_on_deadlock(_op, label=label)


async def upsert_player_stats_for_match_async(
    conn: asyncmy.Connection,
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    player_stats: Sequence[Row],
    forfeit_lookup: Mapping[int, bool],
) -> None:
    """Transactional helper for recompute scripts."""
    rows = []
    for row in player_stats:
        round_index = int(row.get("round_index") or 0)
        rows.append(
            {
                "season": season,
                "division_num": division_num,
                "match_id": match_id,
                "map_id": map_lookup.get(round_index),
                "round_index": round_index,
                "is_forfeit_map": 1 if forfeit_lookup.get(round_index) else 0,
                **row,
            }
        )

    async with conn.cursor() as cur:
        await cur.executemany(_PLAYER_STAT_UPSERT_SQL, rows)
    await conn.commit()


async def upsert_team_stats_for_match_async(
    conn: asyncmy.Connection,
    season: int,
    division_num: int,
    match_id: str,
    map_lookup: Mapping[int, int],
    team_stats: Sequence[Row],
    forfeit_lookup: Mapping[int, bool],
) -> None:
    rows = []
    for row in team_stats:
        round_index = int(row.get("round_index") or 0)
        rows.append(
            {
                "season": season,
                "division_num": division_num,
                "match_id": match_id,
                "map_id": map_lookup.get(round_index),
                "is_forfeit_map": 1 if forfeit_lookup.get(round_index) else 0,
                **row,
            }
        )

    async with conn.cursor() as cur:
        await cur.executemany(_TEAM_STAT_UPSERT_SQL, rows)
    await conn.commit()


async def create_snapshot_ts_async(
    conn: asyncmy.Connection,
    season: int,
    division_num: int,
    match_id: str | None = None,
    *,
    label: str = "snapshot",
) -> int:
    """Insert a new division snapshot row and return the snapshot_ts."""
    async with conn.cursor() as cur:
        if match_id:
            await cur.execute(
                """
                SELECT snapshot_ts
                FROM division_snapshots
                WHERE season = %s AND division_num = %s AND match_id = %s
                LIMIT 1
                """,
                (season, division_num, match_id),
            )
            row = await cur.fetchone()
            if row:
                snapshot_ts = int(row[0])
                LOGGER.debug("%s snapshot_ts=%s (existing)", label, snapshot_ts)
                return snapshot_ts
        await cur.execute(
            """
            INSERT INTO division_snapshots (season, division_num, match_id)
            VALUES (%s, %s, %s)
            """,
            (season, division_num, match_id),
        )
        await cur.execute("SELECT LAST_INSERT_ID()")
        row = await cur.fetchone()
        snapshot_ts = int(row[0]) if row else int(time.time())
    LOGGER.debug("%s snapshot_ts=%s", label, snapshot_ts)
    return snapshot_ts


# ---------------------------------------------------------------------------
# Map/team/player deltas used by API
# ---------------------------------------------------------------------------

async def _get_team_last_prev_ts_async(
    division_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> tuple[int | None, int | None]:
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    rows = await query_async(
        f"""
        SELECT DISTINCT { _TS_EXPR } AS ts
        FROM matches m
        WHERE m.championship_id=:champ AND (:team = m.team1_id OR :team = m.team2_id)
          AND EXISTS (SELECT 1 FROM maps mp WHERE mp.match_id = m.match_id){excl_clause}
        ORDER BY ts ASC
    """,
        {"champ": division_id, "team": team_id, **excl_params},
    )
    if not rows:
        return (None, None)
    curr_ts = rows[-1]["ts"]
    prev_ts = rows[-2]["ts"] if len(rows) >= 2 else None
    return (curr_ts, prev_ts)


async def compute_map_stats_table_data_until_async(
    championship_id: int,
    team_id: str,
    cutoff_ts: int,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    pool_rows = await query_async(
        """
        SELECT DISTINCT mp.map_name AS map_id
        FROM maps mp
        JOIN matches m ON m.match_id = mp.match_id
        WHERE m.championship_id = ?
            AND mp.map_name IS NOT NULL AND mp.map_name <> ''
            AND m.is_forfeit = 0
    """,
        (championship_id,),
    )

    if pool_rows:
        all_maps = [r["map_id"] for r in pool_rows]
    else:
        all_maps = ["de_nuke", "de_inferno", "de_mirage", "de_overpass", "de_dust2", "de_ancient", "de_train", "de_anubis"]

    cte_sql, map_params = _build_allmaps_cte(all_maps)

    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded)

    sql = f"""
        WITH allmaps AS (
            {cte_sql}
        ),
        my_matches AS (
            SELECT m.*
            FROM matches m
            WHERE m.championship_id = :champ
              AND (:team = m.team1_id OR :team = m.team2_id)
              AND { _TS_EXPR } <= :cutoff
              {excl_clause}
        ),
        team_maps AS (
            SELECT
                mp.map_name AS map,
                CASE WHEN m.team1_id = :team THEN mp.score_team1 ELSE mp.score_team2 END AS rounds_for,
                CASE WHEN m.team1_id = :team THEN mp.score_team2 ELSE mp.score_team1 END AS rounds_against,
                CASE
                    WHEN m.team1_id = :team AND mp.score_team1 > mp.score_team2 THEN 1
                    WHEN m.team2_id = :team AND mp.score_team2 > mp.score_team1 THEN 1
                    ELSE 0
                END AS win,
                1 AS game,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id = :team
                ) THEN 1 ELSE 0 END AS own_pick,
                CASE WHEN EXISTS (
                    SELECT 1 FROM map_votes v
                    WHERE v.match_id = m.match_id
                      AND LOWER(v.status) = 'pick'
                      AND v.map_name = mp.map_name
                      AND v.selected_by_team_id IS NOT NULL
                      AND v.selected_by_team_id <> :team
                ) THEN 1 ELSE 0 END AS opp_pick
            FROM my_matches m
            JOIN maps mp
              ON mp.match_id = m.match_id
             AND mp.round_index IS NOT NULL
        ),
        team_drops AS (
            SELECT
                v.match_id,
                v.map_name,
                v.selected_by_team_id,
                v.round_num,
                ROW_NUMBER() OVER (
                    PARTITION BY v.match_id, v.selected_by_team_id
                    ORDER BY COALESCE(v.round_num, 999), v.map_name
                ) AS drop_idx
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop'
              AND v.selected_by_team_id = :team
        ),
        opp_drops AS (
            SELECT v.match_id, v.map_name
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) = 'drop'
              AND (
                    (m.team1_id = :team AND v.selected_by_team_id = m.team2_id) OR
                    (m.team2_id = :team AND v.selected_by_team_id = m.team1_id)
                  )
        ),
        ban_counts AS (
            SELECT
                am.map,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 1), 0) AS ban1,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx = 2), 0) AS ban2,
                COALESCE((SELECT COUNT(*) FROM opp_drops od WHERE od.map_name = am.map), 0) AS opp_ban,
                COALESCE((SELECT COUNT(*) FROM team_drops td WHERE td.map_name = am.map AND td.drop_idx IN (1,2)), 0) AS total_own_ban
            FROM allmaps am
        ),
        perf AS (
            SELECT
                mp.map_name AS map,
                SUM(ps.kills)  AS kills,
                SUM(ps.deaths) AS deaths,
                SUM( (COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0)) * COALESCE(ps.adr,0) ) AS adr_weighted,
                SUM(  COALESCE(mp.score_team1,0)+COALESCE(mp.score_team2,0) )                          AS rounds_weight
            FROM player_stats ps
            JOIN my_matches m
              ON m.match_id = ps.match_id
            JOIN maps mp
              ON mp.match_id   = ps.match_id
             AND mp.round_index = ps.round_index
            WHERE ps.team_id = :team
            GROUP BY mp.map_name
        ),
        decov AS (
            SELECT
                v.map_name AS map,
                COUNT(*)   AS decov_cnt
            FROM map_votes v
            JOIN my_matches m ON m.match_id = v.match_id
            WHERE LOWER(v.status) IN ('decider','overflow')
            GROUP BY v.map_name
        )
        SELECT
            am.map                                                        AS map,
            COALESCE(COUNT(tm.map), 0)                                    AS played,
            COALESCE(SUM(tm.own_pick), 0)                                 AS picks,
            COALESCE(SUM(tm.opp_pick), 0)                                 AS opp_picks,
            COALESCE(SUM(tm.win), 0)                                      AS wins,
            COALESCE(SUM(tm.game), 0)                                     AS games,
            CASE WHEN COALESCE(SUM(tm.game),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(tm.win) / SUM(tm.game) END              AS wr,
            COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_own,
            COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0) AS games_own,
            CASE WHEN COALESCE(SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(CASE WHEN tm.own_pick=1 THEN tm.win ELSE 0 END)
                              / SUM(CASE WHEN tm.own_pick=1 THEN tm.game ELSE 0 END) END AS wr_own,
            COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.win  ELSE 0 END),0) AS wins_opp,
            COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0) AS games_opp,
            CASE WHEN COALESCE(SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END),0)=0 THEN 0.0
                 ELSE 100.0 * SUM(CASE WHEN tm.opp_pick=1 THEN tm.win ELSE 0 END)
                              / SUM(CASE WHEN tm.opp_pick=1 THEN tm.game ELSE 0 END) END AS wr_opp,
            COALESCE(SUM(tm.rounds_for), 0) - COALESCE(SUM(tm.rounds_against), 0) AS rd,
            COALESCE(bc.ban1, 0)                                          AS ban1,
            COALESCE(bc.ban2, 0)                                          AS ban2,
            COALESCE(bc.opp_ban, 0)                                       AS opp_ban,
            COALESCE(bc.total_own_ban, 0)                                 AS total_own_ban,
            COALESCE(1.0 * p.kills / NULLIF(p.deaths,0), 0.0)             AS kd,
            COALESCE(1.0 * p.adr_weighted / NULLIF(p.rounds_weight,0), 0.0) AS adr,
            COALESCE(dc.decov_cnt, 0)                                     AS decov
        FROM allmaps am
        LEFT JOIN team_maps tm ON tm.map = am.map
        LEFT JOIN ban_counts bc ON bc.map = am.map
        LEFT JOIN perf p        ON p.map  = am.map
        LEFT JOIN decov dc      ON dc.map = am.map
        GROUP BY am.map
    """

    rows = await query_async(sql, {"champ": championship_id, "team": team_id, "cutoff": cutoff_ts, **excl_params, **map_params})

    out: list[dict] = []
    for r in rows:
        mid = r["map"]

        def _ival(key: str) -> int:
            return int(r.get(key, 0) or 0)

        def _fval(key: str) -> float:
            return float(r.get(key, 0.0) or 0.0)

        out.append(
            {
                "map": mid,
                "map_pretty": mid,
                "played": _ival("played"),
                "picks": _ival("picks"),
                "opp_picks": _ival("opp_picks"),
                "wins": _ival("wins"),
                "games": _ival("games"),
                "wr": _fval("wr"),
                "wins_own": _ival("wins_own"),
                "games_own": _ival("games_own"),
                "wr_own": _fval("wr_own"),
                "wins_opp": _ival("wins_opp"),
                "games_opp": _ival("games_opp"),
                "wr_opp": _fval("wr_opp"),
                "rd": _ival("rd"),
                "ban1": _ival("ban1"),
                "ban2": _ival("ban2"),
                "opp_ban": _ival("opp_ban"),
                "total_own_ban": _ival("total_own_ban"),
                "kd": _fval("kd"),
                "adr": _fval("adr"),
                "decov": _ival("decov"),
            }
        )
    return out


async def compute_map_stats_with_delta_async(
    championship_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> dict[str, dict]:
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    curr_ts, _ = await _get_team_last_prev_ts_async(championship_id, team_id, excluded)
    if curr_ts is None:
        return {}

    prev_cutoff = max(0, int(curr_ts) - 1)

    curr = await compute_map_stats_table_data_until_async(championship_id, team_id, curr_ts, excluded)
    prev = await compute_map_stats_table_data_until_async(championship_id, team_id, prev_cutoff, excluded)

    curr_by = {r["map"]: r for r in curr}
    prev_by = {r["map"]: r for r in prev}

    out: dict[str, dict] = {}
    for m, c in curr_by.items():
        p = prev_by.get(m)
        if not p:
            out[m] = {"curr": c, "prev": None, "delta": None}
        else:
            d = {}
            for k, v in c.items():
                if isinstance(v, (int, float)):
                    d[k] = v - (p.get(k) or 0)
            out[m] = {"curr": c, "prev": p, "delta": d}
    return out


async def compute_team_map_deltas_async(
    championship_id: int | str,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> dict[str, dict]:
    """Backwards-compatible alias retained for API callers."""
    return await compute_map_stats_with_delta_async(championship_id, team_id, excluded_team_ids)


async def compute_player_map_deltas_async(
    championship_id: str,
    player_id: str,
) -> dict[str, dict]:
    rows = await query_async(
        """
        SELECT
            COALESCE(mp.map_name, CONCAT('map_', ps.map_id)) AS map_label,
            COALESCE(mp.map_name, CONCAT('map_', ps.map_id)) AS map_name,
            COUNT(DISTINCT ps.match_id) AS maps_played,
            SUM(COALESCE(mp.score_team1, 0) + COALESCE(mp.score_team2, 0)) AS rounds_played,
            SUM(COALESCE(ps.kills, 0)) AS kills,
            SUM(COALESCE(ps.deaths, 0)) AS deaths,
            SUM(COALESCE(ps.assists, 0)) AS assists,
            SUM(COALESCE(ps.headshots, 0)) AS headshots,
            SUM(COALESCE(ps.damage, 0)) AS damage,
            SUM(COALESCE(ps.mvps, 0)) AS mvps,
            SUM(COALESCE(ps.sniper_kills, 0)) AS sniper_kills,
            SUM(COALESCE(ps.utility_damage, 0)) AS utility_damage,
            SUM(COALESCE(ps.enemies_flashed, 0)) AS enemies_flashed,
            SUM(COALESCE(ps.flash_count, 0)) AS flash_count,
            SUM(COALESCE(ps.flash_successes, 0)) AS flash_successes,
            SUM(COALESCE(ps.mk_2k, 0)) AS mk_2k,
            SUM(COALESCE(ps.mk_3k, 0)) AS mk_3k,
            SUM(COALESCE(ps.mk_4k, 0)) AS mk_4k,
            SUM(COALESCE(ps.mk_5k, 0)) AS mk_5k,
            SUM(COALESCE(ps.clutch_kills, 0)) AS clutch_kills,
            SUM(COALESCE(ps.cl_1v1_attempts, 0)) AS c11_att,
            SUM(COALESCE(ps.cl_1v1_wins, 0)) AS c11_win,
            SUM(COALESCE(ps.cl_1v2_attempts, 0)) AS c12_att,
            SUM(COALESCE(ps.cl_1v2_wins, 0)) AS c12_win,
            SUM(COALESCE(ps.entry_count, 0)) AS entry_count,
            SUM(COALESCE(ps.entry_wins, 0)) AS entry_wins,
            SUM(COALESCE(ps.pistol_kills, 0)) AS pistol_kills
        FROM player_stats ps
        JOIN matches m ON m.match_id = ps.match_id
        LEFT JOIN maps mp ON mp.match_id = ps.match_id AND mp.round_index = ps.round_index
        WHERE m.championship_id = :champ AND ps.player_id = :player AND COALESCE(ps.is_forfeit_map, 0) = 0
        GROUP BY map_label
        ORDER BY map_label
        """,
        {"champ": championship_id, "player": player_id},
    )

    out: dict[str, dict] = {}
    for row in rows:
        map_name = row.get("map_name") or "Unknown"
        kills = int(row.get("kills") or 0)
        deaths = int(row.get("deaths") or 0)
        rounds = int(row.get("rounds_played") or 0)

        curr = {
            "maps_played": int(row.get("maps_played") or 0),
            "rounds": rounds,
            "kills": kills,
            "deaths": deaths,
            "assists": int(row.get("assists") or 0),
            "kd": (kills / deaths) if deaths else float(kills),
            "kr": (kills / rounds) if rounds else 0.0,
            "adr": (float(row.get("damage") or 0.0) / rounds) if rounds else 0.0,
            "hs_pct": ((float(row.get("headshots") or 0.0) / kills) * 100.0) if kills else 0.0,
            "mvps": int(row.get("mvps") or 0),
            "sniper_kills": int(row.get("sniper_kills") or 0),
            "utility_damage": int(row.get("utility_damage") or 0),
            "enemies_flashed": int(row.get("enemies_flashed") or 0),
            "flash_count": int(row.get("flash_count") or 0),
            "flash_successes": int(row.get("flash_successes") or 0),
            "clutch_kills": int(row.get("clutch_kills") or 0),
            "c11_att": int(row.get("c11_att") or 0),
            "c11_win": int(row.get("c11_win") or 0),
            "c12_att": int(row.get("c12_att") or 0),
            "c12_win": int(row.get("c12_win") or 0),
            "entry_count": int(row.get("entry_count") or 0),
            "entry_wins": int(row.get("entry_wins") or 0),
            "mk_2k": int(row.get("mk_2k") or 0),
            "mk_3k": int(row.get("mk_3k") or 0),
            "mk_4k": int(row.get("mk_4k") or 0),
            "mk_5k": int(row.get("mk_5k") or 0),
            "pistol_kills": int(row.get("pistol_kills") or 0),
            "damage": int(row.get("damage") or 0),
        }
        out[map_name] = {"curr": curr, "prev": None, "delta": None}
    return out


async def get_team_matches_mirror_async(
    championship_id: int,
    team_id: str,
    excluded_team_ids: Collection[str] | None = None,
) -> list[dict]:
    excluded = _prepare_excluded(excluded_team_ids, ignore=[team_id])
    excl_clause, excl_params = _build_exclusion_clause(excluded, alias="m")

    sql = f"""
    WITH my_matches AS (
      SELECT
        m.match_id, m.championship_id, m.team1_id, m.team2_id,
        m.best_of, m.status, m.is_forfeit, m.winner_team_id,
        m.scheduled_at,
        COALESCE(m.started_at, m.scheduled_at, m.configured_at, 0) AS ts,
        CASE WHEN NULLIF(m.finished_at, 0) IS NOT NULL THEN 1 ELSE 0 END AS played
      FROM matches m
      WHERE m.championship_id = :champ AND (:team = m.team1_id OR :team = m.team2_id){excl_clause}
    ),
    mp AS (
      SELECT
        mm.match_id, mm.team1_id, mm.team2_id,
        mm.best_of, mm.status, mm.is_forfeit AS match_is_forfeit, mm.winner_team_id AS match_winner_team_id,
        mm.ts, mm.played, mm.scheduled_at,
        ma.round_index, ma.map_name, ma.score_team1, ma.score_team2, ma.winner_team_id AS map_winner_team_id,
        COALESCE(ma.is_forfeit, 0) AS map_is_forfeit,
        mc.image_sm, mc.image_lg
      FROM my_matches mm
      LEFT JOIN maps ma ON ma.match_id = mm.match_id
      LEFT JOIN maps_catalog mc ON LOWER(mc.map_id) = LOWER(ma.map_name)
    ),
    ps_agg AS (
      SELECT
        ps.match_id, ps.round_index, ps.team_id,
        SUM(COALESCE(ps.kills,0))   AS kills,
        SUM(COALESCE(ps.deaths,0))  AS deaths,
        SUM(COALESCE(ps.damage,0))  AS dmg,
        AVG(NULLIF(ps.adr,0))       AS adr_avg
      FROM player_stats ps
      JOIN my_matches m ON m.match_id = ps.match_id
      GROUP BY ps.match_id, ps.round_index, ps.team_id
    ),
    picks AS (
      SELECT v.match_id, v.map_name,
             MAX(v.selected_by_team_id) AS pick_team_id
      FROM map_votes v
      JOIN my_matches m ON m.match_id = v.match_id
      WHERE v.status = 'pick'
      GROUP BY v.match_id, v.map_name
    )
    SELECT
      mp.match_id, mp.ts, mp.status, mp.best_of, mp.played, mp.scheduled_at,
      mp.match_is_forfeit, mp.match_winner_team_id,
      mp.team1_id, mp.team2_id,
      COALESCE(tc1.team_name, t1.name) AS team1_name,
      COALESCE(tc2.team_name, t2.name) AS team2_name,
      t1.avatar AS t1_avatar,
      t2.avatar AS t2_avatar,
      mp.round_index, mp.map_name, mp.score_team1, mp.score_team2,
      mp.map_is_forfeit, mp.map_winner_team_id,
      mp.image_sm, mp.image_lg,
      pk.pick_team_id,
      COALESCE(ps1.kills, 0)      AS t1_kills,
      COALESCE(ps1.deaths, 0)     AS t1_deaths,
      COALESCE(ps1.adr_avg, 0.0)  AS t1_adr,
      COALESCE(ps1.dmg, 0)        AS t1_dmg,
      COALESCE(ps2.kills, 0)      AS t2_kills,
      COALESCE(ps2.deaths, 0)     AS t2_deaths,
      COALESCE(ps2.adr_avg, 0.0)  AS t2_adr,
      COALESCE(ps2.dmg, 0)        AS t2_dmg
    FROM mp
    LEFT JOIN ps_agg ps1 ON ps1.match_id=mp.match_id AND ps1.round_index=mp.round_index AND ps1.team_id=mp.team1_id
    LEFT JOIN ps_agg ps2 ON ps2.match_id=mp.match_id AND ps2.round_index=mp.round_index AND ps2.team_id=mp.team2_id
    LEFT JOIN picks pk    ON pk.match_id=mp.match_id AND pk.map_name=mp.map_name
    LEFT JOIN teams t1    ON t1.team_id = mp.team1_id
    LEFT JOIN teams t2    ON t2.team_id = mp.team2_id
    LEFT JOIN team_championships tc1 ON tc1.team_id = mp.team1_id AND tc1.championship_id = :champ
    LEFT JOIN team_championships tc2 ON tc2.team_id = mp.team2_id AND tc2.championship_id = :champ
    ORDER BY (mp.ts IS NULL) ASC, mp.ts ASC, mp.match_id ASC, mp.round_index ASC
    """

    rows = await query_async(sql, {"champ": championship_id, "team": team_id, **excl_params})

    out: dict[str, dict] = {}
    for r in rows:
        mid = r["match_id"]
        if mid not in out:
            me_on_left = r["team1_id"] == team_id
            opp_id = r["team2_id"] if me_on_left else r["team1_id"]
            opp_name = r["team2_name"] if me_on_left else r["team1_name"]
            opp_avatar = r["t2_avatar"] if me_on_left else r["t1_avatar"]
            my_name = r["team1_name"] if me_on_left else r["team2_name"]

            out[mid] = {
                "match_id": mid,
                "status": r["status"],
                "best_of": r["best_of"],
                "ts": r["ts"],
                "scheduled_at": r.get("scheduled_at"),
                "played": int(r["played"] or 0),
                "is_forfeit": bool(r["match_is_forfeit"]),
                "winner_team_id": r["match_winner_team_id"],
                "left": {"team_id": team_id, "team_name": my_name or "", "avatar": (r["t1_avatar"] if me_on_left else r["t2_avatar"])},
                "right": {"team_id": opp_id, "team_name": opp_name or "", "avatar": opp_avatar},
                "faceit_url": f"https://www.faceit.com/cs2/room/{mid}" if mid else "",
                "maps": [],
            }

        if r["round_index"] is None:
            continue

        me_is_t1 = r["team1_id"] == team_id
        rf = r["score_team1"] if me_is_t1 else r["score_team2"]
        ra = r["score_team2"] if me_is_t1 else r["score_team1"]

        me_kills = int(float((r["t1_kills"] if me_is_t1 else r["t2_kills"]) or 0))
        me_deaths = int(float((r["t1_deaths"] if me_is_t1 else r["t2_deaths"]) or 0))
        me_adr = float((r["t1_adr"] if me_is_t1 else r["t2_adr"]) or 0.0)
        me_damage = int(float((r["t1_dmg"] if me_is_t1 else r["t2_dmg"]) or 0))

        opp_kills = int(float((r["t2_kills"] if me_is_t1 else r["t1_kills"]) or 0))
        opp_deaths = int(float((r["t2_deaths"] if me_is_t1 else r["t1_deaths"]) or 0))
        opp_adr = float((r["t2_adr"] if me_is_t1 else r["t1_adr"]) or 0.0)
        opp_damage = int(float((r["t2_dmg"] if me_is_t1 else r["t1_dmg"]) or 0))

        me_kd = (float(me_kills) / me_deaths) if me_deaths else float(me_kills)
        opp_kd = (float(opp_kills) / opp_deaths) if opp_deaths else float(opp_kills)

        out[mid]["maps"].append(
            {
                "round_index": r["round_index"],
                "map": r["map_name"],
                "map_name": r["map_name"],
                "image_sm": r.get("image_sm"),
                "image_lg": r.get("image_lg"),
                "rf": rf if rf is not None else 0,
                "ra": ra if ra is not None else 0,
                "is_forfeit": bool(r["map_is_forfeit"]),
                "winner_team_id": r["map_winner_team_id"],
                "pick_team_id": r["pick_team_id"],
                "left": {"adr": me_adr, "kd": float(me_kd), "dmg": me_damage, "kills": me_kills, "deaths": me_deaths},
                "right": {"adr": opp_adr, "kd": float(opp_kd), "dmg": opp_damage, "kills": opp_kills, "deaths": opp_deaths},
            }
        )

    return [out[mid] for mid in sorted(out, key=lambda k: (out[k]["ts"] is None, out[k]["ts"] or 0, k))]


# ---------------------------------------------------------------------------
# Division summary helpers for season overview
# ---------------------------------------------------------------------------

async def get_all_base_divisions_for_season(conn: asyncmy.Connection, season: int) -> list[Row]:
    """Fetch base divisions (non-playoff) for a season."""
    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                c.championship_id as division_id,
                c.division_num as tier,
                c.name,
                'waiting' as status
            FROM championships c
            WHERE c.season = %s
              AND c.is_playoffs = 0
            ORDER BY c.division_num;
            """,
            (season,),
        )
        return await cur.fetchall()


async def get_division_stats_for_v3(conn: asyncmy.Connection, division_id: int) -> Row:
    """Aggregated stats for the season overview endpoints."""
    async with conn.cursor(cursors.DictCursor) as cur:
        played_condition = build_played_match_condition(
            alias="m",
            include_forfeits=True,
            include_ignored=True,
        )
        # Some playoff rows can miss finished_at even when the result is final.
        # Treat winner/status as a completion fallback for season-view progress.
        played_or_completed_condition = (
            f"({played_condition} "
            "OR m.winner_team_id IS NOT NULL "
            "OR LOWER(COALESCE(m.status, '')) IN ('finished', 'completed', 'done'))"
        )
        await cur.execute(
            """
            SELECT
                (SELECT COUNT(DISTINCT tst.team_id)
                 FROM team_season_totals tst
                 JOIN championships c ON tst.division_num = c.division_num AND tst.season = c.season
                 WHERE c.championship_id = %(division_id)s) AS teams,
                (SELECT COUNT(*)
                 FROM matches m
                 WHERE m.championship_id = %(division_id)s) AS matches_total,
                (SELECT status FROM matches WHERE championship_id = %(division_id)s ORDER BY finished_at DESC, scheduled_at DESC LIMIT 1) as division_status
            """,
            {"division_id": division_id},
        )
        season_stats = await cur.fetchone()

        await cur.execute(
            f"""
            SELECT
                (SELECT COUNT(*)
                 FROM matches m
                 JOIN championships c ON m.championship_id = c.championship_id
                 WHERE c.parent_championship_id = %(division_id)s) AS matches_total,
                (SELECT t.name
                 FROM matches m
                 JOIN championships c ON m.championship_id = c.championship_id
                 JOIN teams t ON t.team_id = m.winner_team_id
                 WHERE c.parent_championship_id = %(division_id)s
                   AND {played_or_completed_condition}
                   AND m.winner_team_id IS NOT NULL
                 ORDER BY m.finished_at DESC, m.scheduled_at DESC
                 LIMIT 1) AS winner_team,
                (SELECT c.championship_id
                 FROM championships c
                 WHERE c.parent_championship_id = %(division_id)s LIMIT 1) AS playoff_championship_id
            """,
            {"division_id": division_id},
        )
        playoff_stats = await cur.fetchone()

    async with conn.cursor(cursors.DictCursor) as cur:
        await cur.execute(
            f"""
            SELECT COUNT(DISTINCT m.match_id) AS matches_played
            FROM matches m
            WHERE m.championship_id = %(division_id)s
              AND {played_or_completed_condition}
            """,
            {"division_id": str(division_id)},
        )
        season_played_row = await cur.fetchone()
        await cur.execute(
            f"""
            SELECT COUNT(DISTINCT m.match_id) AS matches_played
            FROM matches m
            JOIN championships c ON c.championship_id = m.championship_id
            WHERE c.parent_championship_id = %(division_id)s
              AND {played_or_completed_condition}
            """,
            {"division_id": str(division_id)},
        )
        playoff_played_row = await cur.fetchone()

    season_played = int((season_played_row or {}).get("matches_played") or 0)
    playoff_played = int((playoff_played_row or {}).get("matches_played") or 0)
    if season_stats is not None:
        season_stats["matches_played"] = season_played
    if playoff_stats is not None:
        playoff_stats["matches_played"] = playoff_played

    return {
        "season": season_stats,
        "playoffs": playoff_stats,
    }
