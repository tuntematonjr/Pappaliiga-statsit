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

Environment:
  DATABASE_URL = mariadb://user:pass@host:3306/database?param=value
Only secrets (user/password) are read from the environment; everything
else is kept in source control.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, Optional, Sequence
from urllib.parse import parse_qsl, urlparse

import asyncmy
from asyncmy import auth as asyncmy_auth
from asyncmy import errors as asyncmy_errors

LOGGER = logging.getLogger(__name__)

DEFAULT_POOL_MIN_SIZE = 1
DEFAULT_POOL_MAX_SIZE = 10
DEFAULT_POOL_RECYCLE_SECONDS = 300

SCHEMA_PATH = Path(__file__).with_name("mariadb_schema.sql")

_pool: Optional[asyncmy.Pool] = None
_pool_lock = asyncio.Lock()


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
        minsize = int(query.get("minsize", DEFAULT_POOL_MIN_SIZE))
        maxsize = int(query.get("maxsize", DEFAULT_POOL_MAX_SIZE))
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


@asynccontextmanager
async def connection() -> AsyncIterator[asyncmy.Connection]:
    pool = await get_pool()
    conn = await pool.acquire()
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


@asynccontextmanager
async def readonly_connection() -> AsyncIterator[asyncmy.Connection]:
    pool = await get_pool()
    conn = await pool.acquire()
    try:
        yield conn
    finally:
        pool.release(conn)


async def execute(sql: str, params: Sequence[Any] | Dict[str, Any] | None = None) -> int:
    """Execute a single statement and return the affected-row count."""
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount


async def executemany(sql: str, args: Iterable[Sequence[Any] | Dict[str, Any]]) -> int:
    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, args)
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
            return
        LOGGER.info("Creating database schema from %s", SCHEMA_PATH)
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        await _run_script(conn, sql)


async def reset_db_async(confirm: bool = False) -> None:
    """Drop all tables in the current database and recreate the schema."""
    if not confirm:
        raise RuntimeError("reset_db_async called without confirm=True; aborting to keep data safe")

    async with connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT DATABASE()")
            dbname = (await cur.fetchone() or ("",))[0]
            LOGGER.warning("Dropping all tables from %s", dbname)
            await cur.execute("SET FOREIGN_KEY_CHECKS=0")
            await cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
            )
            tables = [row[0] for row in await cur.fetchall()]
            for table in tables:
                await cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        await _run_script(conn, sql)
        LOGGER.info("Database reset and schema recreated")
