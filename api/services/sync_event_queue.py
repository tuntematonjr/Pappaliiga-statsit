from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import os
from pathlib import Path
import time
from typing import Literal

import faceit_config
from api.services.cache_helpers import clear_api_response_caches
from api.services.cache_reheat import reheat_main_page
from division_overrides import load_division_overrides
from sync_pipeline import sync_championship_async, update_single_match_async
from utils.log_files import (
    DEFAULT_LOG_MAX_AGE_DAYS,
    DEFAULT_LOG_MAX_TOTAL_BYTES,
    build_timestamped_log_path,
    prune_log_files,
)

LOGGER = logging.getLogger(__name__)
_FILE_LOGGER_CONFIGURED = False


def _configure_file_logging() -> None:
    global _FILE_LOGGER_CONFIGURED
    if _FILE_LOGGER_CONFIGURED:
        return

    if not _env_bool("SYNC_EVENT_FILE_LOG_ENABLED", default=True):
        _FILE_LOGGER_CONFIGURED = True
        return

    log_dir = Path(os.environ.get("SYNC_EVENT_LOG_DIR", str(Path("logs") / "faceit_webhooks")))
    log_path = build_timestamped_log_path(log_dir, prefix="sync-events")

    level_name = os.environ.get("SYNC_EVENT_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    from logging import FileHandler

    handler = FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(min(LOGGER.level, level) if LOGGER.level else level)
    try:
        try:
            log_path.touch(exist_ok=True)
        except Exception:
            pass
        prune_log_files(
            log_dir=log_dir,
            file_glob="sync-events-*.log",
            active_log_path=log_path,
            max_age_days=int(os.environ.get("SYNC_EVENT_LOG_MAX_AGE_DAYS", DEFAULT_LOG_MAX_AGE_DAYS)),
            max_total_bytes=int(os.environ.get("SYNC_EVENT_LOG_MAX_TOTAL_BYTES", DEFAULT_LOG_MAX_TOTAL_BYTES)),
            logger=LOGGER,
        )
    except Exception:
        LOGGER.exception("Sync event log pruning failed")
    _FILE_LOGGER_CONFIGURED = True
    LOGGER.info("Sync event file logging enabled path=%s level=%s", log_path, logging.getLevelName(level))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class SyncEventJob:
    kind: Literal["match", "championship"]
    target_id: str
    full: bool = False
    attempt: int = 0
    enqueued_at: float = field(default_factory=time.time)

    @property
    def dedupe_key(self) -> str:
        return f"{self.kind}:{self.target_id}"


class SyncEventQueue:
    def __init__(self) -> None:
        _configure_file_logging()
        self._queue: asyncio.Queue[SyncEventJob | None] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._queued_keys: set[str] = set()
        self._processing_keys: set[str] = set()
        self._recent_keys: dict[str, float] = {}
        self._dedupe_ttl_seconds = max(1, int(os.environ.get("SYNC_EVENT_DEDUPE_TTL", "120")))
        self._retry_interval_seconds = max(1, int(os.environ.get("SYNC_EVENT_RETRY_INTERVAL_SECONDS", "60")))
        self._retry_max_window_seconds = max(
            self._retry_interval_seconds,
            int(os.environ.get("SYNC_EVENT_RETRY_MAX_WINDOW_SECONDS", "900")),
        )
        default_max_attempts = max(2, (self._retry_max_window_seconds // self._retry_interval_seconds) + 1)
        self._retry_max_attempts = max(1, int(os.environ.get("SYNC_EVENT_RETRY_MAX_ATTEMPTS", str(default_max_attempts))))
        self._clear_cache_on_success = _env_bool("SYNC_EVENT_CLEAR_CACHE_ON_SUCCESS", default=True)
        self._reheat_on_success = _env_bool("SYNC_EVENT_REHEAT_ON_SUCCESS", default=True)
        self._validate_avatars = _env_bool("SYNC_EVENT_VALIDATE_AVATARS", default=False)
        self._max_match_concurrency = max(
            1,
            int(os.environ.get("SYNC_EVENT_MAX_MATCH_CONCURRENCY", str(faceit_config.MAX_MATCH_SYNC_CONCURRENCY))),
        )
        self._overrides = load_division_overrides()
        self._db_semaphore = asyncio.Semaphore(
            max(
                1,
                int(os.environ.get("SYNC_EVENT_DB_CONCURRENCY", str(faceit_config.MAX_DB_WRITER_CONCURRENCY))),
            )
        )
        self._worker_started_at: float | None = None
        self._last_job_key: str | None = None
        self._last_job_kind: str | None = None
        self._last_job_target_id: str | None = None
        self._last_job_started_at: float | None = None
        self._last_job_finished_at: float | None = None
        self._last_job_status: str | None = None
        self._last_job_error: str | None = None
        self._last_job_duration_ms: int | None = None

    async def start(self) -> None:
        if self._worker and not self._worker.done():
            return
        self._worker = asyncio.create_task(self._run_worker(), name="sync-event-worker")
        self._worker_started_at = time.time()
        LOGGER.info("Sync event queue worker started")

    async def stop(self) -> None:
        worker = self._worker
        if not worker:
            return
        if worker.done():
            self._worker = None
            return
        await self._queue.put(None)
        await worker
        self._worker = None
        self._worker_started_at = None
        LOGGER.info("Sync event queue worker stopped")

    async def enqueue_match(self, match_id: str) -> bool:
        return await self._enqueue(SyncEventJob(kind="match", target_id=str(match_id).strip()))

    async def enqueue_championship(self, championship_id: str, *, full: bool = False) -> bool:
        return await self._enqueue(
            SyncEventJob(kind="championship", target_id=str(championship_id).strip(), full=bool(full))
        )

    def stats(self) -> dict[str, object]:
        worker_running = bool(self._worker and not self._worker.done())
        return {
            "queue_size": self._queue.qsize(),
            "queued_keys": len(self._queued_keys),
            "processing_keys": len(self._processing_keys),
            "recent_keys": len(self._recent_keys),
            "worker_running": worker_running,
            "worker_started_at": self._worker_started_at,
            "last_job_key": self._last_job_key,
            "last_job_kind": self._last_job_kind,
            "last_job_target_id": self._last_job_target_id,
            "last_job_started_at": self._last_job_started_at,
            "last_job_finished_at": self._last_job_finished_at,
            "last_job_status": self._last_job_status,
            "last_job_error": self._last_job_error,
            "last_job_duration_ms": self._last_job_duration_ms,
        }

    async def _enqueue(self, job: SyncEventJob) -> bool:
        if not job.target_id:
            LOGGER.warning("Rejected sync event with empty target id kind=%s", job.kind)
            return False
        now = time.monotonic()
        key = job.dedupe_key
        async with self._lock:
            if not self._worker or self._worker.done():
                self._worker = asyncio.create_task(self._run_worker(), name="sync-event-worker")
                self._worker_started_at = time.time()
                LOGGER.warning("Sync event queue worker was not running; started lazily on enqueue")
            self._prune_recent(now)
            if key in self._queued_keys or key in self._processing_keys:
                LOGGER.info("Deduped sync event key=%s reason=already_queued_or_processing", key)
                return False
            recent_expiry = self._recent_keys.get(key)
            if recent_expiry and recent_expiry > now:
                LOGGER.info("Deduped sync event key=%s reason=recently_processed", key)
                return False
            self._queued_keys.add(key)
            self._queue.put_nowait(job)
            LOGGER.info(
                "Queued sync event key=%s kind=%s target=%s queue_size=%d",
                key,
                job.kind,
                job.target_id,
                self._queue.qsize(),
            )
            return True

    def _prune_recent(self, now: float) -> None:
        stale = [key for key, expiry in self._recent_keys.items() if expiry <= now]
        for key in stale:
            self._recent_keys.pop(key, None)

    async def _run_worker(self) -> None:
        while True:
            job = await self._queue.get()
            if job is None:
                self._queue.task_done()
                break

            key = job.dedupe_key
            started_monotonic = time.monotonic()
            started_epoch = time.time()
            queue_wait_ms = int(max(0.0, started_epoch - job.enqueued_at) * 1000)
            job_status = "succeeded"
            job_error: str | None = None
            async with self._lock:
                self._queued_keys.discard(key)
                self._processing_keys.add(key)
                self._last_job_key = key
                self._last_job_kind = job.kind
                self._last_job_target_id = job.target_id
                self._last_job_started_at = started_epoch
                self._last_job_finished_at = None
                self._last_job_status = "processing"
                self._last_job_error = None
                self._last_job_duration_ms = None

            LOGGER.info(
                "Processing sync event key=%s kind=%s target=%s attempt=%d queue_wait_ms=%d",
                key,
                job.kind,
                job.target_id,
                job.attempt + 1,
                queue_wait_ms,
            )

            retry_job: SyncEventJob | None = None
            try:
                if job.kind == "match":
                    championship_id = await update_single_match_async(
                        job.target_id,
                        overrides=self._overrides,
                        validate_avatars=self._validate_avatars,
                    )
                    if not championship_id:
                        raise RuntimeError("match_sync_not_ready")
                else:
                    await sync_championship_async(
                        job.target_id,
                        full=job.full,
                        overrides=self._overrides,
                        end_on_error=True,
                        db_semaphore=self._db_semaphore,
                        max_match_concurrency=self._max_match_concurrency,
                        validate_avatars=self._validate_avatars,
                    )
                await self._refresh_caches_after_sync(key)
            except Exception as exc:
                job_error = f"{type(exc).__name__}: {exc}"
                if self._should_retry(job):
                    job_status = "retrying"
                    retry_job = SyncEventJob(
                        kind=job.kind,
                        target_id=job.target_id,
                        full=job.full,
                        attempt=job.attempt + 1,
                    )
                    LOGGER.warning(
                        "Sync event failed for %s (%s) - scheduling retry %d/%d in %ss",
                        key,
                        job_error,
                        retry_job.attempt + 1,
                        self._retry_max_attempts,
                        self._retry_interval_seconds,
                    )
                else:
                    job_status = "failed"
                    LOGGER.error(
                        "Sync event %s exhausted retries after %d attempt(s); last error=%s",
                        key,
                        job.attempt + 1,
                        job_error,
                    )
                    LOGGER.exception("Sync event processing failed for %s", key)
            finally:
                duration_ms = int((time.monotonic() - started_monotonic) * 1000)
                now = time.monotonic()
                async with self._lock:
                    self._processing_keys.discard(key)
                    if retry_job is None:
                        self._recent_keys[key] = now + self._dedupe_ttl_seconds
                    self._last_job_finished_at = time.time()
                    self._last_job_status = job_status
                    self._last_job_error = job_error
                    self._last_job_duration_ms = duration_ms
                LOGGER.info(
                    "Finished sync event key=%s status=%s duration_ms=%d queue_size=%d",
                    key,
                    job_status,
                    duration_ms,
                    self._queue.qsize(),
                )
                self._queue.task_done()
            if retry_job is not None:
                asyncio.create_task(
                    self._requeue_retry(retry_job),
                    name=f"sync-event-retry:{retry_job.dedupe_key}:{retry_job.attempt}",
                )

    async def _refresh_caches_after_sync(self, key: str) -> None:
        if self._clear_cache_on_success:
            cleared = await clear_api_response_caches(clear_revision_cache=True)
            LOGGER.info("Cleared API caches after sync key=%s cleared=%s", key, cleared)
        if self._reheat_on_success:
            await reheat_main_page()

    def _should_retry(self, job: SyncEventJob) -> bool:
        return (job.attempt + 1) < self._retry_max_attempts

    async def _requeue_retry(self, job: SyncEventJob) -> None:
        key = job.dedupe_key
        delay_seconds = self._retry_interval_seconds
        try:
            LOGGER.info(
                "Requeueing sync event key=%s attempt=%d in %ss",
                key,
                job.attempt + 1,
                delay_seconds,
            )
            await asyncio.sleep(delay_seconds)
            async with self._lock:
                if key in self._queued_keys or key in self._processing_keys:
                    LOGGER.info("Skipped retry enqueue for %s because key is already queued/processing", key)
                    return
                if not self._worker or self._worker.done():
                    self._worker = asyncio.create_task(self._run_worker(), name="sync-event-worker")
                    self._worker_started_at = time.time()
                    LOGGER.warning("Sync event queue worker restarted by retry enqueue")
                self._queued_keys.add(key)
                self._queue.put_nowait(job)
                LOGGER.info(
                    "Queued retry sync event key=%s kind=%s target=%s attempt=%d queue_size=%d",
                    key,
                    job.kind,
                    job.target_id,
                    job.attempt + 1,
                    self._queue.qsize(),
                )
        except Exception:
            LOGGER.exception("Failed to requeue retry for sync event %s", key)


_QUEUE: SyncEventQueue | None = None


def get_sync_event_queue() -> SyncEventQueue:
    global _QUEUE
    if _QUEUE is None:
        _QUEUE = SyncEventQueue()
    return _QUEUE
