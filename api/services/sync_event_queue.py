from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import os
import time
from typing import Literal

import faceit_config
from division_overrides import load_division_overrides
from sync_pipeline import sync_championship_async, update_single_match_async

LOGGER = logging.getLogger(__name__)


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
    enqueued_at: float = field(default_factory=time.time)

    @property
    def dedupe_key(self) -> str:
        return f"{self.kind}:{self.target_id}"


class SyncEventQueue:
    def __init__(self) -> None:
        self._queue: asyncio.Queue[SyncEventJob | None] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._queued_keys: set[str] = set()
        self._processing_keys: set[str] = set()
        self._recent_keys: dict[str, float] = {}
        self._dedupe_ttl_seconds = max(1, int(os.environ.get("SYNC_EVENT_DEDUPE_TTL", "120")))
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

    async def start(self) -> None:
        if self._worker and not self._worker.done():
            return
        self._worker = asyncio.create_task(self._run_worker(), name="sync-event-worker")
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
        LOGGER.info("Sync event queue worker stopped")

    async def enqueue_match(self, match_id: str) -> bool:
        return await self._enqueue(SyncEventJob(kind="match", target_id=str(match_id).strip()))

    async def enqueue_championship(self, championship_id: str, *, full: bool = False) -> bool:
        return await self._enqueue(
            SyncEventJob(kind="championship", target_id=str(championship_id).strip(), full=bool(full))
        )

    def stats(self) -> dict[str, int | bool]:
        worker_running = bool(self._worker and not self._worker.done())
        return {
            "queue_size": self._queue.qsize(),
            "queued_keys": len(self._queued_keys),
            "processing_keys": len(self._processing_keys),
            "recent_keys": len(self._recent_keys),
            "worker_running": worker_running,
        }

    async def _enqueue(self, job: SyncEventJob) -> bool:
        if not job.target_id:
            return False
        now = time.monotonic()
        key = job.dedupe_key
        async with self._lock:
            self._prune_recent(now)
            if key in self._queued_keys or key in self._processing_keys:
                return False
            recent_expiry = self._recent_keys.get(key)
            if recent_expiry and recent_expiry > now:
                return False
            self._queued_keys.add(key)
            self._queue.put_nowait(job)
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
            async with self._lock:
                self._queued_keys.discard(key)
                self._processing_keys.add(key)

            try:
                if job.kind == "match":
                    LOGGER.info("Processing sync event for match %s", job.target_id)
                    await update_single_match_async(
                        job.target_id,
                        overrides=self._overrides,
                        validate_avatars=self._validate_avatars,
                    )
                else:
                    LOGGER.info("Processing sync event for championship %s (full=%s)", job.target_id, job.full)
                    await sync_championship_async(
                        job.target_id,
                        full=job.full,
                        overrides=self._overrides,
                        end_on_error=True,
                        db_semaphore=self._db_semaphore,
                        max_match_concurrency=self._max_match_concurrency,
                        validate_avatars=self._validate_avatars,
                    )
            except Exception:
                LOGGER.exception("Sync event processing failed for %s", key)
            finally:
                now = time.monotonic()
                async with self._lock:
                    self._processing_keys.discard(key)
                    self._recent_keys[key] = now + self._dedupe_ttl_seconds
                self._queue.task_done()


_QUEUE: SyncEventQueue | None = None


def get_sync_event_queue() -> SyncEventQueue:
    global _QUEUE
    if _QUEUE is None:
        _QUEUE = SyncEventQueue()
    return _QUEUE
