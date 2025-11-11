"""Temporary runtime diagnostics for sync hangs and pool starvation.

# TODO(pipeline-diagnostics): remove once connection starvation root cause fixed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from db_async import get_pool_snapshot

LOGGER = logging.getLogger("pappaliiga.sync.diagnostics")


def _default_dump_path() -> Path:
    root = Path("debug")
    root.mkdir(parents=True, exist_ok=True)
    return root / "sync_state.json"


def _task_snapshot(task: asyncio.Task[Any]) -> Dict[str, Any]:
    stack = task.get_stack(limit=10)
    formatted = []
    for frame in stack:
        formatted.extend(traceback.format_stack(frame, limit=1))
    return {
        "name": task.get_name(),
        "done": task.done(),
        "coro": repr(task.get_coro()),
        "stack": formatted[-10:],
    }


@dataclass
class SyncDiagnostics:
    """Collects periodic runtime diagnostics and writes them to disk."""

    dump_path: Path = field(default_factory=_default_dump_path)
    interval_seconds: float = float(os.environ.get("SYNC_DIAG_INTERVAL", "15"))
    hang_threshold_seconds: float = float(os.environ.get("SYNC_DIAG_HANG_THRESHOLD", "30"))
    enabled: bool = os.environ.get("SYNC_DIAG_ENABLED", "1") != "0"

    def __post_init__(self) -> None:
        self._task: asyncio.Task[Any] | None = None
        self._last_progress: float = time.monotonic()
        self._last_event: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if not self.enabled or self._task:
            return
        self.dump_path.parent.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._monitor_loop(), name="sync-diagnostics-loop")
        LOGGER.debug("Sync diagnostics started; writing to %s", self.dump_path)

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
            LOGGER.debug("Sync diagnostics stopped")

    def mark_progress(self, event: str, detail: str | None = None) -> None:
        if not self.enabled:
            return
        self._last_progress = time.monotonic()
        self._last_event = {
            "event": event,
            "detail": detail,
            "ts": time.time(),
        }

    async def dump_once(self, *, include_tasks: bool = False, reason: str | None = None) -> Dict[str, Any]:
        snapshot = await self._build_snapshot(include_tasks=include_tasks, reason=reason)
        await self._write_snapshot(snapshot)
        return snapshot

    async def _monitor_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.interval_seconds)
                idle_for = time.monotonic() - self._last_progress
                include_tasks = idle_for >= self.hang_threshold_seconds
                if include_tasks:
                    LOGGER.warning(
                        "No sync progress for %.1fs; dumping extended diagnostics to %s",
                        idle_for,
                        self.dump_path,
                    )
                snapshot = await self._build_snapshot(
                    include_tasks=include_tasks,
                    reason="hang-check" if include_tasks else "periodic",
                )
                await self._write_snapshot(snapshot)
        except asyncio.CancelledError:
            pass

    async def _build_snapshot(self, *, include_tasks: bool, reason: str | None) -> Dict[str, Any]:
        async with self._lock:
            pool = await get_pool_snapshot()
            tasks: List[Dict[str, Any]] = []
            if include_tasks:
                tasks = [
                    _task_snapshot(task)
                    for task in asyncio.all_tasks()
                    if task is not asyncio.current_task()
                ]
            return {
                "ts": time.time(),
                "reason": reason,
                "last_progress": self._last_event,
                "pool": pool,
                "tasks": tasks,
            }

    async def _write_snapshot(self, snapshot: Dict[str, Any]) -> None:
        try:
            payload = json.dumps(snapshot, indent=2)
            await asyncio.to_thread(self.dump_path.write_text, payload, encoding="utf-8")
        except Exception as exc:  # pragma: no cover - best effort logging
            LOGGER.warning("Failed to write diagnostics snapshot to %s: %s", self.dump_path, exc)
