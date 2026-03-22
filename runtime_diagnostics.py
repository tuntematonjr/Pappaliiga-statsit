"""Lightweight runtime diagnostics for the sync pipeline.

The sync process moves a lot of data concurrently which makes it hard to see
at-a-glance progress or notice when workers stall.  This module provides a
small drop-in helper (`SyncDiagnostics`) that records progress events and
periodically writes structured snapshots to ``logs/diagnostics/runtime_diagnostics.jsonl``.

The snapshots can be tailed in real time while the sync runs to answer
questions like:
    - How many championships or matches have finished?
    - When was the last event emitted?
    - Is the MariaDB pool saturated?

Usage::

    diagnostics = SyncDiagnostics()
    await diagnostics.start()
    try:
        ...
        diagnostics.mark_progress("match", match_id)
    finally:
        await diagnostics.stop()

The helper is intentionally dependency-free and degrades gracefully when the
environment disables diagnostics (``SYNC_DIAGNOSTICS=0``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

LOGGER = logging.getLogger("pappaliiga.diagnostics")

def _coerce_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


_DEFAULT_LOG_DIR = Path(
    os.environ.get("SYNC_DIAGNOSTICS_LOG_DIR", Path(__file__).with_name("logs") / "diagnostics")
)
_DEFAULT_OUTPUT = Path(
    os.environ.get("SYNC_DIAGNOSTICS_PATH", _DEFAULT_LOG_DIR / "runtime_diagnostics.jsonl")
)
_DEFAULT_INTERVAL = max(1.0, _coerce_float(os.environ.get("SYNC_DIAGNOSTICS_INTERVAL"), 15.0))


@dataclass(slots=True)
class _ProgressEntry:
    count: int = 0
    last_id: Optional[str] = None
    last_ts: Optional[float] = None


class SyncDiagnostics:
    """Collect periodic runtime diagnostics for the sync pipeline."""

    def __init__(
        self,
        *,
        output_path: Path | str | None = None,
        interval_seconds: float = _DEFAULT_INTERVAL,
        include_pool_snapshot: bool = True,
    ) -> None:
        self.enabled = os.environ.get("SYNC_DIAGNOSTICS", "1") != "0"
        self.interval = max(1.0, interval_seconds)
        self.include_pool_snapshot = include_pool_snapshot
        self.output_path = Path(output_path or _DEFAULT_OUTPUT)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self._progress: Dict[str, _ProgressEntry] = {}
        self._lock = Lock()

        self._start_wall: Optional[float] = None
        self._start_monotonic: Optional[float] = None
        self._last_event_ts: Optional[float] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._hostname = socket.gethostname()
        self._pid = os.getpid()
        self._pool_error_logged = False

    async def start(self) -> None:
        """Start periodic diagnostics emission."""
        if not self.enabled:
            return
        if self._task and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._start_wall = time.time()
        self._start_monotonic = time.perf_counter()
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run(), name="sync-diagnostics")
        LOGGER.debug("Runtime diagnostics started (interval=%ss, output=%s)", self.interval, self.output_path)

    async def stop(self) -> None:
        """Stop diagnostics emission and flush a final snapshot."""
        if not self.enabled:
            return
        task = self._task
        if not task:
            return
        self._stop_event.set()
        try:
            await task
        finally:
            self._task = None
            LOGGER.debug("Runtime diagnostics stopped")

    def mark_progress(self, kind: str, identifier: Any | None = None) -> None:
        """Record progress for a logical unit (e.g., 'match' or 'championship')."""
        if not self.enabled:
            return
        key = (kind or "unknown").strip() or "unknown"
        ident = None if identifier is None else str(identifier)
        now = time.time()
        with self._lock:
            entry = self._progress.get(key)
            if not entry:
                entry = _ProgressEntry()
                self._progress[key] = entry
            entry.count += 1
            entry.last_id = ident
            entry.last_ts = now
            self._last_event_ts = now

    async def _run(self) -> None:
        try:
            while True:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval)
                    break
                except asyncio.TimeoutError:
                    await self._flush(final=False)
        except asyncio.CancelledError:
            raise
        finally:
            await self._flush(final=True)

    async def _flush(self, *, final: bool) -> None:
        if not self.enabled:
            return
        snapshot = self._build_snapshot(final=final)
        if self.include_pool_snapshot:
            pool_snapshot = await self._maybe_get_pool_snapshot()
            if pool_snapshot is not None:
                snapshot["db_pool"] = pool_snapshot
        payload = json.dumps(snapshot)
        try:
            await asyncio.to_thread(self._append_line, payload)
        except Exception as exc:  # pragma: no cover - best-effort logging
            LOGGER.warning("Failed to write diagnostics snapshot: %s", exc)

    def _build_snapshot(self, *, final: bool) -> Dict[str, Any]:
        now_wall = time.time()
        uptime = (
            (time.perf_counter() - self._start_monotonic) if self._start_monotonic is not None else 0.0
        )
        with self._lock:
            progress = {
                kind: {
                    "count": entry.count,
                    "last_id": entry.last_id,
                    "last_ts": entry.last_ts,
                }
                for kind, entry in self._progress.items()
            }
            last_event_ts = self._last_event_ts
        snapshot = {
            "ts": now_wall,
            "final": final,
            "uptime_seconds": round(uptime, 3),
            "hostname": self._hostname,
            "pid": self._pid,
            "progress": progress,
            "last_event_ts": last_event_ts,
        }
        if self._start_wall is not None:
            snapshot["started_at"] = self._start_wall
        return snapshot

    async def _maybe_get_pool_snapshot(self) -> Optional[Dict[str, Any]]:
        try:
            from db_async import get_pool_snapshot
        except Exception:
            return None
        try:
            return await get_pool_snapshot()
        except Exception as exc:
            if not self._pool_error_logged:
                LOGGER.debug("DB pool snapshot failed: %s", exc)
                self._pool_error_logged = True
            return None

    def _append_line(self, payload: str) -> None:
        with self.output_path.open("a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.write("\n")


__all__ = ["SyncDiagnostics"]
