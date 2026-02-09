from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiofiles
from fastapi import APIRouter, HTTPException, Request

import faceit_config
from api.services.sync_event_queue import get_sync_event_queue

router = APIRouter()
logger = logging.getLogger(__name__)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


_ORGANIZER_ID = (os.getenv("FACEIT_WEBHOOK_ORGANIZER_ID") or faceit_config.PAPPALIIGA_ORG_ID).strip()
_LOG_DIR = Path(os.getenv("FACEIT_WEBHOOK_LOG_DIR", "logs/faceit_webhooks")).expanduser()
_LOG_RAW = (os.getenv("FACEIT_WEBHOOK_LOG_RAW") or "1").strip().lower() in {"1", "true", "yes", "on"}
_DEDUPE_TTL_SECONDS = max(60, _int_env("FACEIT_WEBHOOK_DEDUPE_TTL", 21600))
_MATCH_SYNC_EVENT = "match_status_finished"
_SEEN_EVENT_IDS: dict[str, float] = {}
_SEEN_LOCK = asyncio.Lock()
_REDACTED_HEADERS = {"authorization", "x-sync-event-token"}


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in _REDACTED_HEADERS:
            cleaned[key] = "***redacted***"
        else:
            cleaned[key] = value
    return cleaned


async def _mark_seen_event_id(event_id: str) -> bool:
    if not event_id:
        return True
    now = time.monotonic()
    async with _SEEN_LOCK:
        expired = [key for key, expiry in _SEEN_EVENT_IDS.items() if expiry <= now]
        for key in expired:
            _SEEN_EVENT_IDS.pop(key, None)
        if event_id in _SEEN_EVENT_IDS:
            return False
        _SEEN_EVENT_IDS[event_id] = now + _DEDUPE_TTL_SECONDS
    return True


async def _append_raw_event(record: dict[str, Any]) -> str:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("%Y-%m-%d.ndjson")
    target = _LOG_DIR / filename
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    async with aiofiles.open(target, "a", encoding="utf-8") as handle:
        await handle.write(line + "\n")
    return str(target)


@router.get("/webhook/faceit")
@router.get("/api/webhooks/faceit")
async def faceit_webhook_status() -> dict[str, Any]:
    return {
        "ok": True,
        "listener": "faceit",
        "organizer_id": _ORGANIZER_ID,
        "match_sync_event": _MATCH_SYNC_EVENT,
        "sync_queue": get_sync_event_queue().stats(),
        "dedupe_cache_size": len(_SEEN_EVENT_IDS),
    }


@router.post("/webhook/faceit")
@router.post("/api/webhooks/faceit")
async def receive_faceit_webhook(
    request: Request,
) -> dict[str, Any]:
    try:
        event: dict[str, Any] = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    webhook_event = str(event.get("event") or "").strip()
    event_id = str(event.get("event_id") or "").strip()
    retry_count = _safe_int(event.get("retry_count"), default=0)
    payload = event.get("payload")
    payload = payload if isinstance(payload, dict) else {}
    queue = get_sync_event_queue()

    first_seen = await _mark_seen_event_id(event_id)
    deduped = not first_seen
    accepted = False
    reason = ""
    target_id = ""
    kind = ""
    organizer_id = str(payload.get("organizer_id") or "").strip()
    if deduped:
        reason = "duplicate_event_id"
    elif _ORGANIZER_ID and organizer_id and organizer_id != _ORGANIZER_ID:
        reason = "organizer_mismatch"
    else:
        if webhook_event == _MATCH_SYNC_EVENT:
            target_id = str(payload.get("id") or "").strip()
            if target_id:
                accepted = await queue.enqueue_match(target_id)
                kind = "match"
            else:
                reason = "missing_match_id"
        else:
            reason = "unsupported_event"

    if not kind and not reason:
        reason = "unsupported_or_missing_target"

    queue_snapshot = queue.stats()
    record_path = None
    if _LOG_RAW:
        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "event": webhook_event,
            "event_id": event_id,
            "retry_count": retry_count,
            "accepted": accepted,
            "deduped": deduped,
            "reason": reason or None,
            "kind": kind,
            "target_id": target_id,
            "queue": queue_snapshot,
            "headers": _redact_headers(dict(request.headers)),
            "body": event,
        }
        record_path = await _append_raw_event(record)

    if reason:
        return {
            "ok": True,
            "accepted": accepted,
            "deduped": deduped,
            "reason": reason,
            "event": webhook_event,
            "event_id": event_id,
            "organizer_id": organizer_id or None,
            "expected_organizer_id": _ORGANIZER_ID or None,
            "queue": queue_snapshot,
            "logged_to": record_path,
        }

    logger.info(
        "FACEIT webhook received event=%s event_id=%s target=%s accepted=%s retry=%d",
        webhook_event,
        event_id,
        target_id,
        accepted,
        retry_count,
    )
    return {
        "ok": True,
        "accepted": accepted,
        "deduped": not accepted,
        "event": webhook_event,
        "event_id": event_id,
        "kind": kind,
        "target_id": target_id,
        "queue": queue_snapshot,
        "logged_to": record_path,
    }
