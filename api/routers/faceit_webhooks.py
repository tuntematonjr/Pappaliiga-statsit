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

    first_seen = await _mark_seen_event_id(event_id)
    if not first_seen:
        return {
            "ok": True,
            "accepted": False,
            "deduped": True,
            "reason": "duplicate_event_id",
            "event": webhook_event,
            "event_id": event_id,
            "queue": get_sync_event_queue().stats(),
        }

    organizer_id = str(payload.get("organizer_id") or "").strip()
    if _ORGANIZER_ID and organizer_id and organizer_id != _ORGANIZER_ID:
        return {
            "ok": True,
            "accepted": False,
            "reason": "organizer_mismatch",
            "event": webhook_event,
            "event_id": event_id,
            "organizer_id": organizer_id,
            "expected_organizer_id": _ORGANIZER_ID,
        }

    queue = get_sync_event_queue()
    accepted = False
    target_id = ""
    kind = ""

    if webhook_event.startswith("match_status_"):
        target_id = str(payload.get("id") or "").strip()
        if target_id:
            accepted = await queue.enqueue_match(target_id)
            kind = "match"
    elif webhook_event.startswith("championship_status_"):
        entity = payload.get("entity")
        entity = entity if isinstance(entity, dict) else {}
        target_id = str(entity.get("id") or "").strip()
        if target_id:
            accepted = await queue.enqueue_championship(target_id, full=False)
            kind = "championship"

    record_path = None
    if _LOG_RAW:
        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "event": webhook_event,
            "event_id": event_id,
            "retry_count": retry_count,
            "accepted": accepted,
            "kind": kind,
            "target_id": target_id,
            "headers": _redact_headers(dict(request.headers)),
            "body": event,
        }
        record_path = await _append_raw_event(record)

    if not kind or not target_id:
        return {
            "ok": True,
            "accepted": False,
            "reason": "unsupported_or_missing_target",
            "event": webhook_event,
            "event_id": event_id,
            "queue": queue.stats(),
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
        "queue": queue.stats(),
        "logged_to": record_path,
    }
