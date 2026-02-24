from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any

import aiofiles
from fastapi import APIRouter, Header, HTTPException, Request

from api.services.sync_event_queue import get_sync_event_queue

LOGGER = logging.getLogger(__name__)

WEBHOOK_TOKEN = (os.getenv("FACEIT_WEBHOOK_TOKEN") or "").strip()
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs" / "faceit_webhooks"
LOG_DIR = Path(os.getenv("FACEIT_WEBHOOK_LOG_DIR") or DEFAULT_LOG_DIR).expanduser()
MAX_BODY_BYTES = int(os.getenv("FACEIT_WEBHOOK_MAX_BODY_BYTES", str(2 * 1024 * 1024)))
SYNC_ENABLED = (os.getenv("FACEIT_WEBHOOK_SYNC_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"})
REDACTED_HEADERS = {"authorization", "x-api-key", "x-faceit-api-key"}

router = APIRouter()


def _redact_headers(headers: dict[str, str]) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in REDACTED_HEADERS:
            cleaned[key] = "***redacted***"
        else:
            cleaned[key] = value
    return cleaned


def _json_or_text(body: bytes, content_type: str | None) -> tuple[Any | None, str | None]:
    if not body:
        return None, None

    text = body.decode("utf-8", errors="replace")
    ctype = (content_type or "").lower()
    if "json" in ctype:
        try:
            return json.loads(text), None
        except json.JSONDecodeError:
            return None, text

    return None, text


def _extract_match_id(payload: Any) -> str | None:
    if isinstance(payload, dict):
        direct = payload.get("match_id") or payload.get("matchId")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        nested_payload = payload.get("payload")
        if nested_payload is not None:
            nested = _extract_match_id(nested_payload)
            if nested:
                return nested

        for value in payload.values():
            nested = _extract_match_id(value)
            if nested:
                return nested

    if isinstance(payload, list):
        for item in payload:
            nested = _extract_match_id(item)
            if nested:
                return nested

    return None


async def _append_event(record: dict[str, Any]) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("%Y-%m-%d.ndjson")
    target = LOG_DIR / filename
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    async with aiofiles.open(target, "a", encoding="utf-8") as handle:
        await handle.write(line + "\n")
    return str(target)


@router.api_route("/webhook/faceit", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def faceit_webhook(
    request: Request,
    x_webhook_token: str | None = Header(default=None, alias="X-Webhook-Token"),
) -> dict[str, Any]:
    if WEBHOOK_TOKEN:
        provided = (x_webhook_token or "").strip()
        if provided != WEBHOOK_TOKEN:
            LOGGER.warning(
                "Rejected webhook request due to token mismatch method=%s path=%s remote=%s",
                request.method,
                request.url.path,
                request.client.host if request.client else None,
            )
            raise HTTPException(status_code=403, detail="Forbidden")

    body = await request.body()
    if len(body) > MAX_BODY_BYTES:
        raise HTTPException(status_code=413, detail="Payload too large")

    headers = _redact_headers(dict(request.headers))
    content_type = request.headers.get("content-type")
    body_json, body_text = _json_or_text(body, content_type)

    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "method": request.method,
        "path": request.url.path,
        "query": dict(request.query_params),
        "content_type": content_type,
        "headers": headers,
        "body_bytes": len(body),
        "body_json": body_json,
        "body_text": body_text,
    }

    try:
        stored_at = await _append_event(record)
    except Exception:
        LOGGER.exception("Failed to persist webhook event in %s", LOG_DIR)
        raise HTTPException(status_code=500, detail="Failed to persist webhook event")

    match_id: str | None = None
    sync_queued = False
    sync_reason: str | None = None

    if SYNC_ENABLED and request.method.upper() == "POST" and body_json is not None:
        match_id = _extract_match_id(body_json)
        if match_id:
            try:
                sync_queued = await get_sync_event_queue().enqueue_match(match_id)
                if not sync_queued:
                    sync_reason = "deduped_or_already_processing"
            except Exception as exc:
                sync_reason = f"enqueue_failed:{type(exc).__name__}"
                LOGGER.exception("Failed to enqueue match sync for %s", match_id)
        else:
            sync_reason = "no_match_id_in_payload"

    LOGGER.info(
        "Webhook captured method=%s bytes=%d file=%s match_id=%s queued=%s reason=%s",
        request.method,
        len(body),
        stored_at,
        match_id,
        sync_queued,
        sync_reason,
    )

    return {
        "ok": True,
        "stored_at": stored_at,
        "received_at": record["received_at"],
        "body_bytes": record["body_bytes"],
        "match_id": match_id,
        "sync_queued": sync_queued,
        "sync_reason": sync_reason,
    }


@router.get("/webhook/health")
async def webhook_health() -> dict[str, Any]:
    return {
        "ok": True,
        "listener": "faceit",
        "log_dir": str(LOG_DIR),
        "sync_enabled": SYNC_ENABLED,
        "token_required": bool(WEBHOOK_TOKEN),
        "max_body_bytes": MAX_BODY_BYTES,
    }
