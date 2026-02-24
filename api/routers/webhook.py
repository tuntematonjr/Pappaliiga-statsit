from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from api.services.sync_event_queue import get_sync_event_queue
from api.utils.webhook_helpers import (
    append_event_to_log,
    extract_match_id,
    is_supported_event,
    json_or_text,
    redact_headers,
)

LOGGER = logging.getLogger(__name__)

WEBHOOK_TOKEN = (os.getenv("FACEIT_WEBHOOK_TOKEN") or "").strip()
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs" / "faceit_webhooks"
LOG_DIR = Path(os.getenv("FACEIT_WEBHOOK_LOG_DIR") or DEFAULT_LOG_DIR).expanduser()
MAX_BODY_BYTES = int(os.getenv("FACEIT_WEBHOOK_MAX_BODY_BYTES", str(2 * 1024 * 1024)))
SYNC_ENABLED = (os.getenv("FACEIT_WEBHOOK_SYNC_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"})

router = APIRouter()


@router.api_route("/webhook/faceit", methods=["GET", "POST"])
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

    headers = redact_headers(dict(request.headers))
    content_type = request.headers.get("content-type")
    body_json, body_text = json_or_text(body, content_type)

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
        stored_at = await append_event_to_log(record, LOG_DIR)
    except Exception:
        LOGGER.exception("Failed to persist webhook event in %s", LOG_DIR)
        raise HTTPException(status_code=500, detail="Failed to persist webhook event")

    match_id: str | None = None
    sync_queued = False
    sync_reason: str | None = None

    if SYNC_ENABLED and request.method.upper() == "POST" and body_json is not None:
        if is_supported_event(body_json):
            match_id = extract_match_id(body_json)
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
        else:
            event_type = body_json.get("event", "unknown") if isinstance(body_json, dict) else "unknown"
            sync_reason = f"unsupported_event_type:{event_type}"

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
