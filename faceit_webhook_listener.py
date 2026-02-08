from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiofiles
from fastapi import FastAPI, Header, HTTPException, Request

logger = logging.getLogger("faceit.webhook")
logging.basicConfig(level=os.getenv("WEBHOOK_LOG_LEVEL", "INFO"))

WEBHOOK_TOKEN = (os.getenv("FACEIT_WEBHOOK_TOKEN") or "").strip()
LOG_DIR = Path(os.getenv("FACEIT_WEBHOOK_LOG_DIR", "logs/faceit_webhooks")).expanduser()
MAX_BODY_BYTES = int(os.getenv("FACEIT_WEBHOOK_MAX_BODY_BYTES", str(2 * 1024 * 1024)))
REDACTED_HEADERS = {"authorization", "x-api-key", "x-faceit-api-key"}

app = FastAPI(
    title="FACEIT Webhook Listener",
    version="1.0.0",
    description="Self-hosted endpoint for capturing and inspecting FACEIT webhook payloads.",
)


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


async def _append_event(record: dict[str, Any]) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("%Y-%m-%d.ndjson")
    target = LOG_DIR / filename
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    async with aiofiles.open(target, "a", encoding="utf-8") as handle:
        await handle.write(line + "\n")
    return str(target)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "listener": "faceit", "log_dir": str(LOG_DIR)}


@app.api_route("/webhook/faceit", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def faceit_webhook(
    request: Request,
    x_webhook_token: str | None = Header(default=None, alias="X-Webhook-Token"),
) -> dict[str, Any]:
    if WEBHOOK_TOKEN:
        provided = (x_webhook_token or "").strip()
        if provided != WEBHOOK_TOKEN:
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
    stored_at = await _append_event(record)

    logger.info(
        "Webhook captured method=%s bytes=%d file=%s",
        request.method,
        len(body),
        stored_at,
    )

    return {
        "ok": True,
        "stored_at": stored_at,
        "received_at": record["received_at"],
        "body_bytes": record["body_bytes"],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "faceit_webhook_listener:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8010")),
        reload=False,
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info"),
    )
