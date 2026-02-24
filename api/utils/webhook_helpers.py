"""Shared webhook utilities for both API router and standalone listener."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any

import aiofiles

LOGGER = logging.getLogger(__name__)
REDACTED_HEADERS = {"authorization", "x-api-key", "x-faceit-api-key"}


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Redact sensitive headers for logging."""
    cleaned: dict[str, str] = {}
    for key, value in headers.items():
        if key.lower() in REDACTED_HEADERS:
            cleaned[key] = "***redacted***"
        else:
            cleaned[key] = value
    return cleaned


def json_or_text(body: bytes, content_type: str | None) -> tuple[Any | None, str | None]:
    """Parse body as JSON if content-type indicates it, otherwise return as text."""
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


def extract_match_id(payload: Any) -> str | None:
    """Extract match_id from Faceit webhook payload.
    
    Faceit structure:
      Direct (some webhooks): payload["match_id"]
      Nested (most webhooks): payload["payload"]["id"]
    """
    if not isinstance(payload, dict):
        return None
    
    # Try direct match_id field
    match_id = payload.get("match_id") or payload.get("matchId")
    if isinstance(match_id, str) and (mid := match_id.strip()):
        return mid
    
    # Try nested payload.id (most common Faceit structure)
    nested = payload.get("payload")
    if isinstance(nested, dict):
        match_id = nested.get("id")
        if isinstance(match_id, str) and (mid := match_id.strip()):
            return mid
    
    return None


def is_supported_event(payload: Any) -> bool:
    """Check if event type should trigger sync.
    
    Supported events:
      - match_status_finished: match completed (stats available)
    
    Why only match_status_finished?
      - Guarantees match is complete and stats are ready
      - Avoids duplicate syncs (demo_ready may arrive separately)
      - match_demo_ready alone doesn't guarantee stats are synced
    """
    if not isinstance(payload, dict):
        return False
    event = payload.get("event", "").strip().lower()
    return event == "match_status_finished"


async def append_event_to_log(
    record: dict[str, Any],
    log_dir: Path,
) -> str:
    """Appended JSON-formatted event record to daily ndjson log file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    filename = datetime.now(timezone.utc).strftime("%Y-%m-%d.ndjson")
    target = log_dir / filename
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    try:
        async with aiofiles.open(target, "a", encoding="utf-8") as handle:
            await handle.write(line + "\n")
    except Exception as exc:
        LOGGER.exception("Failed to write webhook event log to %s", target)
        raise
    return str(target)
