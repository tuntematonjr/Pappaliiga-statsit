from __future__ import annotations

import hmac
import os

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from api.services.sync_event_queue import get_sync_event_queue


router = APIRouter()


def _require_sync_token(x_sync_event_token: str | None = Header(default=None, alias="X-Sync-Event-Token")) -> None:
    expected = (os.getenv("SYNC_EVENT_TOKEN") or "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Sync event routes are not configured")
    provided = (x_sync_event_token or "").strip()
    if not provided or not hmac.compare_digest(provided, expected):
        raise HTTPException(status_code=403, detail="Forbidden")


class SyncEventPayload(BaseModel):
    match_id: str | None = None
    championship_id: str | None = None
    full: bool = False


@router.post("/api/sync/events", dependencies=[Depends(_require_sync_token)])
async def enqueue_sync_event(payload: SyncEventPayload) -> dict[str, object]:
    match_id = (payload.match_id or "").strip()
    championship_id = (payload.championship_id or "").strip()
    if bool(match_id) == bool(championship_id):
        raise HTTPException(
            status_code=400,
            detail="Provide exactly one of 'match_id' or 'championship_id'",
        )

    queue = get_sync_event_queue()
    if match_id:
        accepted = await queue.enqueue_match(match_id)
        kind = "match"
        target = match_id
    else:
        accepted = await queue.enqueue_championship(championship_id, full=payload.full)
        kind = "championship"
        target = championship_id

    return {
        "ok": True,
        "accepted": accepted,
        "deduped": not accepted,
        "kind": kind,
        "target_id": target,
        "full": bool(payload.full),
        "queue": queue.stats(),
    }


@router.get("/api/sync/events/status", dependencies=[Depends(_require_sync_token)])
async def sync_event_status() -> dict[str, object]:
    queue = get_sync_event_queue()
    return {"ok": True, "queue": queue.stats()}

