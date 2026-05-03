"""Proxy for replay2.pappa.aukko.net to avoid CORS issues.

Browser requests come to /api/replay2/replays/{match_id}/status?map_id=N
and are forwarded server-side to https://replay2.pappa.aukko.net.
"""
from __future__ import annotations

import logging

import httpx
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

REPLAY2_BASE = "https://replay2.pappa.aukko.net"
FETCH_TIMEOUT = 8  # seconds

router = APIRouter()


@router.get("/replay2/replays/{match_id}/status")
async def proxy_replay2_status(
    match_id: str = Path(..., max_length=128),
    map_id: int = Query(..., ge=1, le=20),
) -> JSONResponse:
    url = f"{REPLAY2_BASE}/replays/{match_id}/status"
    try:
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url, params={"map_id": map_id})
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Replay not found")
        if resp.status_code >= 500:
            raise HTTPException(status_code=502, detail="Replay server error")
        return JSONResponse(content=resp.json(), status_code=resp.status_code)
    except HTTPException:
        raise
    except Exception as exc:
        logger.debug("replay2 proxy error for %s map %s: %s", match_id, map_id, exc)
        raise HTTPException(status_code=502, detail="Could not reach replay server") from exc
