from __future__ import annotations

import os

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from division_registry import refresh_divisions
import faceit_config

from db_async import close_pool, create_schema_async
from faceit_client_async import shutdown_clients
from sync_pipeline import update_single_match_async

app = FastAPI(title="Pappaliiga Faceit webhook", version="1.0.0")

_WEBHOOK_SECRET = os.environ.get("FACEIT_WEBHOOK_SECRET", "").strip()


class MatchUpdatePayload(BaseModel):
    match_id: str = Field(..., alias="matchId", description="Faceit match identifier")

    class Config:
        allow_population_by_field_name = True


class DivisionRefreshPayload(BaseModel):
    min_season: int | None = Field(default=faceit_config.DEFAULT_CURRENT_SEASON, alias="minSeason", ge=0)
    dry_run: bool = Field(default=False, alias="dryRun")
    require_matches: bool = Field(default=False, alias="requireMatches")
    min_matches: int = Field(default=0, alias="minMatches", ge=0)
    require_teams: bool = Field(default=True, alias="requireTeams")
    min_teams: int = Field(default=1, alias="minTeams", ge=0)

    class Config:
        allow_population_by_field_name = True


@app.on_event("startup")
async def startup() -> None:
    # Ensure schema exists before handling requests; safe to call repeatedly.
    await create_schema_async(force=False)


@app.on_event("shutdown")
async def shutdown() -> None:
    await shutdown_clients()
    await close_pool()


@app.post("/webhook/match-update")
async def handle_match_update(
    payload: MatchUpdatePayload,
    x_webhook_secret: str | None = Header(default=None, convert_underscores=False),
) -> dict[str, str | None]:
    if _WEBHOOK_SECRET:
        if not x_webhook_secret or x_webhook_secret.strip() != _WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    match_id = payload.match_id
    try:
        championship_id = await update_single_match_async(match_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to refresh match {match_id}: {exc}") from exc

    if championship_id is None:
        raise HTTPException(status_code=404, detail="Match not found or details unavailable")

    return {
        "status": "ok",
        "match_id": match_id,
        "championship_id": championship_id,
    }


@app.post("/webhook/divisions/refresh")
async def handle_division_refresh(
    payload: DivisionRefreshPayload,
    x_webhook_secret: str | None = Header(default=None, convert_underscores=False),
) -> dict[str, object]:
    if _WEBHOOK_SECRET:
        if not x_webhook_secret or x_webhook_secret.strip() != _WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    try:
        result = await refresh_divisions(
            min_season=payload.min_season if payload.min_season is not None else faceit_config.DEFAULT_CURRENT_SEASON,
            dry_run=payload.dry_run,
            require_matches=payload.require_matches,
            min_matches=payload.min_matches,
            min_new_division_teams=payload.min_teams if payload.require_teams else 0,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to refresh divisions: {exc}") from exc

    response = {
        "status": "ok",
        "dry_run": payload.dry_run,
        "result": result.to_dict(),
    }
    return response
