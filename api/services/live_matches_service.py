"""Live Faceit matches for the Pappaliiga organization."""
from __future__ import annotations

import asyncio
from typing import Any

from faceit_config import PAPPALIIGA_ORG_ID
from faceit_client_async import (
    get_championship_matches_async,
    list_championships_for_organizer_async,
)

from api.utils.cache import AsyncTTLCache

_LIVE_MATCHES_CACHE = AsyncTTLCache(ttl_seconds=300, maxsize=1)
_CHAMPIONSHIP_FETCH_CONCURRENCY = 4


def _integer_score(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_match(
    match: dict[str, Any],
    championship_id: str,
    championship_name: str,
) -> dict[str, Any] | None:
    match_id = match.get("match_id") or match.get("id")
    teams = match.get("teams") or {}
    faction_keys = list(teams)[:2]
    if not match_id or len(faction_keys) < 2:
        return None

    left_key, right_key = faction_keys
    left_team = teams.get(left_key) or {}
    right_team = teams.get(right_key) or {}
    scores = (match.get("results") or {}).get("score") or {}

    return {
        "match_id": str(match_id),
        "championship_id": match.get("championship_id") or championship_id,
        "championship_name": championship_name,
        "status": match.get("status"),
        "scheduled_at": match.get("scheduled_at"),
        "started_at": match.get("started_at"),
        "faceit_url": f"https://www.faceit.com/cs2/room/{match_id}",
        "team1": {
            "id": left_team.get("faction_id") or left_team.get("id"),
            "name": left_team.get("name") or "Joukkue 1",
            "avatar": left_team.get("avatar"),
        },
        "team2": {
            "id": right_team.get("faction_id") or right_team.get("id"),
            "name": right_team.get("name") or "Joukkue 2",
            "avatar": right_team.get("avatar"),
        },
        "team1_score": _integer_score(scores.get(left_key)),
        "team2_score": _integer_score(scores.get(right_key)),
    }


async def get_live_organization_matches() -> list[dict[str, Any]]:
    """Return all currently ongoing matches in Pappaliiga's Faceit organization."""

    async def producer() -> list[dict[str, Any]]:
        championships = await list_championships_for_organizer_async(PAPPALIIGA_ORG_ID)
        semaphore = asyncio.Semaphore(_CHAMPIONSHIP_FETCH_CONCURRENCY)

        async def fetch_championship(
            championship: dict[str, Any],
        ) -> tuple[str, str, list[dict[str, Any]]]:
            championship_id = championship.get("championship_id") or championship.get("id")
            if not championship_id:
                return "", "", []
            championship_name = str(championship.get("name") or "Pappaliiga")
            async with semaphore:
                matches = await get_championship_matches_async(
                    str(championship_id),
                    match_type="ongoing",
                )
            return str(championship_id), championship_name, matches

        championship_matches = await asyncio.gather(
            *(fetch_championship(championship) for championship in championships)
        )
        normalized = [
            item
            for championship_id, championship_name, matches in championship_matches
            for match in matches
            if (
                item := _normalize_match(
                    match,
                    championship_id,
                    championship_name,
                )
            ) is not None
        ]
        return sorted(
            normalized,
            key=lambda item: (
                item.get("started_at") or item.get("scheduled_at") or 0,
                item["match_id"],
            ),
        )

    matches, _ = await _LIVE_MATCHES_CACHE.get_or_set("pappaliiga-ongoing", producer)
    return matches
