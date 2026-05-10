"""Division-page bundle endpoint — returns all data the division view needs in one request."""
from __future__ import annotations

import asyncio
import math
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.services import divisions_service, matches_service
from api.services.cache_helpers import GLOBAL_CACHE, get_championship_revision

router = APIRouter()


@router.get("/division-page/{championship_id}")
async def get_division_page(championship_id: str) -> Dict[str, Any]:
    """Return division details, match list, and map catalog in a single response.

    Replaces the two separate API calls the division view previously made:
      GET /api/divisions/{championship_id}
      GET /api/matches/division/{championship_id}
    """
    revision = await get_championship_revision(championship_id)
    cache_key = ("division-page", championship_id, revision)

    async def producer() -> Dict[str, Any]:
        try:
            champ_row = await divisions_service.fetch_division_by_id(championship_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        details_task = asyncio.create_task(divisions_service.get_division_details(champ_row))
        matches_task = asyncio.create_task(
            matches_service.get_division_matches(championship_id, limit=500, offset=0)
        )

        details, matches_result = await asyncio.gather(
            details_task, matches_task, return_exceptions=True
        )

        if isinstance(details, Exception):
            details = {}
        if isinstance(matches_result, Exception):
            matches_items = []
        else:
            matches_items = matches_result[0] if isinstance(matches_result, tuple) else []

        bracket = None
        if details and details.get("is_playoff"):
            bracket = _build_bracket(matches_items)

        return {
            "ok": True,
            "details": details,
            "matches": matches_items,
            "bracket": bracket,
        }

    cached_value, _ = await GLOBAL_CACHE.get_or_set(cache_key, producer)
    return cached_value


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _match_sort_key(match_row: dict[str, Any], round_number: int | None = None) -> tuple:
    """Sort key for playoff matches.
    
    For round 1 matches in 8-team brackets, prioritizes bracket_round_position (0-3).
    For other rounds or when bracket_round_position is missing, falls back to scheduled_at/match_id.
    """
    match_id = str(match_row.get("match_id") or "")
    bracket_pos = match_row.get("bracket_round_position")
    scheduled = _to_int(match_row.get("scheduled_at"))
    
    # If round 1 and bracket position is available, use it as primary sort key
    if round_number == 1 and bracket_pos is not None:
        try:
            return (0, int(bracket_pos), match_id)
        except (ValueError, TypeError):
            pass
    
    # Fallback: sort by scheduled_at, then match_id
    if scheduled is not None and scheduled > 0:
        return (1, scheduled, match_id)
    return (2, match_id)


def _build_bracket(matches: list[dict]) -> dict:
    """Group playoff matches into bracket rounds, padding empty TBD rounds at the end.
    
    Uses bracket_round_position for round 1 ordering when available (seed-based seeding).
    Falls back to scheduled_at/match_id ordering for rounds without position data or non-round-1.
    """
    rounds_map: dict[int, list] = {}
    for m in matches:
        rn = m.get("round_number") or m.get("roundNumber")
        key = int(rn) if rn is not None else 0
        rounds_map.setdefault(key, []).append(m)

    # Sort matches within each round
    for round_key, ms in rounds_map.items():
        ms.sort(key=lambda m: _match_sort_key(m, round_number=round_key))

    sorted_keys = sorted(k for k in rounds_map if k > 0) + ([0] if 0 in rounds_map else [])
    existing_real: list[int] = [k for k in sorted_keys if k > 0]

    # Determine expected total rounds from the first round's match count
    first_key = existing_real[0] if existing_real else 0
    count_round1 = len(rounds_map.get(first_key, []))
    if count_round1 > 1:
        expected_total = math.ceil(math.log2(count_round1)) + 1
    else:
        expected_total = max(len(existing_real), 1)

    # Pad with empty rounds until we reach the expected total
    while len(existing_real) < expected_total:
        next_k = (max(existing_real) + 1) if existing_real else 1
        sorted_keys.append(next_k)
        rounds_map[next_k] = []
        existing_real.append(next_k)

    total_real = len(existing_real)

    def _round_label(round_key: int, position: int) -> str:
        if round_key == 0:
            return "Tuntematon kierros"
        from_end = total_real - 1 - position
        if from_end == 0:
            return "Finaali"
        if from_end == 1:
            return "Välierät"
        return f"Kierros {round_key}"

    rounds = []
    for idx, key in enumerate(sorted_keys):
        ms = rounds_map.get(key, [])
        best_of = ms[0].get("best_of") if ms else None
        # Expected match count for this round
        exp = max(1, count_round1 // (2 ** idx)) if count_round1 > 0 else 1
        rounds.append({
            "round_number": key if key > 0 else None,
            "label": _round_label(key, idx),
            "best_of": best_of,
            "match_count_expected": exp,
            "matches": ms,
        })

    return {"rounds": rounds}
