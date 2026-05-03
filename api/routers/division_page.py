"""Division-page bundle endpoint — returns all data the division view needs in one request."""
from __future__ import annotations

import asyncio
import math
from typing import Any, Dict

from fastapi import APIRouter, HTTPException

from api.exceptions import NotFoundError
from api.services import divisions_service, matches_service
from db_async import query_async

router = APIRouter()


@router.get("/division-page/{championship_id}")
async def get_division_page(championship_id: str) -> Dict[str, Any]:
    """Return division details, match list, and map catalog in a single response.

    Replaces the two separate API calls the division view previously made:
      GET /api/divisions/{championship_id}
      GET /api/matches/division/{championship_id}
    """
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


def _build_bracket(matches: list[dict]) -> dict:
    """Group playoff matches into bracket rounds, padding empty TBD rounds at the end."""
    rounds_map: dict[int, list] = {}
    for m in matches:
        rn = m.get("round_number") or m.get("roundNumber")
        key = int(rn) if rn is not None else 0
        rounds_map.setdefault(key, []).append(m)

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
