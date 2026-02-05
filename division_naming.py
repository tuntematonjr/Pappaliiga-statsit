from __future__ import annotations

from typing import Any, Optional


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_division_name(season: Any, division_num: Any, is_playoffs: Any = False) -> str:
    season_num = _as_int(season)
    div_num = _as_int(division_num)

    if div_num == 0:
        base = "Mestaruussarja"
    elif div_num is None:
        base = "Divisioona"
    else:
        base = f"{div_num} Divisioona"

    if bool(is_playoffs):
        base = f"{base} Playoffs"

    if season_num and season_num > 0:
        base = f"{base} S{season_num}"

    return base
