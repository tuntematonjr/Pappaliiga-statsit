from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_player_stats_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a player_stats row into the legacy stats object used by API responses."""
    return {
        "Kills": row.get("kills") or 0,
        "Deaths": row.get("deaths") or 0,
        "Assists": row.get("assists") or 0,
        "MVPs": row.get("mvps") or 0,
        "Headshots": row.get("headshots") or 0,
        "Damage": row.get("damage") or 0,
        "Sniper Kills": row.get("sniper_kills") or 0,
        "Pistol Kills": row.get("pistol_kills") or 0,
        "Knife Kills": row.get("knife_kills") or 0,
        "Zeus Kills": row.get("zeus_kills") or 0,
        "First Kills": row.get("first_kills") or 0,
        "Enemies Flashed": row.get("enemies_flashed") or 0,
        "Flash Count": row.get("flash_count") or 0,
        "Flash Successes": row.get("flash_successes") or 0,
        "Utility Damage": row.get("utility_damage") or 0,
        "Utility Count": row.get("utility_count") or 0,
        "Utility Successes": row.get("utility_successes") or 0,
        "Utility Enemies": row.get("utility_enemies") or 0,
        "Double Kills": row.get("mk_2k") or 0,
        "Triple Kills": row.get("mk_3k") or 0,
        "Quadro Kills": row.get("mk_4k") or 0,
        "Penta Kills": row.get("mk_5k") or 0,
        "Clutch Kills": row.get("clutch_kills") or 0,
        "1v1Count": row.get("cl_1v1_attempts") or 0,
        "1v1Wins": row.get("cl_1v1_wins") or 0,
        "1v2Count": row.get("cl_1v2_attempts") or 0,
        "1v2Wins": row.get("cl_1v2_wins") or 0,
        "Entry Count": row.get("entry_count") or 0,
        "Entry Wins": row.get("entry_wins") or 0,
        "K/D Ratio": row.get("kd") or 0.0,
        "K/R Ratio": row.get("kr") or 0.0,
        "ADR": row.get("adr") or 0.0,
        "Headshots %": row.get("hs_pct") or 0.0,
        "Result": row.get("result") or 0,
    }
