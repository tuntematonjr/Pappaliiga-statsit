# All comments in English per user preference.
# Practical config. You can hardcode divisions here or load them from JSON if you like.
# API key is read from env FACEIT_API_KEY to avoid committing secrets.
# Keep things simple and explicit.

import os
import json
from pathlib import Path
from typing import Any, List

from env_loader import load_env

load_env(Path(__file__).parent)
API_KEY = os.environ.get("FACEIT_API_KEY", "").strip()


def _int_env(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


# DB / concurrency tuning defaults (overridable via env)
DEFAULT_DB_POOL_MIN_SIZE = 2
DEFAULT_DB_POOL_MAX_SIZE = 30
DB_POOL_MIN_SIZE = _int_env("DB_POOL_MIN_SIZE", DEFAULT_DB_POOL_MIN_SIZE)
DB_POOL_MAX_SIZE = _int_env("DB_POOL_MAX_SIZE", DEFAULT_DB_POOL_MAX_SIZE)
DB_CONNECTIONS_PER_WORKER = _int_env("DB_CONNECTIONS_PER_WORKER", 3)
MAX_DB_WRITER_CONCURRENCY = _int_env("MAX_DB_WRITER_CONCURRENCY", 6)
MAX_MATCH_SYNC_CONCURRENCY = _int_env("MAX_MATCH_SYNC_CONCURRENCY", 4)

DEFAULT_CURRENT_SEASON = 12
CURRENT_SEASON = DEFAULT_CURRENT_SEASON
TOOL_VERSION = 0.6

# Base URLs (public Open Data v4 + Democracy history for vetoes).
OPEN_BASE = "https://open.faceit.com/data/v4"
DEMOCRACY_BASE = "https://www.faceit.com/api/democracy/v1"

# Organizer ID for Pappaliiga (fixed, no need to search every time)
PAPPALIIGA_ORG_ID = "1bfc69fa-5a21-4ed9-9ef3-37edbd7210d8"

DIVISIONS_JSON = Path(__file__).with_name("divisions.json")
DIVISIONS: List[dict[str, Any]] = []


def _compute_current_season(entries: List[dict[str, Any]]) -> int:
    seasons_regular: List[int] = []
    seasons_all: List[int] = []

    for entry in entries:
        raw = entry.get("season")
        if raw is None:
            continue
        try:
            season_value = int(raw)
        except (TypeError, ValueError):
            continue
        seasons_all.append(season_value)

        is_playoff = entry.get("is_playoffs") or entry.get("is_playoff")
        if isinstance(is_playoff, str):
            is_playoff = is_playoff.strip().lower() in {"1", "true", "yes"}
        if not is_playoff:
            seasons_regular.append(season_value)

    source = seasons_regular or seasons_all
    return max(source) if source else DEFAULT_CURRENT_SEASON


def update_divisions(divisions: List[dict[str, Any]]) -> List[dict[str, Any]]:
    """Update in-memory division cache and recompute current season."""

    global DIVISIONS, CURRENT_SEASON
    DIVISIONS = list(divisions)
    CURRENT_SEASON = _compute_current_season(DIVISIONS)
    return DIVISIONS


def reload_divisions(path: Path | None = None) -> List[dict[str, Any]]:
    """Reload ``divisions.json`` from disk, tolerating parse errors."""

    target = Path(path) if path is not None else DIVISIONS_JSON
    if not target.exists():
        return update_divisions([])

    try:
        with open(target, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception as exc:
        # Be tolerant: if divisions.json is temporarily malformed (comments/omitted sections),
        # warn and continue with an empty list so tools (generator) can run.
        print(f"Warning: failed to parse {target}: {exc}. Continuing with empty DIVISIONS.")
        return update_divisions([])

    if not isinstance(payload, list):
        print(f"Warning: expected list in {target}, found {type(payload).__name__}. Continuing with empty DIVISIONS.")
        return update_divisions([])

    return update_divisions(payload)


# Initialize cache on import.
reload_divisions()
