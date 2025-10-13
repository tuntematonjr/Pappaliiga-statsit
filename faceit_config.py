# All comments in English per user preference.
# Practical config. You can hardcode divisions here or load them from JSON if you like.
# API key is read from env FACEIT_API_KEY to avoid committing secrets.
# Keep things simple and explicit.

import os, json
from pathlib import Path
from env_loader import load_env

load_env(Path(__file__).parent)
API_KEY = os.environ.get("FACEIT_API_KEY", "").strip()

CURRENT_SEASON = 11
TOOL_VERSION = 0.6

# Base URLs (public Open Data v4 + Democracy history for vetoes).
OPEN_BASE = "https://open.faceit.com/data/v4"
DEMOCRACY_BASE = "https://www.faceit.com/api/democracy/v1"

# Organizer ID for Pappaliiga (fixed, no need to search every time)
PAPPALIIGA_ORG_ID = "1bfc69fa-5a21-4ed9-9ef3-37edbd7210d8"

DIVISIONS_JSON = Path(__file__).with_name("divisions.json")
DIVISIONS = []
if DIVISIONS_JSON.exists():
    try:
        with open(DIVISIONS_JSON, "r", encoding="utf-8") as f:
            DIVISIONS = json.load(f)
    except Exception as exc:
        # Be tolerant: if divisions.json is temporarily malformed (comments/omitted sections),
        # warn and continue with an empty list so tools (generator) can run.
        print(f"Warning: failed to parse {DIVISIONS_JSON}: {exc}. Continuing with empty DIVISIONS.")
        DIVISIONS = []
