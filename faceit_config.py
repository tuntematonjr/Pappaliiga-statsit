# All comments in English per user preference.
# Practical config. You can hardcode divisions here or load them from JSON if you like.
# API key is read from env FACEIT_API_KEY to avoid committing secrets.
# Keep things simple and explicit.

import os, json
from pathlib import Path

API_KEY = os.environ.get("FACEIT_API_KEY", "").strip()

CURRENT_SEASON = 11
TOOL_VERSION = 0.2

# Lue .env vain jos ympäristömuuttuja puuttuu
if not API_KEY:
    try:
        from dotenv import load_dotenv
        dotenv_path = Path(__file__).with_name(".env")
        if dotenv_path.exists():
            load_dotenv(dotenv_path)
            API_KEY = os.environ.get("FACEIT_API_KEY", "").strip()
    except ImportError:
        pass

# Base URLs (public Open Data v4 + Democracy history for vetoes).
OPEN_BASE = "https://open.faceit.com/data/v4"
DEMOCRACY_BASE = "https://www.faceit.com/api/democracy/v1"

# Organizer ID for Pappaliiga (fixed, no need to search every time)
PAPPALIGA_ORG_ID = "1bfc69fa-5a21-4ed9-9ef3-37edbd7210d8"

BASE_SLEEP = 0.10
MAX_SLEEP = 1.50
BACKOFF_FACTOR = 1.75
RECOVER_FACTOR = 0.85
RECOVER_STEPS = 3

DIVISIONS_JSON = Path(__file__).with_name("divisions.json")
DIVISIONS = []
if DIVISIONS_JSON.exists():
    with open(DIVISIONS_JSON, "r", encoding="utf-8") as f:
        DIVISIONS = json.load(f)
