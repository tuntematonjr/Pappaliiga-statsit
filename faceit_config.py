# All comments in English per user preference.
# Practical config. You can hardcode divisions here or load them from JSON if you like.
# API key is read from env FACEIT_API_KEY to avoid committing secrets.
# Keep things simple and explicit.

import os
from pathlib import Path

API_KEY = os.environ.get("FACEIT_API_KEY", "").strip()

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

# Divisions you want to process.
# `division_id` is an internal number you choose for convenience in the DB / filenames.
# `name` is what appears in HTML.
# `championship_id` is from Faceit (copy from the championship page URL).
DIVISIONS = [
    {
        "division_id": 5,
        "name": "5 Divisioona S11",
        "slug": "div5",
        "championship_id": "2357bf41-ba0c-4405-aa2e-2c63d252d3f",
    },
    {
        "division_id": 19,
        "name": "19 Divisioona S11",
        "slug": "div19",
        "championship_id": "9279327e-d3c8-4300-bce5-40f377e1d2ff",
    },
    {
        "division_id": 20,
        "name": "20 Divisioona S11",
        "slug": "div20",
        "championship_id": "7ff31db2-456c-426d-adaa-7bc640a257eb",
    },
    {
        "division_id": 22,
        "name": "22 Divisioona S11",
        "slug": "div22",
        "championship_id": "3c5bd8b6-1ea7-4fbc-9340-bf2f7bbd9245",
    },
]

# Networking / rate limiting
REQUEST_TIMEOUT = 25  # seconds
RATE_LIMIT_SLEEP = 0.4  # seconds between calls, be a good citizen
MAX_RETRIES = 3

