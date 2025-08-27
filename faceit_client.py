# Simple Faceit client. No heavy abstractions, just practical functions.
import time
import requests
from typing import Dict, Any, List, Optional
from faceit_config import API_KEY, OPEN_BASE, DEMOCRACY_BASE, REQUEST_TIMEOUT, RATE_LIMIT_SLEEP, MAX_RETRIES

HEADERS_OPEN = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_KEY}" if API_KEY else "",
}
HEADERS_DEMOCRACY = {
    "Accept": "application/json",
    # Democracy history endpoint does not need Authorization for read.
    # We still send a UA.
    "User-Agent": "cs2-faceit-reports/1.0",
}

def _get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                time.sleep(max(RATE_LIMIT_SLEEP, 1.0))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            time.sleep(RATE_LIMIT_SLEEP * (i + 1))
    raise RuntimeError(f"GET failed for {url}: {last_err}")

# --- Open Data v4 endpoints ---

def list_championship_matches(championship_id: str, match_type: str = "past", limit: int = 100) -> List[Dict[str, Any]]:
    # Paginates until all items fetched.
    # `match_type` can be 'past', 'upcoming', 'live' per Open API.
    url = f"{OPEN_BASE}/championships/{championship_id}/matches"
    items: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"type": match_type, "offset": offset, "limit": limit}
        data = _get(url, HEADERS_OPEN, params=params)
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(RATE_LIMIT_SLEEP)
    return items

def get_match_details(match_id: str) -> Dict[str, Any]:
    url = f"{OPEN_BASE}/matches/{match_id}"
    return _get(url, HEADERS_OPEN)

def get_match_stats(match_id: str) -> Dict[str, Any]:
    # per Open v4 docs: /matches/{match_id}/stats
    url = f"{OPEN_BASE}/matches/{match_id}/stats"
    return _get(url, HEADERS_OPEN)

# --- Democracy history (veto tickets, including map drops/picks) ---

def get_democracy_history(match_id: str) -> Dict[str, Any]:
    url = f"{DEMOCRACY_BASE}/match/{match_id}/history"
    return _get(url, HEADERS_DEMOCRACY)
