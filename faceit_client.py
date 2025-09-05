# faceit_client.py
# Faceit client with adaptive rate limiting (no legacy RATE_LIMIT_SLEEP/REQUEST_TIMEOUT/MAX_RETRIES).
# All comments in English.

from __future__ import annotations
import time
import requests
from typing import Dict, Any, List, Optional

from faceit_config import API_KEY, OPEN_BASE, DEMOCRACY_BASE

# Try to import adaptive tuning from config; fall back to sane defaults if missing.
try:
    from faceit_config import BASE_SLEEP, MAX_SLEEP, BACKOFF_FACTOR, RECOVER_FACTOR, RECOVER_STEPS
except Exception:
    BASE_SLEEP = 0.10
    MAX_SLEEP = 1.50
    BACKOFF_FACTOR = 1.75
    RECOVER_FACTOR = 0.85
    RECOVER_STEPS = 3

# Local HTTP timeout (seconds). Kept local on purpose; not part of config anymore.
DEFAULT_TIMEOUT = 20
ADAPT_MAX_RETRIES = 4  # total attempts per request

HEADERS_OPEN = {
    "Accept": "application/json",
    "Authorization": f"Bearer {API_KEY}" if API_KEY else "",
    "User-Agent": "pappaliiga-stats/1.0",
}
HEADERS_DEMOCRACY = {
    "Accept": "application/json",
    "User-Agent": "pappaliiga-stats/1.0",
}


class AdaptiveLimiter:
    """
    Adaptive sleeper:
      - Start at BASE_SLEEP
      - On 429 or exception: grow by BACKOFF_FACTOR (capped to MAX_SLEEP)
      - On successive successes: decay towards BASE_SLEEP by RECOVER_FACTOR every RECOVER_STEPS
    """
    def __init__(self, base: float, maxv: float, grow: float, recover: float, recover_steps: int):
        self.base = max(0.0, base)
        self.maxv = maxv
        self.grow = grow
        self.recover = recover
        self.recover_steps = max(1, recover_steps)
        self.cur = self.base
        self.ok_streak = 0

    def on_throttle(self) -> None:
        self.cur = min(self.maxv, max(self.cur, self.base) * self.grow)
        self.ok_streak = 0

    def on_error(self) -> None:
        self.cur = min(self.maxv, max(self.cur, self.base) * self.grow)
        self.ok_streak = 0

    def on_success(self) -> None:
        self.ok_streak += 1
        if self.ok_streak >= self.recover_steps and self.cur > self.base:
            self.cur = max(self.base, self.cur * self.recover)
            self.ok_streak = 0

    def sleep(self) -> None:
        if self.cur > 0:
            time.sleep(self.cur)


# One module-level limiter instance used by all calls
_ADAPT = AdaptiveLimiter(
    base=BASE_SLEEP, maxv=MAX_SLEEP, grow=BACKOFF_FACTOR,
    recover=RECOVER_FACTOR, recover_steps=RECOVER_STEPS
)


def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    """Parse Retry-After header if present (seconds)."""
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except Exception:
        return None


def _get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    last_err: Optional[Exception] = None
    for _ in range(ADAPT_MAX_RETRIES):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
            if resp.status_code == 429:
                _ADAPT.on_throttle()
                ra = _retry_after_seconds(resp)
                # Respect server hint if present; otherwise at least 1s, otherwise current adaptive delay.
                delay = max(1.0, ra if ra is not None else _ADAPT.cur)
                time.sleep(delay)
                continue
            resp.raise_for_status()
            _ADAPT.on_success()
            return resp.json()
        except Exception as e:
            last_err = e
            _ADAPT.on_error()
            _ADAPT.sleep()
    raise RuntimeError(f"GET failed for {url}: {last_err}")


# --- Open Data v4 endpoints ---

def list_championship_matches(championship_id: str, match_type: str = "all", limit: int = 100) -> List[Dict[str, Any]]:
    """
    Paginates until all items fetched.
    match_type ∈ {'all','past','upcoming','live','ongoing'}.
    - Faceit API hyväksyy 'ongoing' (alias 'live').
    - Jos 'all', tehdään kolme erillistä hakua ja yhdistetään tulokset.
    """
    if match_type == "all":
        out: List[Dict[str, Any]] = []
        for mt in ("past", "upcoming", "ongoing"):
            out.extend(list_championship_matches(championship_id, mt, limit=limit))
        return out

    assert match_type in {"past", "upcoming", "live", "ongoing"}
    if match_type == "live":
        match_type = "ongoing"

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
        _ADAPT.sleep()
    return items

def get_match_details(match_id: str) -> Dict[str, Any]:
    url = f"{OPEN_BASE}/matches/{match_id}"
    return _get(url, HEADERS_OPEN)


def get_match_stats(match_id: str) -> Dict[str, Any]:
    url = f"{OPEN_BASE}/matches/{match_id}/stats"
    return _get(url, HEADERS_OPEN)


# --- Democracy history (veto tickets, including map drops/picks) ---

def get_democracy_history(match_id: str) -> Dict[str, Any]:
    url = f"{DEMOCRACY_BASE}/match/{match_id}/history"
    return _get(url, HEADERS_DEMOCRACY)


def list_championships_for_organizer(organizer_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """List all championships for a given organizer_id."""
    url = f"{OPEN_BASE}/organizers/{organizer_id}/championships"
    items: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {"offset": offset, "limit": limit}
        data = _get(url, HEADERS_OPEN, params=params)
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        _ADAPT.sleep()
    return items
