# faceit_client.py
# Faceit client with adaptive rate limiting (no legacy RATE_LIMIT_SLEEP/REQUEST_TIMEOUT/MAX_RETRIES).
# All comments in English.

from __future__ import annotations
import logging
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
    "User-Agent": "pappaliiga-stats/1.0",
}
if API_KEY:
    HEADERS_OPEN["Authorization"] = f"Bearer {API_KEY}"
    
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

def _get(url: str, headers: dict, params: dict | None = None, *, retries: int = ADAPT_MAX_RETRIES, backoff: float = 0.8):
    """
    GET with adaptive sleep + Retry-After support.
    - Uses DEFAULT_TIMEOUT for all calls
    - Honors 429 Retry-After
    - Falls back once without Authorization on 403 (as before)
    - Advances _ADAPT on success/error
    """
    last_err = None
    tried_unauth = False

    for attempt in range(max(1, retries)):
        # adaptive pre-sleep
        _ADAPT.sleep()
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
            sc = resp.status_code

            # Handle 429 with Retry-After
            if sc == 429:
                ra = _retry_after_seconds(resp)
                if ra is not None and ra > 0:
                    time.sleep(ra)
                else:
                    _ADAPT.on_throttle()
                continue

            # CI-friendly warning for 403/404 (unchanged)
            if sc in (403, 404):
                snippet = (resp.text or "")[:200].replace("\n", " ")
                print(f"[warn] GET {url} -> {sc}. Body[:200]={snippet}", flush=True)

                if sc == 403 and "Authorization" in headers and not tried_unauth:
                    noauth = dict(headers)
                    noauth.pop("Authorization", None)
                    tried_unauth = True
                    time.sleep(backoff * (2 ** attempt))
                    resp2 = requests.get(url, headers=noauth, params=params, timeout=DEFAULT_TIMEOUT)
                    if resp2.ok:
                        _ADAPT.on_success()
                        return resp2.json()
                    else:
                        print(f"[warn] Fallback unauth GET {url} -> {resp2.status_code}", flush=True)

                return None

            resp.raise_for_status()
            _ADAPT.on_success()
            return resp.json()

        except requests.HTTPError as e:
            last_err = e
            print(f"[warn] HTTPError on GET {url}: {e}", flush=True)
            _ADAPT.on_error()
        except requests.RequestException as e:
            last_err = e
            print(f"[warn] RequestException on GET {url}: {e}", flush=True)
            _ADAPT.on_error()

        # simple exponential backoff between attempts
        time.sleep(backoff * (2 ** attempt))

    raise RuntimeError(f"GET failed for {url}: {last_err}")

def list_championship_matches(championship_id: str, match_type: str = "all", limit: int = 100) -> list[dict]:
    """
    Palauttaa listan Faceit-matseja. Jos listaus päätyy 403/404 -> skip ja jatka.
    match_type: "past" | "ongoing" | "upcoming" | "all"
    """
    types = ["past", "ongoing", "upcoming"] if match_type == "all" else [match_type]
    out: list[dict] = []
    base = f"{OPEN_BASE}/championships/{championship_id}/matches"

    for mt in types:
        offset = 0
        while True:
            params = {"type": mt, "offset": offset, "limit": limit}
            data = _get(base, HEADERS_OPEN, params=params)
            if data is None:
                print(f"[skip] championship {championship_id} list {mt} -> None (403/404)", flush=True)
                break

            items = data.get("items") or []
            if not items:
                break

            out.extend(items)
            if len(items) < limit:
                break
            offset += limit

    return out

def get_match_details(match_id: str) -> Dict[str, Any]:
    url = f"{OPEN_BASE}/matches/{match_id}"
    data = _get(url, HEADERS_OPEN)
    return data or None


def get_match_stats(match_id: str) -> Dict[str, Any]:
    url = f"{OPEN_BASE}/matches/{match_id}/stats"
    data = _get(url, HEADERS_OPEN)
    return data or None


# --- Democracy history (veto tickets, including map drops/picks) ---

def get_democracy_history(match_id: str) -> Dict[str, Any]:
    url = f"{DEMOCRACY_BASE}/match/{match_id}/history"
    data = _get(url, HEADERS_DEMOCRACY)
    return data or None

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
