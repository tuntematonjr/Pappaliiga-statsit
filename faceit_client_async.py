from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Optional

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from faceit_config import (
    API_KEY,
    DEMOCRACY_BASE,
    OPEN_BASE,
)

LOGGER = logging.getLogger(__name__)

MAX_HTTP_CONCURRENCY = 8
HTTP_TIMEOUT = httpx.Timeout(20.0, connect=10.0, read=20.0, write=20.0)
MAX_RETRIES = 5
HOURLY_LIMIT = int(os.environ.get("FACEIT_HOURLY_LIMIT", "0"))
HOURLY_WINDOW_SECONDS = 3600.0

BASE_SLEEP = 0.10
MAX_SLEEP = 1.50
BACKOFF_FACTOR = 1.75
RECOVER_FACTOR = 0.85
RECOVER_STEPS = 3

_open_client: Optional[httpx.AsyncClient] = None
_demo_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()


class RateLimitError(Exception):
    """Raised internally to trigger retries when Faceit throttles us."""


class AdaptiveLimiter:
    def __init__(self, base: float, max_value: float, grow: float, recover: float, recover_steps: int) -> None:
        self.base = max(0.0, base)
        self.max_value = max_value
        self.grow = grow
        self.recover = recover
        self.recover_steps = max(1, recover_steps)
        self.cur = self.base
        self.ok_streak = 0
        self._lock = asyncio.Lock()

    async def _decay_if_needed(self) -> None:
        if self.ok_streak >= self.recover_steps and self.cur > self.base:
            self.cur = max(self.base, self.cur * self.recover)
            self.ok_streak = 0

    async def on_success(self) -> None:
        async with self._lock:
            self.ok_streak += 1
            await self._decay_if_needed()

    async def on_error(self) -> None:
        async with self._lock:
            self.cur = min(self.max_value, max(self.cur, self.base) * self.grow)
            self.ok_streak = 0

    async def on_throttle(self) -> None:
        async with self._lock:
            self.cur = min(self.max_value, max(self.cur, self.base) * self.grow)
            self.ok_streak = 0

    async def sleep(self) -> None:
        async with self._lock:
            delay = self.cur
        if delay > 0:
            await asyncio.sleep(delay)

    async def delay_hint(self) -> float:
        async with self._lock:
            return self.cur


_LIMITER = AdaptiveLimiter(BASE_SLEEP, MAX_SLEEP, BACKOFF_FACTOR, RECOVER_FACTOR, RECOVER_STEPS)


class HourlyLimiter:
    def __init__(self, capacity: int, window_seconds: float) -> None:
        self.capacity = capacity
        self.window = max(0.01, window_seconds)
        self._events: Deque[float] = deque()
        self._lock = asyncio.Lock()
        self._total_requests = 0
        self._started_at: float | None = None

    async def acquire(self) -> float:
        if self.capacity <= 0:
            return 0.0
        waited = 0.0
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                cutoff = now - self.window
                while self._events and self._events[0] <= cutoff:
                    self._events.popleft()
                if len(self._events) < self.capacity:
                    self._events.append(now)
                    if self._started_at is None:
                        self._started_at = now
                    self._total_requests += 1
                    return waited
                sleep_for = (self._events[0] + self.window) - now
            sleep_for = max(sleep_for, 0.01)
            LOGGER.debug(
                "Hourly limiter reached %d/%d requests; sleeping %.2fs",
                len(self._events),
                self.capacity,
                sleep_for,
            )
            await asyncio.sleep(sleep_for)
            waited += sleep_for

    async def reset(self) -> None:
        async with self._lock:
            self._events.clear()
            self._total_requests = 0
            self._started_at = None

    async def snapshot(self) -> Dict[str, float]:
        if self.capacity <= 0:
            return {
                "request_count_total": 0.0,
                "requests_last_minute": 0.0,
                "requests_last_five_minutes": 0.0,
                "requests_last_hour": 0.0,
                "hourly_usage_ratio": 0.0,
                "hourly_requests_remaining": 0.0,
                "average_requests_per_minute": 0.0,
            }
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            cutoff = now - self.window
            while self._events and self._events[0] <= cutoff:
                self._events.popleft()
            events = list(self._events)
            last_minute_cutoff = now - 60.0
            last_five_cutoff = now - 300.0
            requests_last_minute = sum(1 for ts in events if ts >= last_minute_cutoff)
            requests_last_five_minutes = sum(1 for ts in events if ts >= last_five_cutoff)
            requests_last_hour = len(events)
            requests_remaining = max(self.capacity - requests_last_hour, 0)
            started_at = self._started_at
            runtime_seconds = (now - started_at) if started_at is not None else 0.0
            avg_per_minute = (
                (self._total_requests / (runtime_seconds / 60.0))
                if runtime_seconds > 0
                else 0.0
            )
            total_requests = self._total_requests

        hourly_usage_ratio = (
            min(requests_last_hour / self.capacity, 1.0) if self.capacity else 0.0
        )

        return {
            "request_count_total": float(total_requests),
            "requests_last_minute": float(requests_last_minute),
            "requests_last_five_minutes": float(requests_last_five_minutes),
            "requests_last_hour": float(requests_last_hour),
            "hourly_usage_ratio": float(hourly_usage_ratio),
            "hourly_requests_remaining": float(requests_remaining),
            "average_requests_per_minute": float(avg_per_minute),
        }


_HOURLY_LIMITER = HourlyLimiter(HOURLY_LIMIT, HOURLY_WINDOW_SECONDS)

_rate_limit_hits = 0
_rate_limit_wait = 0.0
_hourly_wait_events = 0
_hourly_wait_seconds = 0.0
_rate_lock = asyncio.Lock()


async def reset_rate_limit_stats() -> None:
    global _rate_limit_hits, _rate_limit_wait, _hourly_wait_events, _hourly_wait_seconds
    async with _rate_lock:
        _rate_limit_hits = 0
        _rate_limit_wait = 0.0
        _hourly_wait_events = 0
        _hourly_wait_seconds = 0.0
    await _HOURLY_LIMITER.reset()


async def _record_rate_limit(wait_seconds: float) -> None:
    global _rate_limit_hits, _rate_limit_wait
    async with _rate_lock:
        _rate_limit_hits += 1
        if wait_seconds > 0:
            _rate_limit_wait += wait_seconds


async def _record_hourly_wait(wait_seconds: float) -> None:
    global _hourly_wait_events, _hourly_wait_seconds
    if wait_seconds <= 0:
        return
    async with _rate_lock:
        _hourly_wait_events += 1
        _hourly_wait_seconds += wait_seconds


async def get_rate_limit_stats() -> Dict[str, float]:
    async with _rate_lock:
        base_stats = {
            "throttle_hits": float(_rate_limit_hits),
            "throttle_wait_seconds": float(_rate_limit_wait),
            "hourly_wait_events": float(_hourly_wait_events),
            "hourly_wait_seconds": float(_hourly_wait_seconds),
        }
    hourly_snapshot = await _HOURLY_LIMITER.snapshot()
    base_stats.update(hourly_snapshot)
    return base_stats


async def _get_clients() -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
    global _open_client, _demo_client
    if _open_client and _demo_client:
        return _open_client, _demo_client

    async with _client_lock:
        if _open_client and _demo_client:
            return _open_client, _demo_client

        headers_open = {
            "Accept": "application/json",
            "User-Agent": "pappaliiga-sync/1.0",
        }
        if API_KEY:
            headers_open["Authorization"] = f"Bearer {API_KEY}"

        _open_client = httpx.AsyncClient(
            base_url=OPEN_BASE,
            timeout=HTTP_TIMEOUT,
            limits=httpx.Limits(max_connections=MAX_HTTP_CONCURRENCY, max_keepalive_connections=MAX_HTTP_CONCURRENCY),
            headers=headers_open,
        )
        _demo_client = httpx.AsyncClient(
            base_url=DEMOCRACY_BASE,
            timeout=HTTP_TIMEOUT,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=2),
            headers={
                "Accept": "application/json",
                "User-Agent": "pappaliiga-sync/1.0",
            },
        )
    return _open_client, _demo_client


async def shutdown_clients() -> None:
    global _open_client, _demo_client
    if _open_client:
        await _open_client.aclose()
        _open_client = None
    if _demo_client:
        await _demo_client.aclose()
        _demo_client = None


async def _request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    expected_status: Iterable[int] | None = None,
) -> Optional[Dict[str, Any]]:
    expected = set(expected_status or {200})

    def _log_retry(retry_state: Any) -> None:
        exc = retry_state.outcome.exception()
        if isinstance(exc, RateLimitError):
            return
        wait = getattr(getattr(retry_state, "next_action", None), "sleep", None)
        wait_msg = f"{wait:.2f}s" if isinstance(wait, (int, float)) else "unknown"
        LOGGER.warning(
            "Retrying %s %s (attempt %d/%d) in %s due to %s",
            method,
            url,
            retry_state.attempt_number,
            MAX_RETRIES,
            wait_msg,
            exc,
        )

    async for attempt in AsyncRetrying(
        retry=retry_if_exception_type((RateLimitError, httpx.HTTPError, httpx.TransportError, httpx.TimeoutException)),
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential_jitter(initial=0.75, max=5.0),
        before_sleep=_log_retry,
        reraise=True,
    ):
        with attempt:
            await _LIMITER.sleep()
            hourly_wait = await _HOURLY_LIMITER.acquire()
            if hourly_wait:
                await _record_hourly_wait(hourly_wait)
                if hourly_wait >= 5:
                    LOGGER.warning(
                        "Hourly limiter slept %.2fs before %s %s",
                        hourly_wait,
                        method,
                        url,
                    )
            resp = await client.request(method, url, params=params)
            status = resp.status_code

            if status == 429:
                await _LIMITER.on_throttle()
                retry_after_header = resp.headers.get("Retry-After")
                wait_seconds = 0.0
                if retry_after_header:
                    try:
                        wait_seconds = float(retry_after_header)
                    except ValueError:
                        LOGGER.debug("Non-numeric Retry-After header from Faceit: %s", retry_after_header)
                        retry_after_header = None

                if wait_seconds <= 0:
                    wait_seconds = await _LIMITER.delay_hint()

                LOGGER.warning(
                    "Rate limit hit (attempt %d) for %s %s – waiting %.2fs before retry (Retry-After=%s)",
                    attempt.retry_state.attempt_number,
                    method,
                    url,
                    wait_seconds,
                    retry_after_header or "n/a",
                )

                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)

                await _record_rate_limit(wait_seconds)
                raise RateLimitError("Faceit rate limited request")

            if status == 404:
                await _LIMITER.on_success()
                return None

            if status == 403:
                LOGGER.warning("Faceit returned 403 for %s %s", method, url)
                await _LIMITER.on_error()
                return None

            if status not in expected:
                try:
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    await _LIMITER.on_error()
                    raise exc

            await _LIMITER.on_success()
            if status == 204:
                return None
            if resp.content:
                try:
                    return resp.json()
                except ValueError as exc:
                    await _LIMITER.on_error()
                    raise httpx.HTTPError(f"Failed to decode JSON from {url}") from exc
            return None

    return None


async def get_championship_matches_async(championship_id: str, match_type: str = "all", limit: int = 100) -> List[Dict[str, Any]]:
    open_client, _ = await _get_clients()
    match_types = ["past", "ongoing", "upcoming"] if match_type == "all" else [match_type]
    results: List[Dict[str, Any]] = []
    for mt in match_types:
        offset = 0
        while True:
            payload = await _request_json(
                open_client,
                "GET",
                f"/championships/{championship_id}/matches",
                params={"type": mt, "offset": offset, "limit": limit},
                expected_status={200},
            )
            if payload is None:
                LOGGER.warning("Failed to fetch matches for championship %s type %s", championship_id, mt)
                break
            items = payload.get("items") or []
            if not items:
                break
            results.extend(items)
            if len(items) < limit:
                break
            offset += limit
            await asyncio.sleep(0.05)
    return results


async def get_match_details_async(match_id: str) -> Optional[Dict[str, Any]]:
    open_client, _ = await _get_clients()
    return await _request_json(open_client, "GET", f"/matches/{match_id}")

async def get_championship_details_async(championship_id: str, *, expanded: List[str] | None = None, silent: bool = False) -> Optional[Dict[str, Any]]:
    """Retrieve championship details from Faceit.

    Args:
        championship_id: Faceit championship id.
        expanded: Optional list of expansion fields (e.g. 'organizer').
        silent: When True, swallow exceptions and return None.

    Returns:
        Parsed JSON dict or None.
    """
    open_client, _ = await _get_clients()
    params = None
    if expanded:
        params = {"expanded": ",".join(expanded)}
    try:
        return await _request_json(open_client, "GET", f"/championships/{championship_id}", params=params)
    except Exception:
        if silent:
            return None
        raise


async def get_match_stats_async(match_id: str) -> Optional[Dict[str, Any]]:
    open_client, _ = await _get_clients()
    return await _request_json(open_client, "GET", f"/matches/{match_id}/stats")


async def get_map_votes_async(match_id: str) -> Optional[Dict[str, Any]]:
    _, demo_client = await _get_clients()
    return await _request_json(demo_client, "GET", f"/match/{match_id}/history")


async def get_championship_teams_async(championship_id: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
    """
    List registered teams for a championship.
    Args:
        championship_id: Faceit championship identifier.
        limit: Page size (Faceit allows up to 100).
    Returns:
        List of team dictionaries (may be empty).
    """
    open_client, _ = await _get_clients()
    results: List[Dict[str, Any]] = []
    offset = 0
    failed = False
    while True:
        payload = await _request_json(
            open_client,
            "GET",
            f"/championships/{championship_id}/teams",
            params={"offset": offset, "limit": limit},
            expected_status={200},
        )
        if payload is None:
            LOGGER.warning("Failed to fetch teams for championship %s", championship_id)
            failed = True
            break
        items = payload.get("items") or []
        if not items:
            break
        results.extend(items)
        if len(items) < limit:
            break
        offset += limit
        await asyncio.sleep(0.05)
    if failed:
        return None
    return results


async def list_championships_for_organizer_async(organizer_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    List all championships for a given organizer (async).
    Args:
        organizer_id: The Faceit organizer ID.
        limit: Max items per request (Faceit API default 20, max 100).
    Returns:
        List of championship dicts.
    """
    open_client, _ = await _get_clients()
    results: List[Dict[str, Any]] = []
    offset = 0
    while True:
        payload = await _request_json(
            open_client,
            "GET",
            f"/organizers/{organizer_id}/championships",
            params={"offset": offset, "limit": limit},
            expected_status={200},
        )
        if payload is None:
            LOGGER.warning("Failed to fetch championships for organizer %s", organizer_id)
            break
        items = payload.get("items") or []
        if not items:
            break
        results.extend(items)
        if len(items) < limit:
            break
        offset += limit
        await asyncio.sleep(0.05)
    return results
