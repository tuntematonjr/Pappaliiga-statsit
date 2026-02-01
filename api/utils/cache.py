from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Hashable, Optional, Tuple, TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float


class AsyncTTLCache:
    """Simple in-memory TTL cache for async services with LRU eviction and single-flight."""

    def __init__(self, ttl_seconds: float = 30.0, maxsize: int = 128) -> None:
        self.ttl = ttl_seconds
        self.maxsize = maxsize
        self._entries: "OrderedDict[Hashable, _CacheEntry]" = OrderedDict()
        self._lock = asyncio.Lock()
        self._inflight: Dict[Hashable, asyncio.Future] = {}
        # Statistics
        self._hits = 0
        self._misses = 0
        self._sets = 0

    async def get(self, key: Hashable) -> Optional[Any]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry:
                self._misses += 1
                return None
            if entry.expires_at < time.monotonic():
                self._entries.pop(key, None)
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return entry.value

    async def set(self, key: Hashable, value: Any, *, ttl_seconds: Optional[float] = None) -> None:
        async with self._lock:
            self._sets += 1
            now = time.monotonic()
            expired_keys = [k for k, v in self._entries.items() if v.expires_at < now]
            for expired_key in expired_keys:
                self._entries.pop(expired_key, None)

            if key in self._entries:
                self._entries.pop(key, None)

            self._entries[key] = _CacheEntry(
                value=value,
                expires_at=now + (ttl_seconds if ttl_seconds is not None else self.ttl),
            )
            self._entries.move_to_end(key)

            while len(self._entries) > self.maxsize:
                self._entries.popitem(last=False)

    async def get_or_set(
        self,
        key: Hashable,
        producer: Callable[[], Awaitable[T]],
        *,
        ttl_seconds: Optional[float] = None,
    ) -> Tuple[T, bool]:
        """Return cache value if present, otherwise compute via producer.

        Returns:
            Tuple[value, is_cached]
        """
        cached = await self.get(key)
        if cached is not None:
            return cached, True

        async with self._lock:
            cached = self._entries.get(key)
            if cached is not None and cached.expires_at >= time.monotonic():
                self._entries.move_to_end(key)
                return cached.value, True

            inflight = self._inflight.get(key)
            if inflight is None:
                loop = asyncio.get_running_loop()
                inflight = loop.create_future()
                self._inflight[key] = inflight
                is_producer = True
            else:
                is_producer = False

        if not is_producer:
            value = await inflight
            return value, True

        try:
            value = await producer()
            await self.set(key, value, ttl_seconds=ttl_seconds)
            inflight.set_result(value)
        except Exception as exc:  # pragma: no cover - surface errors to all waiters
            inflight.set_exception(exc)
            raise
        finally:
            async with self._lock:
                self._inflight.pop(key, None)

        return value, False

    async def invalidate(self, key: Hashable) -> None:
        async with self._lock:
            self._entries.pop(key, None)

    async def invalidate_matching(self, predicate: Callable[[Hashable], bool]) -> int:
        async with self._lock:
            keys = [key for key in self._entries if predicate(key)]
            for key in keys:
                self._entries.pop(key, None)
            return len(keys)

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()

    def get_stats(self) -> dict:
        """Return cache statistics."""
        total_requests = self._hits + self._misses
        hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "sets": self._sets,
            "hit_rate": round(hit_rate, 2),
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._hits = 0
        self._misses = 0
        self._sets = 0
