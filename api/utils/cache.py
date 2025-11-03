from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Hashable, Optional, Tuple, TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float


class AsyncTTLCache:
    """Simple in-memory TTL cache for async services."""

    def __init__(self, ttl_seconds: float = 30.0, maxsize: int = 128) -> None:
        self.ttl = ttl_seconds
        self.maxsize = maxsize
        self._entries: Dict[Hashable, _CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: Hashable) -> Optional[Any]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return None
            if entry.expires_at < time.monotonic():
                self._entries.pop(key, None)
                return None
            return entry.value

    async def set(self, key: Hashable, value: Any) -> None:
        async with self._lock:
            if len(self._entries) >= self.maxsize:
                # Drop the oldest entry
                oldest_key, _ = min(self._entries.items(), key=lambda item: item[1].expires_at)
                self._entries.pop(oldest_key, None)
            self._entries[key] = _CacheEntry(value=value, expires_at=time.monotonic() + self.ttl)

    async def get_or_set(
        self,
        key: Hashable,
        producer: Callable[[], Awaitable[T]],
    ) -> Tuple[T, bool]:
        """Return cache value if present, otherwise compute via producer.

        Returns:
            Tuple[value, is_cached]
        """
        cached = await self.get(key)
        if cached is not None:
            return cached, True

        value = await producer()
        await self.set(key, value)
        return value, False
