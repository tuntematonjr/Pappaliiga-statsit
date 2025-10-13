import asyncio

import pytest

from faceit_client_async import HourlyLimiter


def test_hourly_limiter_snapshot_counts() -> None:
    async def _run() -> None:
        limiter = HourlyLimiter(capacity=10, window_seconds=3600.0)
        for _ in range(3):
            await limiter.acquire()
        snapshot = await limiter.snapshot()

        assert snapshot["request_count_total"] >= 3
        assert snapshot["requests_last_hour"] >= 3
        assert snapshot["requests_last_minute"] >= 3
        assert snapshot["hourly_requests_remaining"] == pytest.approx(10 - snapshot["requests_last_hour"])
        assert snapshot["average_requests_per_minute"] > 0

    asyncio.run(_run())


def test_hourly_limiter_reset_clears_stats() -> None:
    async def _run() -> None:
        limiter = HourlyLimiter(capacity=5, window_seconds=60.0)
        await limiter.acquire()
        await limiter.reset()
        snapshot = await limiter.snapshot()

        assert snapshot["request_count_total"] == 0
        assert snapshot["requests_last_hour"] == 0
        assert snapshot["average_requests_per_minute"] == 0
        assert snapshot["hourly_requests_remaining"] == pytest.approx(5)

    asyncio.run(_run())
