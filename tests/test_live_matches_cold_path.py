import unittest
from unittest.mock import AsyncMock, call, patch

from api.services import live_matches_service


class LiveMatchesColdPathTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await live_matches_service._LIVE_MATCHES_CACHE.clear()

    async def test_only_terminal_championships_are_skipped(self):
        championships = [
            {"id": "finished", "status": "FINISHED"},
            {"id": "cancelled", "status": "cancelled"},
            {"id": "canceled", "state": "Canceled"},
            {"id": "started", "status": "started"},
            {"id": "unknown", "status": "new-status"},
            {"id": "missing"},
        ]
        with (
            patch.object(
                live_matches_service,
                "list_championships_for_organizer_async",
                new=AsyncMock(return_value=championships),
            ),
            patch.object(
                live_matches_service,
                "get_championship_matches_async",
                new=AsyncMock(return_value=[]),
            ) as get_matches,
        ):
            self.assertEqual(await live_matches_service.get_live_organization_matches(), [])
            self.assertEqual(await live_matches_service.get_live_organization_matches(), [])

        self.assertCountEqual(
            get_matches.await_args_list,
            [
                call("started", match_type="ongoing"),
                call("unknown", match_type="ongoing"),
                call("missing", match_type="ongoing"),
            ],
        )
