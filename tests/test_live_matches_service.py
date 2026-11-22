import unittest
from unittest.mock import AsyncMock, patch

from api.services import live_matches_service


class LiveMatchesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await live_matches_service._LIVE_MATCHES_CACHE.clear()

    async def test_returns_normalized_ongoing_match_and_uses_cache(self):
        championship = {
            "championship_id": "championship-1",
            "name": "Pappaliiga S13 Divisioona 1",
        }
        match = {
            "match_id": "match-1",
            "status": "ONGOING",
            "started_at": 1_700_000_000_000,
            "faceit_url": "https://www.faceit.com/en/{lang}/cs2/room/match-1",
            "teams": {
                "faction1": {
                    "faction_id": "team-1",
                    "name": "Team One",
                    "avatar": "https://example.test/team-1.png",
                },
                "faction2": {
                    "faction_id": "team-2",
                    "name": "Team Two",
                    "avatar": "https://example.test/team-2.png",
                },
            },
            "results": {"score": {"faction1": 8, "faction2": 5}},
        }
        with (
            patch.object(
                live_matches_service,
                "list_championships_for_organizer_async",
                new=AsyncMock(return_value=[championship]),
            ) as list_championships,
            patch.object(
                live_matches_service,
                "get_championship_matches_async",
                new=AsyncMock(return_value=[match]),
            ) as get_matches,
        ):
            first = await live_matches_service.get_live_organization_matches()
            second = await live_matches_service.get_live_organization_matches()

        self.assertEqual(first, second)
        self.assertEqual(
            first,
            [
                {
                    "match_id": "match-1",
                    "championship_id": "championship-1",
                    "championship_name": "Pappaliiga S13 Divisioona 1",
                    "status": "ONGOING",
                    "scheduled_at": None,
                    "started_at": 1_700_000_000_000,
                    "faceit_url": "https://www.faceit.com/cs2/room/match-1",
                    "team1": {
                        "id": "team-1",
                        "name": "Team One",
                        "avatar": "https://example.test/team-1.png",
                    },
                    "team2": {
                        "id": "team-2",
                        "name": "Team Two",
                        "avatar": "https://example.test/team-2.png",
                    },
                    "team1_score": 8,
                    "team2_score": 5,
                }
            ],
        )
        list_championships.assert_awaited_once()
        get_matches.assert_awaited_once_with("championship-1", match_type="ongoing")
