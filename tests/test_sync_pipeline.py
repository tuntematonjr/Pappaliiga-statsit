from __future__ import annotations

import pytest

from sync_pipeline import MatchContext, NormalisedMatch, _build_normalised_match, safe_float, safe_int


def _base_details() -> dict:
    return {
        "match_id": "match-123",
        "status": "FINISHED",
        "scheduled_at": 1710000000,
        "configured_at": 1710000500,
        "started_at": 1710000600,
        "finished_at": 1710003600,
        "best_of": 1,
        "teams": {
            "faction1": {
                "faction_id": "team-1",
                "name": "Team One",
                "avatar": "",
                "roster": [
                    {"player_id": "p1", "nickname": "Player1"},
                    {"player_id": "p2", "nickname": "Player2"},
                ],
            },
            "faction2": {
                "faction_id": "team-2",
                "name": "Team Two",
                "avatar": "",
                "roster": [
                    {"player_id": "p3", "nickname": "Player3"},
                    {"player_id": "p4", "nickname": "Player4"},
                ],
            },
        },
        "results": {
            "winner": "faction1",
            "score": {"faction1": 1, "faction2": 0},
        },
    }


def _stats_payload() -> dict:
    return {
        "rounds": [
            {
                "match_round": "1",
                "round_stats": {
                    "Map": "de_mirage",
                    "Score": "13 / 7",
                    "Winner": "team-1",
                },
                "teams": [
                    {
                        "team_id": "team-1",
                        "team_stats": {
                            "Final Score": "13",
                            "First Half Score": "8",
                            "Second Half Score": "5",
                            "Team Headshots": "45.5",
                            "Team Win": "1",
                        },
                        "players": [
                            {
                                "player_id": "p1",
                                "nickname": "Player1",
                                "player_stats": {
                                    "Kills": "20",
                                    "Deaths": "10",
                                    "Assists": "5",
                                    "K/D Ratio": "2.0",
                                    "K/R Ratio": "1.0",
                                    "ADR": "90.5",
                                    "Headshots %": "50",
                                },
                            },
                            {
                                "player_id": "p2",
                                "nickname": "Player2",
                                "player_stats": {
                                    "Kills": "15",
                                    "Deaths": "12",
                                    "Assists": "6",
                                    "K/D Ratio": "1.25",
                                    "K/R Ratio": "0.75",
                                    "ADR": "75.0",
                                    "Headshots %": "35",
                                },
                            },
                        ],
                    },
                    {
                        "team_id": "team-2",
                        "team_stats": {
                            "Final Score": "7",
                            "First Half Score": "4",
                            "Second Half Score": "3",
                            "Team Headshots": "40.0",
                            "Team Win": "0",
                        },
                        "players": [
                            {
                                "player_id": "p3",
                                "nickname": "Player3",
                                "player_stats": {
                                    "Kills": "18",
                                    "Deaths": "17",
                                    "Assists": "2",
                                    "K/D Ratio": "1.06",
                                    "K/R Ratio": "0.9",
                                    "ADR": "80.0",
                                    "Headshots %": "40",
                                },
                            },
                        ],
                    },
                ],
            }
        ]
    }


def test_safe_coercion_helpers() -> None:
    assert safe_int("42") == 42
    assert safe_int("bad", 5) == 5
    assert pytest.approx(safe_float("13,37"), 0.01) == 13.37
    assert safe_float("", 0.0) == 0.0


def test_build_normalised_match_regular() -> None:
    ctx = MatchContext(
        championship_id="champ-1",
        season=11,
        division_num=1,
        slug="div1-s11",
        is_playoffs=False,
        banned_team_ids=set(),
        banned_lookup={},
    )
    details = _base_details()
    stats = _stats_payload()
    result = _build_normalised_match(ctx, details, stats, votes_json={})

    assert result.match_row["is_forfeit"] == 0
    assert result.match_row["winner_team_id"] == "team-1"
    assert len(result.map_rows) == 1
    assert result.map_rows[0]["map_name"] == "de_mirage"
    assert result.map_rows[0]["is_forfeit"] == 0
    assert len(result.player_stats) == 3  # two players + one opponent
    assert any(stat["player_id"] == "p1" for stat in result.player_stats)
    assert any(team_row["team_id"] == "team-1" for team_row in result.team_stats)


def test_build_normalised_match_forfeit_marks_flags() -> None:
    ctx = MatchContext(
        championship_id="champ-2",
        season=11,
        division_num=2,
        slug="div2-s11",
        is_playoffs=False,
        banned_team_ids={"team-forfeit"},
        banned_lookup={"team-forfeit": {"team_id": "team-forfeit", "team_name": "Banned"}},
    )
    details = {
        **_base_details(),
        "match_id": "forfeit-1",
        "teams": {
            "faction1": {"faction_id": "team-forfeit", "name": "Banned", "avatar": ""},
            "faction2": {"faction_id": "team-clean", "name": "Clean", "avatar": ""},
        },
        "results": {"winner": "faction2", "score": {"faction1": 0, "faction2": 1}},
    }
    normalised = _build_normalised_match(ctx, details, stats=None, votes_json=None)

    assert normalised.match_row["is_forfeit"] == 1
    assert normalised.match_row["ignored_due_ban"] == 1
    assert all(row["is_forfeit"] == 1 for row in normalised.map_rows)
```}