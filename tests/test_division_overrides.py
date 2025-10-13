import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from division_overrides import (
    combined_status_teams,
    load_division_overrides,
    banned_teams_for_division,
    quit_teams_for_division,
)


def test_load_missing_file_returns_empty(tmp_path):
    missing = tmp_path / "missing.json"
    assert load_division_overrides(missing) == {}


def test_banned_team_entries_are_normalised(tmp_path):
    data = {
        "champ-123": {
            "banned_teams": [
                {
                    "team_id": "TEAM-1",
                    "team_name": "Team One",
                    "reason": "Cheating",
                    "banned_at": "2024-03-15",
                    "note": "Decision by admins",
                },
                {
                    "team_id": "",  # ignored
                    "team_name": "Unnamed",
                },
            ]
        },
        "champ-ignored": "not-a-mapping",
    }
    path = tmp_path / "overrides.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    overrides = load_division_overrides(path)
    banned = banned_teams_for_division("champ-123", overrides)

    assert banned == [
        {
            "team_id": "TEAM-1",
            "team_name": "Team One",
            "reason": "Cheating",
            "banned_at": "2024-03-15",
            "note": "Decision by admins",
            "avatar": "",
            "status": "banned",
        }
    ]

    # Returned list is a defensive copy (mutating it does not affect cached overrides)
    banned[0]["team_name"] = "mutated"
    again = banned_teams_for_division("champ-123", overrides)
    assert again[0]["team_name"] == "Team One"


def test_quit_teams_are_loaded(tmp_path):
    data = {
        "champ-xyz": {
            "quit_teams": [
                {
                    "team_id": "TEAM-Q",
                    "team_name": "Quitters",
                    "reason": "Roster collapsed",
                    "quit_at": "2024-05-01",
                }
            ]
        }
    }
    path = tmp_path / "overrides.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    overrides = load_division_overrides(path)
    quitters = quit_teams_for_division("champ-xyz", overrides)
    assert quitters == [
        {
            "team_id": "TEAM-Q",
            "team_name": "Quitters",
            "reason": "Roster collapsed",
            "quit_at": "2024-05-01",
            "note": "",
            "avatar": "",
            "status": "quit",
        }
    ]

    combined = combined_status_teams("champ-xyz", overrides)
    assert combined and combined[0]["status"] == "quit"
