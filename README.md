# ⚠️ **DO NOT EDIT FILES IN `docs/` DIRECTLY**

All source changes to CSS/JS must be made in `web_static/` and copied to `docs/` using `copy_static.bat`. The `docs/` directory is auto-generated and will be overwritten. Manual edits to `docs/` will be lost and may break the site.


![GitHub Logo](https://i.gyazo.com/4338082eb9f98e0ba7d480dc311471d6.jpg) 

# Pappaliiga stats generator

Async HTML generation is the only mode now. CI/GitHub Actions don’t need changes.

> **Database**: The pipeline uses MariaDB via `asyncmy` for all data storage and retrieval.

## Usage

- Commands:
	- html_gen.py                # async, skips up-to-date files
	- html_gen.py --force        # async, rewrites all files

## Batch helpers

- run_all.bat runs data sync and then generates HTML (async). It currently forces rewrite to ensure fresh pages.
- serve_docs.bat serves docs/ locally.

## Notes

- Outputs are written under docs/ for GitHub Pages.
- The generator compares content to avoid unnecessary writes; pass --force to always write.
- Skips rely on MariaDB timestamps (clamped to "now") so reruns are fast when nothing changed.

## Database Schema Updates

- `matches.activity_ts` persists the most recent Faceit timestamp for a match. It is indexed (`idx_matches_activity`, `idx_matches_season_division_status`) and updated by the sync pipeline to speed up ordering.
- `player_stats` and `team_stats` enforce uniqueness on `(match_id, round_index, player_id/team_id)` to prevent duplicate inserts on reruns.
- Additional covering indexes (`idx_matches_ignored_flag`) were added to match filters that exclude banned matches.

### Migration Notes

Forward migration (run in order):

```sql
ALTER TABLE matches
  ADD COLUMN activity_ts BIGINT(20) NOT NULL DEFAULT 0 AFTER last_seen_at;
UPDATE matches
  SET activity_ts = GREATEST(
    COALESCE(finished_at, 0),
    COALESCE(started_at, 0),
    COALESCE(scheduled_at, 0),
    COALESCE(last_seen_at, 0),
    COALESCE(configured_at, 0)
  );
CREATE INDEX idx_matches_activity ON matches (activity_ts);
CREATE INDEX idx_matches_season_division_status ON matches (season, division_num, status);
CREATE INDEX idx_matches_ignored_flag ON matches (ignored_due_ban);
ALTER TABLE player_stats ADD UNIQUE KEY uq_player_stats_match_round_player (match_id, round_index, player_id);
ALTER TABLE team_stats ADD UNIQUE KEY uq_team_stats_match_round_team (match_id, round_index, team_id);
```

Backward migration (drop new objects):

```sql
ALTER TABLE team_stats DROP KEY uq_team_stats_match_round_team;
ALTER TABLE player_stats DROP KEY uq_player_stats_match_round_player;
DROP INDEX idx_matches_ignored_flag ON matches;
DROP INDEX idx_matches_season_division_status ON matches;
DROP INDEX idx_matches_activity ON matches;
ALTER TABLE matches DROP COLUMN activity_ts;
```

Run `python sync.py --verify` after migrating to repopulate cached totals if needed.

## Division overrides

- To flag teams that were banned or quit mid-season, create/update the optional
  `division_overrides.json` file (or run `python manage_team_status.py`). Example structure:

  ```json
  {
    "<championship_id>": {
      "banned_teams": [
        {
          "team_id": "abcdef",
          "team_name": "Example Squad",
          "reason": "Admin decision",
          "banned_at": "2024-03-15"
        }
      ],
      "quit_teams": [
        {
          "team_id": "zyx987",
          "team_name": "Roster Collapse",
          "reason": "Line-up unable to continue",
          "quit_at": "2025-02-01"
        }
      ]
    }
  }
  ```

- When present, both banned and quit teams are surfaced in the generated HTML. Their matches are
  ignored for division-wide aggregates so remaining teams retain fair standings, while still
  showing the affected team’s own data for auditing purposes.
