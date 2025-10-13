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
