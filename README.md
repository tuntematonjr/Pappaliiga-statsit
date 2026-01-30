


![GitHub Logo](https://i.gyazo.com/4338082eb9f98e0ba7d480dc311471d6.jpg) 

# Pappaliiga stats generator

Async HTML generation is the only mode now. CI/GitHub Actions don’t need changes.

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

## Division overrides

- To flag teams that were banned from a division, create an optional `division_overrides.json`
  file alongside the scripts. Example structure:

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
      ]
    }
  }
  ```

- When present, the overrides are surfaced in the generated HTML: banned teams are annotated
  with a red “(BANNED)” suffix and their matches are excluded from division-wide aggregates for
  other teams while still showing the banned team’s own data.
