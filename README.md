# ⚠️ **DO NOT EDIT FILES IN `docs/` DIRECTLY**

All source changes to CSS/JS must be made in `web_static/` and copied to `docs/` using `copy_static.bat`. The `docs/` directory is auto-generated and will be overwritten. Manual edits to `docs/` will be lost and may break the site.


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
