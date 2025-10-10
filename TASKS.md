# Review Findings and Suggested Tasks

## Status

All previously suggested cleanups have been implemented:

- [x] Organizer constant renamed to `PAPPALIIGA_ORG_ID` and its usages updated for consistent branding. 【F:faceit_config.py†L1-L39】【F:gen_divisions_json.py†L16-L32】
- [x] `compute_team_summary_data` now defends against empty aggregates to avoid crashes for stat-less teams. 【F:db.py†L205-L241】
- [x] `faceit_config.py` comments are consistently in English. 【F:faceit_config.py†L1-L24】
- [x] Added targeted tests covering `weighted_percentile`/`weighted_median` behaviour and edge cases. 【F:tests/test_weighted_percentile.py†L1-L43】
