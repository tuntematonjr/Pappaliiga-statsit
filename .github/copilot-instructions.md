# ⚠️ **DO NOT EDIT FILES IN `docs/` DIRECTLY**

All source changes to CSS/JS must be made in `web_static/` and copied to `docs/` using `copy_static.bat`. The `docs/` directory is auto-generated and will be overwritten. Manual edits to `docs/` will be lost and may break the site.

# Pappaliiga Stats Generator - AI Agent Instructions

## Project Overview
CS2 tournament statistics generator for Pappaliiga (Finnish esports league). Fetches data from Faceit API, stores in MariaDB, generates static HTML pages for GitHub Pages hosting.

## Architecture & Data Flow
- **sync.py**: Fetches championship/match data from Faceit API → MariaDB (fully async)
- **html_gen.py**: Reads MariaDB → generates division HTML pages (fully async)
- **Output**: Static HTML files in `docs/` for GitHub Pages deployment
- **Database**: MariaDB schema defined in `mariadb_schema.sql`

## Key Components

### Configuration System
- `faceit_config.py`: API keys (env: `FACEIT_API_KEY`), season constants
- `divisions.json`: Championship metadata (IDs, slugs, division numbers)
- `division_overrides.json`: **Season-specific** team exclusions (banned/quit teams), managed per championship
- Schema: `championships` table is the core entity, matches/teams/players join to it

### Async Patterns (Critical)
- **async_db.py**: Connection pooling, all DB operations have async equivalents
- **sync.py**: Fully async data pipeline with concurrent API fetching
- **html_gen.py**: Async file I/O with aiofiles, concurrent division processing
- Use async patterns for all new code and when refactoring existing components

### Database Schema Patterns
- Championship-centric: `championship_id` is primary foreign key
- Slugs for stable URLs: `div1-s11`, `div1-s11-po` (playoffs)
- Per-map stats: `map_team_stats`, `map_player_stats` tables
- Map voting: `map_votes` tracks veto/pick sequences
- **No global bans**: `is_banned` removed from `teams` table; all exclusions are JSON-based and season-specific

## Development Workflows

### Data Pipeline Commands
```bash
# Full refresh (daily in CI)
python sync.py && python html_gen.py --force

# Quick local updates
python html_gen.py                    # skips unchanged files
python html_gen.py --div 1           # single division
```

### Debugging Tools
- `debug_raw.py --match MATCH_ID`: Raw Faceit API responses
- `serve_docs.bat`: Local HTTP server for testing generated HTML
- `log_server.py`: Local HTTP server for collecting client-side debug logs
- `copy_static.bat`: Utility to copy `web_static/` files to `docs/`
- `division_overrides.py`: Helper for managing banned/quit teams via CLI (see TEAM_STATUS_GUIDE.md)

### Windows Batch Helpers
- `run_all.bat`: Complete sync + generate cycle
- `serve_docs.bat`: Local development server (hardcoded IP binding)
- `copy_static.bat`: Copy static assets from `web_static/` to `docs/`

### Local Development & Testing
- **LAN Testing**: Bind servers to LAN IP (192.168.0.13) for multi-device testing
- **Client Logging**: `sendClientLog()` function posts debug events to `log_server.py`
- **Log Events**: 'toggle-start', 'opening', 'closing', 'forced-collapse', 'class-removed'
- **Static Asset Workflow**: Edit in `web_static/` → run `copy_static.bat` → test in `docs/`

## Code of Conduct for Development

### Code Quality Standards
- **Type Hints**: Always use type hints for function parameters and return values
- **Docstrings**: Document all public functions with clear descriptions, parameters, and return values
- **Naming Conventions**: 
  - Functions: `snake_case`, async functions should end with `_async` suffix
  - Classes: `PascalCase`
  - Constants: `UPPER_CASE`
  - Private functions: prefix with `_`
- **Line Length**: Maximum 120 characters per line
- **Error Messages**: Use clear, actionable error messages with context

### Code Organization
- **Single Responsibility**: Each function should do one thing well
- **DRY Principle**: Don't Repeat Yourself - extract common logic into helper functions
- **File Structure**: Group related functions together, keep files focused on specific domains
- **Import Order**: Standard library → third-party → local imports, alphabetically sorted within each group

### Async Best Practices
- **Connection Management**: Always use connection pooling, never create individual connections
- **Error Handling**: Wrap async operations in try/except, log errors with full context
- **Concurrent Operations**: Use `asyncio.gather()` for parallel operations, handle exceptions properly
- **Resource Cleanup**: Use context managers (`async with`) for database connections and file operations

### Database Interactions
- **Parameterized Queries**: Always use parameterized queries (`:param` syntax) to prevent SQL injection
- **Transaction Safety**: Use transactions for multi-step operations that must be atomic
- **Upsert Pattern**: Prefer `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent operations
- **Index Awareness**: Consider query performance and index usage when writing SQL

### Testing & Validation
- **Before Commit**: Test locally with full sync + generation cycle
- **Edge Cases**: Test with missing data, empty results, and error conditions
- **Performance**: Profile slow operations, optimize before committing
- **Schema Changes**: Test migrations thoroughly, document breaking changes

### Git Workflow
- **Commit Messages**: Use clear, descriptive messages explaining "why" not just "what"
- **Branch Naming**: Use feature branches (`feature/description` or `fix/issue`)
- **Pull Requests**: Include context, testing steps, and screenshots for UI changes
- **Code Review**: Review your own diff before requesting review

## Code Conventions

### Error Handling Patterns
- Faceit API: Graceful degradation, log warnings but continue processing
- Database: Foreign key constraints enforced, upsert patterns everywhere
- Async operations: Use connection pools, handle asyncmy properly with try/except blocks

### HTML Generation
- Template versioning: `HTML_TEMPLATE_VERSION` constant for cache busting
- Content diffing: Compares generated vs existing files to skip unchanged
- CSS/JS externalized: `styles.css`, `app.js` shared across all pages

## File Organization
- `docs/`: **AUTO-GENERATED** HTML output (GitHub Pages root) - **DO NOT EDIT MANUALLY**
- `web_static/`: Source CSS/JS files - edit these, then copy to `docs/` when modified
- All content in `docs/` is regenerated by `html_gen.py` and static assets are copied by `copy_static.bat`

## External Dependencies
- **Faceit Open Data API**: Match details, player stats
- **Faceit Democracy API**: Map veto history
- **GitHub Actions**: Daily scheduled sync (secrets: `FACEIT_API_KEY`)

## Common Patterns
- Function naming: `compute_*_async()` for async aggregations
- Database queries: Use `query_async()` wrapper with proper connection handling
- HTML escaping: Always escape user data with `html.escape()`
- Timezone handling: UTC storage, Finnish display (`Europe/Helsinki`)

## UI/UX Patterns

### Visual Design System
- **Dark theme**: CSS custom properties in `:root` define color palette (`--bg`, `--card`, `--accent`, etc.)
- **Card-based layout**: `.stat-card`, `.hero-card`, `.card` classes with consistent styling
- **Index page**: Dual hero cards (AFI + Pappaliiga), stats overview grid, season navigation
- **Division pages**: Team navigation bar with logos, collapsible team/match sections

### Interactive Elements
- **Collapsible sections**: Use `<details>` with custom JavaScript animations
  - Team sections: `.team-section` with `custom-expanded` class for state management
  - Match details: `.match-row` within `.matches-mirror` containers
  - All sections start collapsed, expand with smooth height/opacity transitions
  - **Critical**: Remove resize listener (`window.addEventListener('resize', adaptDetails)`) to prevent mobile collapse issues
- **Table sorting**: `sortTable()` function with visual indicators and persistent state
- **Responsive filters**: Show/hide played-only matches with checkbox controls
- **Progress bars**: Smooth shimmer animation with opacity transitions (avoid class toggling that resets animation)

### JavaScript Patterns
- **Custom animations**: Override default `<details>` behavior with `custom-expanded` class
- **Mobile/PC compatibility**: Event handling works across devices with touch/click
  - Use precise touch detection (max movement: 8px, max time: 400ms)
  - Suppress synthetic click events after touch with `_isTouch` flag
  - Single event handler per summary with `dataset.hasSummaryListener` guard
- **Team navigation**: Auto-expand target sections when clicking team links (# team-{id}
- **State preservation**: Collapsible state managed through CSS classes, not `open` attribute
- **Debug logging**: `sendClientLog()` posts events to local collector (LAN IP configurable)

## Team Status Management
- **Banned/Quit Teams**: Managed exclusively via `division_overrides.json` (season-specific)
- **No Database Field**: `is_banned` removed from `teams` table; runtime flag only for display/exclusion
- **Season Isolation**: Bans/quits only affect their specific championship, not other seasons
- **CLI Tool**: Use `python division_overrides.py` to add/remove/list team exclusions
- **Statistics**: All index/division stats automatically exclude banned/quit teams per season
- **Documentation**: See `TEAM_STATUS_GUIDE.md` for detailed workflow

## Development Principles
- **Async-First**: All new code should use async patterns; migrate sync code when touching it
- **Generated Content**: Never manually edit files in `docs/` - they're auto-regenerated
- **Source of Truth**: CSS/JS changes go in `web_static/`, then copied to `docs/`
- **Season-Specific Exclusions**: Always use `_get_excluded_team_ids_for_championships()` for team filtering
- **UI Consistency**: Follow existing card/section patterns and collapsible behavior
- **Mobile-First UX**: Test expand/collapse on mobile devices; avoid resize listeners that force collapse
- **Smooth Animations**: Use CSS opacity transitions for shimmer effects; avoid class toggling that resets animations

## Performance Considerations
- Async batch processing for division generation
- MariaDB connection pooling (asyncmy)
- Content comparison to avoid unnecessary file writes
- GitHub Pages deployment: Static files only, no server-side processing

## Common Issues & Solutions

### Mobile Expand/Collapse Problems
- **Symptom**: Team sections collapse immediately after expanding on mobile
- **Cause**: `window.addEventListener('resize', adaptDetails)` fires frequently on mobile due to viewport changes
- **Solution**: Remove resize listener; only run `adaptDetails()` on `DOMContentLoaded`

### Progress Bar Glow Reset
- **Symptom**: Shimmer animation resets/flashes when progress reaches 0% or 100%
- **Cause**: Removing and re-adding CSS classes restarts pseudo-element animations
- **Solution**: Use opacity transitions instead of class toggling; keep shimmer continuous

### Touch vs Click Events
- **Symptom**: Double toggles or unresponsive touch on mobile
- **Cause**: Both touch and synthetic click events firing
- **Solution**: Use `touchstart`/`touchend` with movement/time thresholds; suppress click with `_isTouch` flag

### Index Page Team Counting
- **Issue**: Banned/quit teams were incorrectly included in index statistics
- **Root Cause**: SQL exclusion clauses used column alias `tid` instead of actual column names in WHERE clause
- **Solution**: Apply exclusion clauses separately to `team1_id` and `team2_id` in UNION queries
- **Key Function**: `_index_card_stats_async()` and `_calculate_comprehensive_stats_async()` in `html_gen.py`
- **Best Practice**: Always call `_get_excluded_team_ids_for_championships(championship_ids)` before team/player counting queries
