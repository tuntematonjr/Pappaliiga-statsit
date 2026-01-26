# Commit Message

```
feat(team-page): Improve Team Overview UX - Phase 1

Implement critical UI/UX improvements to Team Overview page focusing on
trustworthiness, accessibility, and visual hierarchy. This is Phase 1 of 2.

## Changes Made (7 of 11 requirements)

### 1. Fix season consistency in hero header
- Hero pills now reflect currently selected season (not always first option)
- Add currentSeasonOption computed property
- Add fallback display (—) with tooltips for missing data
- Pills update immediately when season changes

### 2. Fix sorting defaults and visibility
- Change playerDefaultSort to use 'kd' (visible) instead of 'rating' (not visible)
- Change mapDefaultSort to use 'totalRoundsPlayed' (visible) instead of 'played'
- Add aria-sort attributes to SortableTable for accessibility
- Improve sort header tooltips: "Sort by <column>, click to toggle"

### 3. Fix grouped header alignment in map table
- Replace hard-coded MAP_COLUMN_GROUPS with computed groups
- Replace hard-coded SCOUT_MAP_GROUPS with computed groups
- Groups now derive colSpans from column configuration (single source of truth)
- Prevents alignment drift when columns change

### 4. Improve Kauden yleiskuva visual hierarchy
- Split seasonSnapshotStats into two tiers:
  * Tier 1 (primary): Win %, Map win %, Round diff, Matches
  * Tier 2 (secondary): Active players, Maps (W–L), Forfeited maps, Upcoming
- Add tier-specific styling classes for differentiation
- Reduce trend arrow visual dominance (add trend-indicator--subtle)
- Improve all metric tooltips

### 5. Add URL persistence for active tab
- Active tab initializes from route query param (?tab=overview|matches|players|veto)
- selectTab() persists tab in URL query
- Enables shareable links to specific tabs
- Default remains 'overview' if missing/invalid

### 6. Remove emoji empty states
- Replace all emoji-based empty states with neutral UI
- Locations: veto-historia, map stats, matches, players, veto tab, trend chart
- Consistent copy: title + one-line description (no emojis)

### 7. Add regression tests
- Create tests/team_detail_regressions.test.js
- Test coverage:
  * Season pills show correct season (not always first)
  * Default sorts use visible columns
  * Grouped header colSpans match column counts
  * URL tab persistence
  * Empty states have no emojis

## Files Changed
- frontend/static/components/TeamDetail.js (primary component)
- frontend/static/components/SortableTable.js (aria-sort, tooltips)
- tests/team_detail_regressions.test.js (new)
- TEAM_PAGE_IMPROVEMENTS_SUMMARY.md (documentation)
- IMPLEMENTATION_SUMMARY.md (quick reference)

## Breaking Changes
None. All changes are backwards compatible.

## Phase 2 (Remaining Work)
- Improve Veto-historia heatmap (legend, saturation, sort control)
- Improve map performance table (mode rename, heatmap control, scroll affordance)
- Complete comprehensive tooltips (heatmap cells, veto cells)
- Verify backend API support (veto data, division averages)

## Testing
Run regression tests:
```bash
npm test tests/team_detail_regressions.test.js
```

Manual testing:
- Navigate to team page, change season → hero pills update immediately
- Players table default sort is K/D (visible column)
- Detailed map table default sort is "Erät pelattu" (visible column)
- Grouped headers align perfectly
- "Kauden yleiskuva" shows two distinct tiers
- No emojis in empty states
- Tab URL persistence works (?tab=players)

Closes: #[issue-number] (if applicable)
```

---

# Alternative Short Commit Message

```
feat(team-page): Fix critical UX issues - season pills, sorting, alignment

- Fix hero pills to show selected season (not always first)
- Fix default sorts to use visible columns (kd, totalRoundsPlayed)
- Fix grouped header alignment with dynamic colSpan computation
- Split overview metrics into two visual tiers
- Add URL persistence for active tab
- Remove emoji empty states
- Add regression tests

Phase 1 of 2. See TEAM_PAGE_IMPROVEMENTS_SUMMARY.md for details.
```
