# Team Overview Page UI/UX Improvements - Implementation Summary

## Completed Changes (January 19, 2026)

### ✅ 1. Season Consistency in Hero Header
**Status:** Complete

**Changes Made:**
- Added `currentSeasonOption` computed property that reflects the actually selected season (not always seasonOptions[0])
- Hero pills now use `currentSeasonOption?.season` and `currentSeasonOption?.division`
- Added fallback display with em-dash (`—`) when data not available
- Added tooltips for fallback states: "No season data" / "No division data"

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (lines ~650-665, template lines ~2020-2028)

**Code:**
```javascript
currentSeasonOption() {
    const current = this.currentChampionshipId;
    if (!current) return this.seasonOptions[0] || null;
    return this.seasonOptions.find(s => String(s.value) === String(current)) || this.seasonOptions[0] || null;
},
```

---

### ✅ 2. Sorting Defaults and Visibility
**Status:** Complete

**Changes Made:**
- Fixed `playerDefaultSort` to use `'kd'` (visible column) instead of `'rating'` (not visible)
- Fixed `mapDefaultSort` (detailed mode) to use `'totalRoundsPlayed'` (visible) instead of `'played'` (not in MAP_COLUMNS)
- `scoutMapDefaultSort` (compact mode) already correctly uses `'played'` (which IS in SCOUT_MAP_COLUMNS)
- Added `aria-sort` attributes to SortableTable headers for accessibility
- Improved sort tooltips: "Sort by <Column>, click to toggle"

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (playerDefaultSort ~line 1568, mapDefaultSort ~line 1495)
- `frontend/static/components/SortableTable.js` (template ~lines 270-290)

**Code:**
```javascript
playerDefaultSort() {
    return { column: 'kd', order: 'desc', numeric: true };
},
mapDefaultSort() {
    return { column: 'totalRoundsPlayed', order: 'desc', numeric: true };
},
```

---

### ✅ 3. Grouped Header Alignment in Map Table
**Status:** Complete

**Changes Made:**
- Replaced hard-coded `MAP_COLUMN_GROUPS` with dynamically computed groups via `computeMapColumnGroups(MAP_COLUMNS)`
- Replaced hard-coded `SCOUT_MAP_GROUPS` with dynamically computed groups via `computeScoutMapColumnGroups(SCOUT_MAP_COLUMNS)`
- Groups now derive colSpans directly from column configuration (single source of truth)
- Prevents misalignment when columns change

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (lines ~57-95, ~117-145)

**Code:**
```javascript
function computeMapColumnGroups(columns) {
    const groupMap = {
        'mapName': '',
        'totalRoundsPlayed': 'Erät',
        'adr': 'Taistelu', 'kr': 'Taistelu', 'kd': 'Taistelu', 'hsPct': 'Taistelu',
        // ... etc
    };
    // Dynamically compute colSpans by iterating columns
}
const MAP_COLUMN_GROUPS = computeMapColumnGroups(MAP_COLUMNS);
```

---

### ✅ 4. Kauden Yleiskuva Visual Hierarchy
**Status:** Complete

**Changes Made:**
- Split `seasonSnapshotStats` into two tiers:
  - **Tier 1 (Primary)**: Win %, Map win %, Round diff, Matches
  - **Tier 2 (Secondary)**: Active players, Maps (W–L), Forfeited maps, Upcoming matches
- Added separate computed properties: `seasonSnapshotTier1()` and `seasonSnapshotTier2()`
- Updated template to render two separate rows with distinct styling classes:
  - `scout-snapshot-row--tier1` / `scout-snapshot-item--primary` (larger, more prominent)
  - `scout-snapshot-row--tier2` / `scout-snapshot-item--secondary` (smaller, secondary ink)
- Trend arrows now have `trend-indicator--subtle` class for reduced visual dominance
- Improved tooltip text (e.g., "Compared to division average for this season: <value>")

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (seasonSnapshotStats ~lines 939-1110, template ~lines 2145-2175)

**Template:**
```html
<div class="scout-snapshot-row scout-snapshot-row--tier1">
    <div v-for="stat in seasonSnapshotTier1" ...>...</div>
</div>
<div class="scout-snapshot-row scout-snapshot-row--tier2">
    <div v-for="stat in seasonSnapshotTier2" ...>...</div>
</div>
```

---

### ✅ 5. URL Persistence for Active Tab
**Status:** Complete

**Changes Made:**
- `activeTab` data property now initializes from `this.$route?.query?.tab || 'overview'`
- `selectTab(tab)` method persists active tab in URL query via `this.$router.replace({ query })`
- Default remains `overview` if missing/invalid

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (data() ~line 650, selectTab() ~line 1845)

**Code:**
```javascript
data() {
    return {
        activeTab: this.$route?.query?.tab || 'overview',
        // ...
    };
},
selectTab(tab) {
    this.activeTab = tab;
    if (this.$router && this.$route) {
        const query = { ...(this.$route.query || {}) };
        query.tab = tab;
        this.$router.replace({ query }).catch(() => {});
    }
},
```

---

### ✅ 6. Remove Emoji Empty States
**Status:** Complete

**Changes Made:**
- Replaced all emoji-based empty states (`🎮`, `👤`, `🗳️`, `🗺️`, `📈`) with neutral UI
- Removed `<div class="empty-state-icon">emoji</div>` elements
- Kept simple title + one-line description (no emojis)
- Consistent copy across sections (English for clarity)

**Locations Fixed:**
- Veto-historia empty state
- Map stats empty state (both compact and detailed modes)
- Matches empty state
- Players empty state
- Veto tab empty state
- Trend chart empty state

**Files Modified:**
- `frontend/static/components/TeamDetail.js` (multiple template sections)

**Example:**
```html
<!-- Before -->
<div class="empty-state-icon">🗳️</div>
<h3 class="empty-state-title">Ei veto-historiaa</h3>

<!-- After -->
<h3 class="empty-state-title">No veto history</h3>
<p class="empty-state-description">Veto history data is not available for this season.</p>
```

---

## Remaining Work (To Be Implemented)

### 🔶 7. Improve Veto-Historia Heatmap
**Priority:** High  
**Complexity:** Medium

**Requirements:**
- Move legend to compact position (right side of header or collapsible)
- Reduce color saturation slightly while preserving distinct states
- Default map row order prioritizes scan-ability (by usage or most played)
- Add simple control to switch ordering: `Usage` vs `A–Z`
- Improve cell tooltips (see Tooltips section)

**Implementation Notes:**
- Veto heatmap is rendered in template starting ~line 2180
- Legend entries are in `vetoLegendEntries` computed property
- Current cells use classes like `veto-heatmap__cell--team-pick`, `--opp-pick`, `--team-ban-1st`, etc.
- Add sort control similar to mapViewMode toggle
- Consider CSS adjustments to reduce saturation in `styles.css`

---

### 🔶 8. Map Performance Table Improvements
**Priority:** High  
**Complexity:** Medium-High

**Requirements:**
1. **Rename modes:** `Compact` → `Summary`, `Detailed` → `Full` (or Finnish equivalents)
2. **Heatmap control:** Strong heatmap ONLY for `Win %`, `Round diff`, `ADR`, `K/D` (and `Pick%`, `Ban%` if in map table). All other columns neutral styling.
3. **Column grouping:** Ensure grouped headers have subtle background shading per group (low opacity)
4. **Sticky columns:** Keep sticky header and sticky first column (map name) in both modes if feasible
5. **Horizontal scroll affordance:**
   - Add subtle left/right fade shadows on table wrapper when content overflows
   - Add `jump` controls in table toolbar to scroll to key groups: `Results`, `Performance`, `Utility`, `Pick/Ban`
   - Jump controls adjust `scrollLeft` of wrapper
6. **Missing data:** Differentiate `—` from `0` with tooltip "No data for selected season"

**Implementation Notes:**
- Current mode toggle is `mapViewMode: 'compact'|'detailed'`
- Heat styles: `winHeatStyle`, `kdHeatStyle`, `adrHeatStyle` methods (~line 1855-1875)
- Need to add conditional heatmap application in template cells
- Add toolbar with jump buttons above table
- Sticky columns require CSS adjustments

**Code Hints:**
```javascript
// Rename mode labels in template
const modeLabels = { summary: 'Summary', full: 'Full' };

// Add jump method
jumpToGroup(groupLabel) {
    const wrapper = this.$refs.mapTableWrapper;
    // Find first column in group, calculate scrollLeft
}

// Add fade shadow logic
checkScrollOverflow() {
    const wrapper = this.$refs.mapTableWrapper;
    this.showLeftShadow = wrapper.scrollLeft > 0;
    this.showRightShadow = wrapper.scrollLeft < (wrapper.scrollWidth - wrapper.clientWidth);
}
```

---

### 🔶 9. Comprehensive Tooltips
**Priority:** High  
**Complexity:** Medium

**Requirements:**

#### A) Kauden Yleiskuva Metric Tooltips (DONE in Tier 1/2 refactor, but verify completeness)
- ✅ Win %: "Wins / Matches played · includes forfeits"
- ✅ Map win %: "Map wins / Maps played · includes forfeits"
- ✅ Round diff: "Rounds won - rounds lost · forfeits count as 13-0"
- ✅ Matches: "Total matches played this season"
- ✅ Active players: "Players who played at least one map this season"
- ✅ Maps (W–L): "Map-level record for the season"
- ✅ Forfeited maps: "Maps forfeited (lost without playing) · affects total stats"
- ✅ Upcoming matches: "Matches scheduled but not yet played"
- ✅ Trend arrows: "Compared to division average for this season: <value>"

#### B) Heatmap/Gradient Tooltips in Map Table
**Status:** Partial (column tooltips exist, need to enhance cell tooltips)**

- On any heat-colored cell, tooltip must include:
  - Metric name
  - Raw value
  - Scaling explanation: "Scaled across this team's maps for the selected season" (or document if global)
- For Win % cells showing W–L: tooltip must show W–L explicitly

**Implementation:**
```javascript
getHeatTooltip(column, row) {
    if (!this.isHeatmapColumn(column.key)) return column.tooltip || '';
    const value = row[column.key];
    const scaling = 'Scaled across this team\'s maps for the selected season';
    if (column.key === 'winrate' && row.wins != null && row.losses != null) {
        return `${column.label}: ${value} (${row.wins}–${row.losses}) · ${scaling}`;
    }
    return `${column.label}: ${value} · ${scaling}`;
}
```

#### C) Sort Tooltips (DONE)
- ✅ Sortable headers: "Sort by <column>, click to toggle"

#### D) Veto-Historia Cell Tooltips
**Status:** NOT DONE

- For each cell: match identifier (date/opponent if available), map, and action
- For column header (match): show opponent and match result (if available)

**Implementation:**
```javascript
getVetoCellTooltip(vetoEntry, match) {
    const opponent = match?.opponentName || 'Unknown';
    const result = match ? getMatchResult(match) : null;
    const action = vetoEntry.action; // 'pick', 'ban', 'decider', 'overflow'
    const actor = vetoEntry.actor === 'team' ? 'Team' : 'Opponent';
    return `${opponent} · ${vetoEntry.mapName} · ${actor} ${action}${result ? ' · Result: ' + result : ''}`;
}
```

---

### 🔶 10. Backend/API Support
**Priority:** Medium  
**Complexity:** Low-Medium

**Requirements:**
- Ensure season/division/playoffs information is available per selected season for hero pills
- Ensure veto history provides enough data for tooltips (opponent name, match date/id, result)
- Add ordering by usage for veto history if not already available
- Update computed stats logic to match tooltip text (avoid lying tooltips)

**Current State:**
- Team page API endpoint: `/api/teams/{teamId}/page?championship_id={championshipId}`
- Response includes `seasons`, `seasonData`, `vetoHistory`, `mapStats`, etc.
- Verify `vetoHistory` includes `match_id`, `opponent_name`, `match_result`, `match_date`
- Verify division averages are returned in `seasonData` for trend indicators

**Recommended API Checks:**
```python
# In api/services/teams_service.py or relevant service
# Ensure fetch_team_page returns:
{
    "seasons": [...],
    "seasonData": {
        "championshipId": "...",
        "vetoHistory": [
            {
                "match_id": "...",
                "opponent_name": "...",
                "match_date": "...",
                "match_result": "win|loss|draw",
                "map_name": "...",
                "status": "pick|ban|...",
                "selected_by_team_id": "...",
                "order": 1
            }
        ],
        "divisionAverages": {
            "avgWinRate": 50.5,
            "avgMapWinRate": 48.2,
            "avgRoundDiff": -5.3
        }
    }
}
```

---

### 🔶 11. Tests for Critical Regressions
**Priority:** Medium  
**Complexity:** Medium

**Requirements:**
1. **Season pills correctness test:**
   - Mock seasonOptions with multiple seasons
   - Set currentChampionshipId to non-first season
   - Assert hero pills display correct season/division
2. **Default sorting visible test:**
   - Assert players table sorts by `kd` on load (visible column)
   - Assert detailed map table sorts by `totalRoundsPlayed` on load (visible column)
3. **Grouped header alignment test (snapshot or unit):**
   - Assert MAP_COLUMN_GROUPS colSpans sum equals MAP_COLUMNS.length
   - Assert SCOUT_MAP_GROUPS colSpans sum equals SCOUT_MAP_COLUMNS.length

**Implementation:**
- Create test file: `frontend/tests/TeamDetail.spec.js` (if test framework exists)
- Use Vue Test Utils or similar
- Example:
```javascript
import { mount } from '@vue/test-utils';
import TeamDetail from '@/components/TeamDetail';

describe('TeamDetail', () => {
    it('shows correct season in hero pills', () => {
        const wrapper = mount(TeamDetail, {
            data: () => ({
                seasonOptions: [
                    { value: '1', season: 5, division: 1 },
                    { value: '2', season: 4, division: 2 }
                ],
                selectedChampionship: '2'
            })
        });
        expect(wrapper.find('.team-hero__season .pill').text()).toContain('Kausi 4');
    });

    it('sorts players by kd by default', () => {
        // ...
    });

    it('grouped headers align with columns', () => {
        const colsCount = MAP_COLUMNS.length;
        const groupsTotal = MAP_COLUMN_GROUPS.reduce((sum, g) => sum + g.colSpan, 0);
        expect(groupsTotal).toBe(colsCount);
    });
});
```

---

## Additional Notes

### CSS Changes Needed
To fully support the above changes, the following CSS updates are recommended in `frontend/static/styles.css`:

1. **Tier 1/Tier 2 snapshot styles:**
```css
.scout-snapshot-row--tier1 {
    margin-bottom: 1.5rem;
}

.scout-snapshot-item--primary .snapshot-value--large {
    font-size: 2rem;
    font-weight: 700;
}

.scout-snapshot-item--secondary .snapshot-label--secondary {
    font-size: 0.9rem;
    opacity: 0.8;
}

.scout-snapshot-item--secondary .snapshot-value {
    font-size: 1.2rem;
}

.trend-indicator--subtle {
    opacity: 0.6;
    font-size: 0.85em;
}
```

2. **Map table scroll shadows:**
```css
.table-wrapper {
    position: relative;
}

.table-wrapper::before,
.table-wrapper::after {
    content: '';
    position: absolute;
    top: 0;
    bottom: 0;
    width: 30px;
    pointer-events: none;
    transition: opacity 0.2s;
    z-index: 5;
}

.table-wrapper.scroll-shadow-left::before {
    left: 0;
    background: linear-gradient(to right, rgba(0, 0, 0, 0.15), transparent);
}

.table-wrapper.scroll-shadow-right::after {
    right: 0;
    background: linear-gradient(to left, rgba(0, 0, 0, 0.15), transparent);
}
```

3. **Group background shading:**
```css
.table-group-header th {
    background: rgba(var(--accent-rgb), 0.05);
}

.table-group-header th.group-divider {
    border-left: 1px solid rgba(0, 0, 0, 0.1);
}
```

4. **Sticky columns:**
```css
.table-sortable.sticky-header th {
    position: sticky;
    top: 0;
    z-index: 10;
}

.table-sortable th.col-map-name {
    position: sticky;
    left: 0;
    z-index: 11;
    background: var(--bg-color);
}
```

---

## Testing Checklist

Before considering this work complete, test the following scenarios:

- [ ] Navigate to team page, change season dropdown → hero pills update immediately
- [ ] Refresh team page with `?championship=<id>` in URL → correct season shown
- [ ] Team page with missing season data → pills show `—` with tooltip
- [ ] Players table loads → sorted by K/D descending by default
- [ ] Players table sort indicator shows arrow on K/D column
- [ ] Detailed map table loads → sorted by "Erät pelattu" descending by default
- [ ] Grouped headers align perfectly (no offset or drift)
- [ ] "Kauden yleiskuva" shows two distinct tiers (Tier 1 larger, Tier 2 smaller)
- [ ] Trend arrows appear subtle, tooltip shows division average
- [ ] All empty states have no emojis, only title + description
- [ ] Change tab in URL → `?tab=players` works, tab persists on reload
- [ ] Tooltips appear on hover for all metrics, sortable headers, and heatmap cells

---

## File Manifest

All changes were made to:
- `frontend/static/components/TeamDetail.js` (primary component)
- `frontend/static/components/SortableTable.js` (aria-sort, tooltips)

No backend changes were required for the completed work. Backend changes (if any) would be in:
- `api/routers/teams.py`
- `api/services/teams_service.py`

---

## Summary

This implementation addresses 6 of 11 requirements completely, with 5 remaining to be implemented:

**Completed:**
1. ✅ Season consistency in hero header
2. ✅ Sorting defaults and visibility
3. ✅ Grouped header alignment
4. ✅ Kauden yleiskuva visual hierarchy
5. ✅ URL persistence for tabs
6. ✅ Remove emoji empty states

**Remaining:**
7. 🔶 Improve Veto-historia heatmap (legend, saturation, ordering, tooltips)
8. 🔶 Map performance table (mode rename, heatmap control, scroll affordance, jump controls)
9. 🔶 Comprehensive tooltips (heatmap cells, veto cells - partial completion)
10. 🔶 Backend/API support (verify veto history data, division averages)
11. 🔶 Tests for critical regressions

The completed work significantly improves trustworthiness (season pills, sorting), accessibility (aria-sort), and visual hierarchy (two-tier overview, neutral empty states). The remaining work focuses on advanced interactions (scroll affordance, jump controls, heatmap refinement) and polish (tooltips, tests).

---

**End of Summary**
