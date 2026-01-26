feat(team-page): Complete Team Overview UX improvements (11/11)

Comprehensive implementation of UI/UX improvements enhancing trustworthiness,
accessibility, and user experience across Team Overview pages.

## Phase 1 (Requirements 1-7) ✅

### Trust & Accuracy
- **Season pills display selected season** (not arbitrary first)
  Added currentSeasonOption computed property
  
- **Table sorts use visible columns** (no hidden defaults)
  Player table: K/D | Map tables: Total Rounds Played / Played
  
- **Grouped headers dynamically computed** (prevents drift)
  Created computeMapColumnGroups() and computeScoutMapColumnGroups()

### Visual Hierarchy
- **Kauden yleiskuva two-tier layout**
  Tier 1 (prominent): Winrate, Map Balance, Played
  Tier 2 (subtle): Round Diff, Overtime %, First Half Win %

### User Experience
- **URL tab persistence** for shareable links
  Tabs sync with ?tab= query parameter
  
- **Professional empty states** (removed emoji)
  Neutral title + description across 5 sections

### Testing
- **Comprehensive regression test suite**
  300+ lines covering all critical fixes

## Phase 2 (Requirements 8-11) ✅

### Veto-Historia Improvements
- **Sortable veto heatmap** (usage vs alphabetical)
  Added vetoMapOrder data property
  Toggle controls: "Pelimäärä" / "A–Ö"
  Updated vetoTrendMapPool computed sort logic

### Map Table Enhancements  
- **Comprehensive tooltips on heatmap cells**
  Added getHeatTooltip() with min/max/avg context
  Applied to winrate, pickWinRate, oppPickWinRate, rd cells

### Tooltip System
- **Helper methods for contextual tooltips**
  rdHeatStyle() - Round diff gradient heatmap
  isHeatmapColumn() - Identifies heat columns
  getHeatTooltip() - Value + statistical context
  getVetoCellTooltip() - Veto action details

### Backend Verification
- **Confirmed API sufficiency**
  All endpoints provide necessary data
  No backend changes required

## Files Changed

### Components
- frontend/static/components/TeamDetail.js (14+ edits, 2835 lines)
- frontend/static/components/SortableTable.js (accessibility)

### Styles (NEW)
- frontend/static/team-page-improvements.css (450+ lines)
  * Tier 1/2 snapshot styling
  * Veto order controls
  * Map table enhancements
  * Tooltip cursor affordances
  * Responsive adjustments

### Tests (NEW)  
- tests/team_detail_regressions.test.js (300+ lines)
  * Season pill correctness
  * Sort visibility validation
  * Grouped header alignment
  * URL persistence
  * Emoji-free empty states

### Integration
- frontend/index.html (CSS import)

### Documentation
- TEAM_PAGE_IMPROVEMENTS_SUMMARY.md
- IMPLEMENTATION_SUMMARY.md
- PHASE_2_COMPLETION_SUMMARY.md
- IMPLEMENTATION_COMPLETE.md
- COMMIT_MESSAGE_FINAL.md (this file)

## Technical Details

**Approach**: End-to-end implementation with frontend-only changes
**Testing**: Automated regression suite + manual QA checklist
**Performance**: Computed properties cache, efficient tooltip generation
**Accessibility**: ARIA attributes, keyboard navigation, help cursors
**Browser Support**: Chrome 120+, Firefox 121+, Safari 17+

## Impact

**Backend**: None (all changes frontend-only)
**API**: No changes required (existing endpoints sufficient)
**Database**: No schema changes
**Breaking Changes**: None

## Rollback

Safe to revert without backend coordination.
All changes isolated to frontend components and styles.

## Before/After

**Before**: 
- Season pills showed first option (not selected)
- Tables sorted by hidden columns (confusing)
- Grouped headers hard-coded (maintenance burden)
- Emoji empty states (unprofessional)
- No URL tab state (unshare able)
- No veto sorting control
- Missing heatmap tooltips

**After**:
- Season pills reflect user selection
- All sorts use visible columns (trustworthy)
- Headers dynamically computed (maintainable)
- Professional empty states
- Shareable tab URLs
- User-controlled veto sorting
- Rich contextual tooltips

## Testing

✅ Automated regression tests passing
⏳ Manual QA recommended before deploy (see IMPLEMENTATION_COMPLETE.md)

---

Closes: #team-page-ux-improvements
See: IMPLEMENTATION_COMPLETE.md for full technical report
