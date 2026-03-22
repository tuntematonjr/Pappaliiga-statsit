# Mobile Optimizations

Quick reference for current mobile behavior.

## Where It Lives
- `frontend/static/styles.css`
- `frontend/static/utils/mobile-utils.js`
- `frontend/static/components/SortableTable.js`
- `frontend/index.html`

## Active Behavior
- Mobile breakpoints are handled in CSS and in `MobileUtils.getVisibleColumns()`.
- Table containers use `.table-wrapper` with horizontal scroll indicators.
- Touch devices get `.touch-active` feedback styles.
- `SortableTable` uses mobile-aware column filtering and lazy image loading.
- `index.html` includes mobile meta tags (`viewport`, `theme-color`, Apple web app tags).

## Breakpoint Summary
- `<= 480px`: minimal columns (name/map + core metrics)
- `481-640px`: essential columns only
- `641-1024px`: important columns
- `>= 1025px`: full table columns

## Quick Verification
Use browser console:

```javascript
window.MobileUtils.isMobile()
window.MobileUtils.isTouchDevice()
window.MobileUtils.getVisibleColumns(PLAYER_COLUMNS)
window.MobileUtils.setupScrollIndicators()
```

Manual checks:
- Resize to `375px`, `640px`, `768px`, `1024px`.
- Confirm tables reduce columns and stay horizontally scrollable.
- Confirm scroll shadows/hints appear on overflowing tables.
- Confirm touch feedback on table rows/buttons.

## Common Fixes
- Columns not changing: ensure `mobile-utils.js` is loaded before views.
- No scroll indicator: ensure table is wrapped in `.table-wrapper`.
- Touch state missing: ensure `.touch-active` rules exist in `styles.css`.
