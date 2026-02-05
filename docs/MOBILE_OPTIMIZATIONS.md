# Mobile Optimizations

## Overview
Comprehensive mobile and touch device optimizations for Pappaliiga Stats, ensuring excellent user experience across all device sizes from 320px phones to tablets.

## Implementation Summary

### 1. **Responsive CSS (styles.css)**

#### Breakpoints
- **≤480px**: Extra small phones (iPhone SE)
- **≤640px**: Small phones/mobile
- **≤768px**: Tablets and larger phones
- **641-1024px**: Tablets (specific rules)
- **≤896px landscape**: Landscape phone optimization
- **Touch devices**: `@media (hover: none) and (pointer: coarse)`

#### Key Features
- **Smart column hiding**: Tables automatically hide less important columns on smaller screens
- **Touch-friendly targets**: Minimum 44×44px tap areas for all interactive elements
- **Scroll indicators**: Visual hints for horizontally scrollable tables
- **Single-column cards**: Sankari and stat cards stack on mobile
- **Optimized spacing**: Reduced padding and font sizes for compact displays
- **Active states**: Touch feedback instead of hover effects on mobile
- **Performance**: GPU-accelerated animations, smooth scrolling

### 2. **Mobile Utilities (mobile-utils.js)**

JavaScript utility module providing:
- Device detection (mobile, touch capability)
- Horizontal scroll indicators with shadow effects
- Dynamic column visibility based on screen width
- Lazy image loading with Intersection Observer
- Touch feedback enhancement
- Auto-initialization and mutation observer for dynamic content
- Responsive grid column calculation

#### Usage in Components
```javascript
// Get filtered columns for current viewport
const visibleColumns = window.MobileUtils.getVisibleColumns(allColumns);

// Check if mobile
if (window.MobileUtils.isMobile()) {
  // Mobile-specific logic
}

// Setup scroll indicators (auto-called on init)
window.MobileUtils.setupScrollIndicators(container);
```

### 3. **Enhanced HTML Meta Tags**

Added to `index.html`:
```html
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
<meta name="theme-color" content="#0b1020">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Pappaliiga Stats">
```

### 4. **Component Updates**

#### SortableTable Component
- Integrated mobile column filtering using `MobileUtils.getVisibleColumns()`
- Responsive column visibility with resize listener
- Wrapped tables in `.table-wrapper` for scroll indicators
- Added `data-label` attributes for future card-view mode
- Lazy loading for map logos (`loading="lazy"`)

## Responsive Behavior by Screen Size

### Extra Small Phones (≤480px)
- **Tables**: Show only name + 2 key metrics (K/D, ADR, Kills)
- **Font sizes**: 0.75rem-0.8rem
- **Cards**: Single column
- **Buttons**: Compact padding
- **Hero logos**: 80px

### Small Phones (≤640px)
- **Tables**: Show name + top 3-4 metrics
- **Font sizes**: 0.8rem-0.85rem
- **Sankari cards**: Single column (forced)
- **Stat boxes**: Single column
- **Navigation**: Horizontal scroll with thin scrollbar

### Tablets (≤768px)
- **Tables**: Hide utility, weapons, damage, clutch columns
- **Font sizes**: 0.85rem-0.9rem
- **Touch targets**: Minimum 44px height
- **Stat boxes**: 2 columns
- **Scroll indicators**: Gradient shadows on table edges

### Tablets (641-1024px)
- **Tables**: Show name + 5-6 important metrics
- **Sankari cards**: 2 columns
- **Stat boxes**: 3 columns
- **Better balance**: between mobile and desktop views

### Landscape Phones (≤896px landscape)
- **Optimized vertical space**: Reduced hero padding
- **Stat boxes**: 3 columns to use horizontal space
- **Compact sections**: Smaller section padding

## Touch Device Enhancements

### Active States (replaces hover)
- Tables, buttons, tabs get visual feedback on tap
- `.touch-active` class for programmatic touch feedback
- Scale transforms (0.97-0.98) for pressed state
- Color overlays (rgba accent colors)

### Touch Optimization
- `-webkit-overflow-scrolling: touch` for smooth inertial scrolling
- `-webkit-tap-highlight-color` for native tap feedback
- `-webkit-touch-callout: none` to prevent long-press menus on UI elements
- Passive event listeners for better scroll performance

### Scroll Performance
- `will-change: scroll-position` on table wrappers
- Hardware acceleration for animations
- Throttled scroll event handlers using `requestAnimationFrame`
- ResizeObserver for efficient responsive updates

## Table Column Priority System

Columns are hidden in this order (least to most important):

1. **First to hide (≤768px)**:
   - Weapons (pistol, sniper kills)
   - Damage stats (total damage)
   - Clutch stats
   - Flash stats (except flash success)
   - Utility damage (except UDPR)

2. **Hidden on small mobile (≤640px)**:
   - Only keep: Name, K/D, ADR, Kills

3. **Hidden on extra small (≤480px)**:
   - Only keep: Name, K/D, ADR

### Always Visible Columns
- Player/team names (`.col-name`)
- Map names (`.col-map-name`)

## Testing

### Browser DevTools Testing
```javascript
// Test mobile detection
window.MobileUtils.isMobile()       // true on ≤768px
window.MobileUtils.isTouchDevice()  // true if touch capable

// Test column filtering
const cols = PLAYER_COLUMNS;
window.MobileUtils.getVisibleColumns(cols)  // Returns filtered array

// Test scroll indicators
window.MobileUtils.setupScrollIndicators()  // Updates all tables
```

### Device Testing Checklist
- [ ] iPhone SE (375px) - minimal view
- [ ] iPhone 12 Pro (390px) - small mobile
- [ ] iPhone 12 Pro Max (428px) - large mobile
- [ ] iPad Mini (768px) - tablet portrait
- [ ] iPad Pro (1024px) - tablet landscape
- [ ] Test both portrait and landscape
- [ ] Test touch scrolling on tables
- [ ] Verify tap targets are easy to hit
- [ ] Check text readability (min 14px actual size)

### Performance Testing
- [ ] Lighthouse mobile score (target: 90+)
- [ ] Check for horizontal overflow (`document.body.scrollWidth`)
- [ ] Test scroll smoothness (60fps target)
- [ ] Verify lazy loading works
- [ ] Check table scroll indicators appear/disappear correctly

## Future Enhancements

### Planned Improvements
1. **Card view mode toggle**: Allow users to switch between table and card layouts
2. **Column picker**: Let users customize visible columns
3. **Pull-to-refresh**: Native-like refresh gesture
4. **Bottom navigation**: Sticky nav for easier thumb access
5. **PWA features**: Add manifest.json, service worker, offline support
6. **Dark mode toggle**: User preference for light/dark themes
7. **Gesture support**: Swipe between tabs

### Accessibility Improvements
- [ ] Improve keyboard navigation on mobile
- [ ] Add skip-to-content links
- [ ] Better screen reader announcements
- [ ] Focus management in modals
- [ ] High contrast mode support

## Troubleshooting

### Tables Not Hiding Columns
- Check if `window.MobileUtils` is loaded
- Verify component uses `visibleColumns` computed property
- Check browser console for JavaScript errors

### Scroll Indicators Not Appearing
- Ensure `.table-wrapper` class is present
- Check if table actually overflows (`scrollWidth > clientWidth`)
- Verify MobileUtils.init() was called

### Touch Feedback Not Working
- Confirm device reports as touch capable
- Check if event listeners are passive
- Verify `.touch-active` CSS class exists

### Performance Issues
- Enable Chrome DevTools Performance monitor
- Check for layout thrashing (excessive reflows)
- Verify images use lazy loading
- Reduce number of DOM nodes if possible

## Related Files
- `/frontend/static/styles.css` - All responsive CSS
- `/frontend/static/utils/mobile-utils.js` - Mobile JavaScript utilities
- `/frontend/static/components/SortableTable.js` - Mobile-aware table component
- `/frontend/index.html` - Meta tags and viewport config
- `/docs/MOBILE_OPTIMIZATIONS.md` - This file

## References
- [Touch Target Sizes](https://www.nngroup.com/articles/touch-target-size/) - Nielsen Norman Group
- [Mobile Web Best Practices](https://developers.google.com/web/fundamentals/design-and-ux/principles) - Google
- [iOS HIG - Adaptivity and Layout](https://developer.apple.com/design/human-interface-guidelines/layout)
- [Material Design - Responsive Layout Grid](https://material.io/design/layout/responsive-layout-grid.html)
