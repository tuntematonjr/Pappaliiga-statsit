# Vue Components Usage Guide

This guide explains how to use the new reusable Vue components in the Pappaliiga Stats frontend.

## Components Overview

### 1. ProgressBar Component
A reusable progress bar with animations and customizable styling.

#### Props
- `value` (Number, required): Current progress value
- `max` (Number, default: 100): Maximum value
- `color` (String, default: 'default'): Color variant - 'default', 'ok', 'warn', 'err', 'accent'
- `showShimmer` (Boolean, default: true): Enable shimmer animation
- `height` (String, default: '14px'): Bar height
- `label` (String, default: ''): Optional label text
- `showPercentage` (Boolean, default: false): Show percentage text

#### Example Usage
```vue
<!-- Basic usage -->
<progress-bar :value="75" :max="100"></progress-bar>

<!-- With color and label -->
<progress-bar 
    :value="stats.wins" 
    :max="stats.total_games"
    color="ok"
    label="Win Rate"
    :showPercentage="true"
></progress-bar>

<!-- Custom height without shimmer -->
<progress-bar 
    :value="50" 
    height="20px"
    :showShimmer="false"
></progress-bar>
```

---

### 2. SplitBar Component
Win/Loss split bar visualization with gradient styling.

#### Props
- `wins` (Number, required): Number of wins
- `losses` (Number, required): Number of losses
- `height` (String, default: '32px'): Bar height
- `showLabels` (Boolean, default: true): Show W/L labels
- `showShimmer` (Boolean, default: true): Enable shimmer animation

#### Example Usage
```vue
<!-- Basic usage -->
<split-bar :wins="15" :losses="10"></split-bar>

<!-- Custom height without labels -->
<split-bar 
    :wins="teamStats.wins" 
    :losses="teamStats.losses"
    height="40px"
    :showLabels="false"
></split-bar>
```

---

### 3. SortableTable Component
Advanced table with sorting, colorization, and customization options.

#### Props
- `columns` (Array, required): Column definitions
- `data` (Array, required): Table data
- `defaultSort` (Object, default: null): Initial sort state
- `colorizeColumns` (Array, default: []): Columns to apply gradient colorization
- `stickyHeader` (Boolean, default: false): Enable sticky header
- `compact` (Boolean, default: false): Compact table styling

#### Column Definition Format
```javascript
{
    key: 'player_name',        // Data key
    label: 'Player',           // Display label
    sortable: true,            // Enable sorting (default: true)
    numeric: false,            // Is numeric column (affects sort)
    align: 'left',             // Text alignment: 'left', 'right', 'center'
    decimals: 2,               // Decimal places for numbers
    format: (value) => value   // Custom formatter function
}
```

#### Example Usage
```vue
<sortable-table
    :columns="playerColumns"
    :data="players"
    :defaultSort="{ column: 'rating', order: 'desc' }"
    :colorizeColumns="['rating', 'kd', 'adr']"
    stickyHeader
>
    <!-- Custom cell rendering with slot -->
    <template #cell-player_name="{ row }">
        <router-link :to="`/player/${row.player_id}`">
            {{ row.player_name }}
        </router-link>
    </template>
</sortable-table>
```

#### Column Definition Example
```javascript
data() {
    return {
        playerColumns: [
            { key: 'rank', label: '#', sortable: false, align: 'center' },
            { key: 'player_name', label: 'Player', sortable: true, numeric: false },
            { key: 'rating', label: 'Rating', sortable: true, numeric: true, decimals: 2 },
            { key: 'kd', label: 'K/D', sortable: true, numeric: true, decimals: 2 },
            { key: 'adr', label: 'ADR', sortable: true, numeric: true, decimals: 1 },
            { key: 'hs_percent', label: 'HS%', sortable: true, numeric: true, 
              format: (v) => `${v.toFixed(1)}%` }
        ],
        players: [
            { player_id: 1, player_name: 'Player1', rating: 1.25, kd: 1.4, adr: 85.5, hs_percent: 52.3 },
            // ... more players
        ]
    };
}
```

---

## Composables

### useTableSort
Provides table sorting functionality.

```javascript
const { sortData, currentSort, toggleSort } = window.useTableSort(data, defaultSort);

// sortData() - Returns sorted data array
// currentSort - Reactive object: { column: 'name', order: 'asc' }
// toggleSort(column, isNumeric) - Toggle sort for column
```

### useProgressBars
Creates progress bar configurations.

```javascript
const { createProgressBar, createSplitWRBar } = window.useProgressBars();

// createProgressBar(value, max, color, shimmer)
// createSplitWRBar(wins, losses, shimmer)
```

### useCellColorization
Applies gradient colorization to table cells based on quartiles.

```javascript
const { colorizeColumn, getCellStyle } = window.useCellColorization();

// colorizeColumn(values) - Calculate quartiles
// getCellStyle(value, quartileData, inverse) - Get cell style
```

---

## Integration Examples

### Example 1: Enhanced Home View with Progress Bars

```vue
<div class="stat-card">
    <h3>Season Progress</h3>
    <progress-bar 
        :value="stats.matches_played" 
        :max="stats.total_matches"
        color="accent"
        :showPercentage="true"
    ></progress-bar>
</div>
```

### Example 2: Team Stats with Split Bars

```vue
<div class="team-overview">
    <h3>{{ team.name }}</h3>
    <split-bar 
        :wins="team.wins" 
        :losses="team.losses"
    ></split-bar>
    <p>Win Rate: {{ ((team.wins / (team.wins + team.losses)) * 100).toFixed(1) }}%</p>
</div>
```

### Example 3: Player Leaderboard with Sortable Table

```vue
<sortable-table
    :columns="[
        { key: 'rank', label: '#', sortable: false, align: 'center' },
        { key: 'nickname', label: 'Player', sortable: true },
        { key: 'rating', label: 'Rating', numeric: true, decimals: 2 },
        { key: 'kills', label: 'K', numeric: true },
        { key: 'deaths', label: 'D', numeric: true },
        { key: 'kd', label: 'K/D', numeric: true, decimals: 2 },
        { key: 'adr', label: 'ADR', numeric: true, decimals: 1 }
    ]"
    :data="topPlayers"
    :defaultSort="{ column: 'rating', order: 'desc' }"
    :colorizeColumns="['rating', 'kd', 'adr']"
>
    <template #cell-nickname="{ row }">
        <router-link :to="`/player/${row.player_id}`" class="player-link">
            {{ row.nickname }}
        </router-link>
    </template>
</sortable-table>
```

---

## Component Registration

Components are automatically available globally after loading:

```html
<!-- Load order in index.html -->
<!-- 1. Composables (dependencies) -->
<script src="/static/composables/useTableSort.js"></script>
<script src="/static/composables/useProgressBars.js"></script>
<script src="/static/composables/useCellColorization.js"></script>

<!-- 2. Components -->
<script src="/static/components/ProgressBar.js"></script>
<script src="/static/components/SplitBar.js"></script>
<script src="/static/components/SortableTable.js"></script>
```

To use in a Vue component:
```javascript
window.MyView = {
    name: 'MyView',
    components: {
        ProgressBar: window.ProgressBar,
        SplitBar: window.SplitBar,
        SortableTable: window.SortableTable
    },
    // ... rest of component
};
```

---

## Styling

All components use the unified CSS from `styles.css`:
- Progress bars: `.progress-bar`, `.progress-fill`, `.progress-glow`
- Split bars: `.bar-split`, `.win`, `.loss`, `.bar-shimmer`
- Tables: `.table-sortable`, `.sortable`, `.cell-excellent`, `.cell-grad`

Custom styling can be applied via:
1. CSS classes (recommended)
2. Inline styles via props
3. Scoped styles in component

---

## Best Practices

1. **Performance**: Use `colorizeColumns` sparingly on large tables
2. **Accessibility**: Always provide meaningful labels for progress bars
3. **Responsive**: Test tables on mobile - consider compact mode
4. **Sorting**: Set appropriate `numeric` flag for correct sort order
5. **Custom Cells**: Use slots for complex cell content (links, images, etc.)

---

## Troubleshooting

### Table not sorting correctly
- Check `numeric: true` for number columns
- Ensure data values are numbers, not strings

### Progress bar not animating
- Verify `showShimmer` is true
- Check that value is between 0 and max (exclusive)

### Colorization not working
- Ensure column key in `colorizeColumns` matches data key
- Check that column has numeric values

### Component not found
- Verify script load order in index.html
- Check browser console for errors
- Ensure composables load before components
