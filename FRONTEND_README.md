# Pappaliiga Stats - Vue.js Frontend

Modern single-page application for CS2 tournament statistics with glassmorphism UI, responsive charts, and rich team/player dashboards.

## Features

- **Dynamic Data Loading**: Pinia-backed stores cache heavy API responses
- **Division & Team Dashboards**: Sticky headers, comparison charts, and map breakdowns
- **Player Analytics**: KPI grids, sparkline trends, radar charts, and compare modal
- **Responsive Layout**: Fluid typography and breakpoints down to 480px
- **Vue Router SPA**: Client-side routing without reloads

## Architecture

### Tech Stack

- **Vue 3**: Reactive UI framework
- **Vue Router**: Client-side routing
- **Pinia**: State management (optional, included for future use)
- **Vanilla CSS**: Custom styling with CSS variables

### Project Structure

```
frontend/
├── index.html                   # Main HTML entry point
└── static/
    ├── api-client.js            # API wrapper (supports runtime base override)
    ├── app-main.js              # Vue app initialization
    ├── styles.css               # Global styles & design tokens
    ├── components/
    │   ├── HeroBanner.js        # Glass hero layout
    │   ├── SeasonToggle.js      # Season pills + segmented control
    │   ├── StatPanel.js         # Compact metric grid
    │   ├── TeamComparisonChart.js
    │   ├── SparklineChart.js / RadarChart.js
    │   └── MapsStats.js         # Map stat wrapper
    ├── stores/
    │   ├── useHomeStore.js
    │   ├── useDivisionStore.js
    │   ├── useTeamStore.js
    │   └── usePlayerStore.js
    └── views/
        ├── HomeView.js          # Landing page with stats overview
        ├── SeasonsView.js       # Seasons/divisions list
        ├── DivisionView.js      # Division details
        ├── TeamDetailView.js    # Wrapper for team dashboard
        └── PlayerView.js        # Player analytics dashboard
```

## Components

### Key Components

- **HeroBanner** — full-width hero with responsive overlay, used on home & detail pages.
- **SeasonToggle** — segmented control + pill selector for switching seasons/phase.
- **StatPanel** — compact KPI grid used across home, division, team, and player dashboards.
- **SparklineChart / RadarChart** — lightweight SVG charts (no external libs) for trends & attribute profiles.
- **PlayerCompareModal** — side-by-side player metric comparison fetched on demand.
- **MapsStats** — wrapper around `MapStatsTable` with loading/error states.

## Development

### Local Setup

1. **Start API Server**:
   ```bash
   python -m api.main
   ```

2. **Open Browser**:
   Navigate to `http://localhost:8000`

3. **Development Mode**:
   - Uses CDN-hosted Vue/Vue Router/Pinia (no bundler required)
   - Edit `.js` files under `frontend/static/` and refresh
   - Hot reload on file changes via Uvicorn

### Overriding the API Base

By default the API client points to `window.location.origin + '/api'`. To target another backend (e.g. staging), set `window.__API_BASE__` **before** `api-client.js` loads in `index.html`:

```html
<script>
  window.__API_BASE__ = 'https://stats.example.com/api';
</script>
<script src="/static/api-client.js"></script>
```

All subsequent requests use the overridden base URL.

### Adding New Views

1. Create new view in `frontend/static/views/`
2. Register route in `app-main.js`
3. Add navigation link in `index.html`

Example:
```javascript
// views/NewView.js
window.NewView = {
    name: 'NewView',
    template: `<div>New View Content</div>`,
    // ... component logic
};

// app-main.js
routes: [
    // ... existing routes
    { path: '/new', name: 'new', component: window.NewView }
]
```

## API Integration

All API calls use the global `apiClient` instance. Common helpers:

```javascript
// Get team info
const team = await window.apiClient.getTeamInfo(teamId);

// Get map stats with deltas
const mapStats = await window.apiClient.getTeamMapStats(teamId, championshipId);
```

**Tip:** Team and player detail views rely on Pinia stores (`useTeamStore`, `usePlayerStore`) to memoize profile, season, and map stats per ID.

## Styling

### CSS Variables

All colors defined in `:root`:
```css
--bg: #0a0e27;       /* Background */
--card: #1a1f3a;     /* Card background */
--accent: #4a9eff;   /* Primary accent */
--text: #e0e6ed;     /* Text color */
--success: #4ade80;  /* Positive delta */
--danger: #f87171;   /* Negative delta */
```

### Theme Customization

Edit `frontend/static/styles.css` to adjust colors, spacing, or glass effects. Design tokens live under the `:root` block for quick theming.

## Production Build

For production, consider:

1. **Bundle for Performance**:
   ```bash
   # Use Vite or Webpack to bundle
   npm install
   npm run build
   ```

2. **Self-host Vue**:
   - Download Vue.js, Vue Router, Pinia
   - Replace CDN links with local files
   - Improves load time and offline capability

3. **Minify Assets**:
   - Minify CSS and JS
   - Optimize images
   - Enable gzip compression in Nginx

## Deployment

### With Nginx

```nginx
server {
    listen 80;
    server_name stats.pappaliiga.fi;

    # API proxy
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }

    # Frontend
    location / {
        root /opt/pappaliiga-statsit/frontend;
        try_files $uri $uri/ /index.html;
    }

    # Static assets with caching
    location /static/ {
        root /opt/pappaliiga-statsit/frontend;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }
}
```

### SPA Routing

The `try_files $uri $uri/ /index.html` directive ensures:
- Direct URL access works (e.g., `/team/123`)
- Browser refresh doesn't 404
- All routes handled by Vue Router

## Features in Detail

### Map Stats with Deltas

The killer feature - shows performance changes over time:

- **Current**: Latest aggregated stats
- **Previous**: Snapshot from last computation
- **Delta**: Difference (curr - prev)

Example use cases:
- Track team improvement on specific maps
- Monitor player performance trends
- Identify statistical anomalies

### Season Selector

Switch between seasons/divisions without page reload:
- Dropdown with all available data
- Automatically loads map stats for selected season
- Preserves selection in URL query params

### Responsive Design

Mobile-optimized:
- Collapsible navigation
- Touch-friendly tables
- Flexible grid layouts
- Portrait/landscape support

## Future Enhancements

- [ ] Charts/graphs for trend visualization
- [ ] Match-by-match timeline
- [ ] Player comparison tool
- [ ] Team vs team head-to-head
- [ ] Advanced filtering/search
- [ ] Export to CSV/PDF
- [ ] Dark/light theme toggle
- [ ] Internationalization (EN/FI)
