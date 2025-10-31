# Pappaliiga Stats - Vue.js Frontend

Modern single-page application for CS2 tournament statistics with detailed per-map analytics.

## Features

- **Dynamic Data Loading**: Fetches data from REST API
- **Per-Map Statistics**: Team and player breakdowns with curr/prev/delta indicators
- **Responsive Design**: Works on desktop and mobile
- **Vue Router**: Client-side routing for seamless navigation
- **Real-time Updates**: No page reloads required

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
    ├── api-client.js            # API wrapper
    ├── app-main.js              # Vue app initialization
    ├── styles.css               # Global styles
    ├── components/
    │   ├── LoadingSpinner.js    # Loading state component
    │   ├── ErrorMessage.js      # Error display component
    │   ├── DeltaIndicator.js    # Curr/prev/delta display
    │   └── MapStatsTable.js     # Per-map stats table
    └── views/
        ├── HomeView.js          # Landing page with stats overview
        ├── SeasonsView.js       # Seasons/divisions list
        ├── DivisionView.js      # Division details
        ├── TeamView.js          # Team stats with map breakdown
        └── PlayerView.js        # Player stats with map breakdown
```

## Components

### Delta Indicator

Shows current value with optional delta arrow and tooltip:

```javascript
<delta-indicator 
    :value="curr.kd" 
    :delta="delta.kd"
    :prev="prev.kd"
    format="decimal"
    :show-delta="true"
/>
```

- **Green ↑**: Positive improvement
- **Red ↓**: Negative change
- Tooltip shows previous value and change amount

### Map Stats Table

Displays per-map performance with sortable columns:

```javascript
<map-stats-table 
    :map-stats="mapStats"
    title="Per-Map Performance"
    :show-wins="true"
    :show-rating="true"
    :show-mvps="true"
/>
```

Features:
- Click column headers to sort
- Automatic delta calculations
- Responsive table layout

## Development

### Local Setup

1. **Start API Server**:
   ```bash
   python -m api.main
   ```

2. **Open Browser**:
   Navigate to `http://localhost:8000`

3. **Development Mode**:
   - Uses CDN-hosted Vue/Vue Router (no build step)
   - Edit `.js` files and refresh browser
   - Hot reload on file changes (via Uvicorn)

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

All API calls use the global `apiClient` instance:

```javascript
// Get team info
const team = await window.apiClient.getTeamInfo(teamId);

// Get map stats with deltas
const mapStats = await window.apiClient.getTeamMapStats(teamId, championshipId);
```

Response format for map stats:
```json
{
    "map_name": "de_dust2",
    "curr": { "maps_played": 15, "kd": 1.25, ... },
    "prev": { "maps_played": 12, "kd": 1.18, ... },
    "delta": { "maps_played": 3, "kd": 0.07, ... }
}
```

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

Edit `frontend/static/styles.css` to change colors, spacing, etc.

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
