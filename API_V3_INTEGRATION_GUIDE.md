# Pappaliiga Stats API v3.1 - Integration Guide

## Overview

This document describes the new API structure introduced in v3.1, designed to provide comprehensive season and division statistics with full playoff support.

## New Endpoints

### 1. List All Seasons
**GET** `/api/seasons`

Returns a list of all available seasons with metadata.

**Response:**
```json
[
  {
    "id": 11,
    "name": "Season 11",
    "status": "finished",  // "finished" | "active" | "upcoming"
    "startDate": "2025-01-10",
    "endDate": "2025-03-15",
    "divisionsCount": 26
  }
]
```

---

### 2. Get Season Summary
**GET** `/api/seasons/{season_id}/summary`

Returns aggregated statistics for a specific season.

**Response:**
```json
{
  "seasonId": 11,
  "teams": 315,
  "players": 2262,
  "matches": 7090,
  "rounds": 151722,
  "kills": 528314,
  "deaths": 532357,
  "winRate": 0.95,
  "kdRatio": 0.992,
  "adrAvg": 86.7,
  "clutchWins": 210,
  "entryDiff": 145,
  "utilityDamage": 24000,
  "finishedPercent": 95.0,
  "progress": {
    "divisionsFinished": 24,
    "divisionsTotal": 26
  }
}
```

**Frontend Usage:**
```javascript
const summary = await apiClient.getSeasonSummary(11);
// Use SeasonSummaryBar component to display
import { SeasonSummaryBar } from './components/SeasonSummaryBar.js';
const summaryBar = SeasonSummaryBar({ summary: summary.data });
```

---

### 3. Get Season Divisions
**GET** `/api/seasons/{season_id}/divisions`

Returns all divisions for a season with embedded season and playoff statistics.

**Response:**
```json
[
  {
    "divisionId": "1-abc123",
    "tier": 1,
    "name": "1 Divisioona",
    "status": "finished",
    "isPlayoff": false,
    "season": {
      "teams": 12,
      "matchesPlayed": 88,
      "matchesTotal": 120,
      "finishedPercent": 73.3
    },
    "playoffs": {
      "status": "finished",
      "teams": 8,
      "matchesPlayed": 7,
      "matchesTotal": 7,
      "winner": "AFI Strike"
    },
    "winners": [
      {"teamName": "AFI Strike", "place": 1},
      {"teamName": "Terrorbyte", "place": 2}
    ],
    "bestPlayer": {
      "name": "player1",
      "rating": 1.21
    },
    "mvpTeam": "AFI Strike"
  }
]
```

**Frontend Usage:**
```javascript
const divisions = await apiClient.getDivisions(11);
// DivisionCardList automatically handles the new structure
```

---

### 4. Get Division Detailed Stats
**GET** `/api/seasons/{season_id}/divisions/{division_id}/stats`

Returns detailed breakdown for a specific division including team stats, player leaderboards, and playoff bracket.

**Response:**
```json
{
  "divisionId": "1-abc123",
  "season": {
    "teams": [
      {
        "name": "AFI Strike",
        "matches": 20,
        "wins": 16,
        "losses": 4,
        "rounds": 460,
        "kills": 2100,
        "deaths": 1800,
        "adr": 92.5,
        "rating": 1.14
      }
    ],
    "playerLeaders": {
      "topFrags": {"player": "PlayerX", "value": 654},
      "bestKd": {"player": "PlayerY", "value": 1.54},
      "mostMvps": {"player": "PlayerZ", "value": 42}
    }
  },
  "playoffs": {
    "matchesPlayed": 7,
    "matchesTotal": 7,
    "bracket": [
      {
        "round": 1,
        "matchId": "1-match123",
        "team1": "AFI Strike",
        "team2": "Terrorbyte",
        "winner": "AFI Strike"
      }
    ]
  }
}
```

**Frontend Usage:**
```javascript
const stats = await apiClient.getDivisionDetailedStats(11, '1-abc123');
// Use in DivisionView to display team tables and playoff brackets
```

---

### 5. Health Check
**GET** `/api/health`

Simple health endpoint for monitoring and status checks.

**Response:**
```json
{
  "ok": true,
  "version": "v3.1",
  "uptime": 123456
}
```

---

## Frontend API Client Integration

### Updated Methods

The `apiClient` has been enhanced with new methods:

```javascript
// Get list of seasons
const seasons = await apiClient.getSeasons();

// Get season summary (enhanced)
const summary = await apiClient.getSeasonSummary(seasonId);

// Get divisions (enhanced with new data)
const divisions = await apiClient.getDivisions(seasonId);

// NEW: Get detailed division stats
const divisionStats = await apiClient.getDivisionDetailedStats(seasonId, divisionId);
```

### Route Fallback System

The API client automatically tries multiple route patterns for compatibility:

```javascript
// For seasons list
['/api/seasons', '/api/v1/seasons']

// For season summary
[
  '/api/seasons/{id}/summary',
  '/api/v1/seasons/{id}/summary',
  '/api/seasons/{id}/stats/summary'
]

// For season divisions
[
  '/api/seasons/{id}/divisions',
  '/api/v1/seasons/{id}/divisions',
  '/api/divisions/season/{id}'
]

// For division stats
[
  '/api/seasons/{seasonId}/divisions/{divisionId}/stats',
  '/api/v1/seasons/{seasonId}/divisions/{divisionId}/stats',
  '/api/divisions/{divisionId}/stats'
]
```

---

## Component Integration

### SeasonSummaryBar Component

Display season-wide aggregated statistics.

**Import:**
```javascript
import { SeasonSummaryBar } from './components/SeasonSummaryBar.js';
```

**Usage:**
```javascript
const summary = await apiClient.getSeasonSummary(11);
const summaryBar = SeasonSummaryBar({ summary: summary.data });
document.querySelector('#summary-container').appendChild(summaryBar);
```

### Enhanced DivisionCardList

The `DivisionCardList` component now displays:
- Best player with rating
- MVP team
- Winners list (for finished divisions)
- Enhanced playoff information

**Usage (unchanged):**
```javascript
<division-card-list
  :divisions="divisions"
  :season-options="seasonOptions"
  :selected-season="currentSeason"
  @change-season="handleSeasonChange"
/>
```

---

## Data Flow Example

```javascript
// 1. Load seasons list
const seasons = await apiClient.getSeasons();

// 2. Select a season and load summary
const summary = await apiClient.getSeasonSummary(seasons[0].id);

// 3. Display summary bar
const summaryBar = SeasonSummaryBar({ summary: summary.data });

// 4. Load divisions
const divisions = await apiClient.getDivisions(seasons[0].id);

// 5. When user clicks a division, load detailed stats
const divisionStats = await apiClient.getDivisionDetailedStats(
  seasons[0].id,
  divisions[0].divisionId
);
```

---

## Offline/Fallback Behavior

The API client includes:
- **Retry logic**: 2 retries with exponential backoff
- **Caching**: ETag support + localStorage fallback
- **Circuit breaker**: Opens after 3 consecutive failures, cooldown 30s
- **Timeout**: 8 seconds per request

If the API is unreachable:
1. Cached data is served (if available)
2. "Offline mode" banner displayed
3. Circuit breaker prevents excessive retries

---

## Migration Notes

### From Old Endpoints

- **Old:** `/api/divisions/season/{id}` → **New:** `/api/seasons/{id}/divisions`
- **Old:** `/api/seasons/{id}/stats` → **New:** `/api/seasons/{id}/summary`

Both old and new endpoints are supported for backward compatibility.

### Data Structure Changes

Division objects now include:
- `bestPlayer`: { name, rating }
- `mvpTeam`: string
- `winners`: [{ teamName, place }]
- Enhanced `playoffs` object with detailed bracket

---

## Error Handling

```javascript
try {
  const summary = await apiClient.getSeasonSummary(11);
} catch (error) {
  if (error instanceof ApiEndpointNotFound) {
    console.warn('Endpoint not found, trying fallback');
  } else {
    console.error('Failed to load summary', error);
  }
}
```

---

## Performance Considerations

- **Caching:** All responses cached for 5 minutes by default
- **Batch loading:** Consider loading seasons list once at app startup
- **Lazy loading:** Division details only loaded when user clicks
- **Progressive rendering:** DivisionCardList uses virtual scrolling for large lists

---

## Future Enhancements

Planned for upcoming releases:
- Real-time WebSocket updates for live matches
- GraphQL endpoint for flexible queries
- Paginated division lists for very large seasons
- CSV/JSON export for statistics

---

## Support & Questions

For issues or questions:
- Check browser console for detailed error messages
- API docs available at `/docs` (Swagger UI)
- Test endpoints manually using `/docs` interactive interface

---

**Last Updated:** December 2024  
**API Version:** v3.1  
**Frontend Compatibility:** All modern browsers (ES6+)
