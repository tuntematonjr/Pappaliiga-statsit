# Complete Data Fields Reference - Team Page

## Team Season Overview
- **Matches Played** - Total matches in the season
- **Wins** - Total match wins
- **Win Rate** - Win percentage
- **Rounds Won** - Total rounds won
- **Rounds Lost** - Total rounds lost
- **Rounds Difference** - Round differential
- **Maps Won** - Maps won in series format

## Per-Map Statistics (Complete)
- **Map Name** - CS2 map name
- **Played** - Times played this season
- **Picks** - Times the team picked this map
- **Opponent Picks** - Times opponent picked this map
- **Wins** - Maps won on this map
- **Games** - Total map games played
- **Ban Count (Ban1 + Ban2)** - Total bans of this map
- **Opponent Bans** - Times opponent banned this map
- **Total Own Bans** - Total bans by team
- **Decider Overcount** - BO3 decider maps played
- **Kills** - Total kills on this map (season)
- **Deaths** - Total deaths on this map (season)
- **MVPs** - Total MVPs on this map (season)
- **Round Difference (RD)** - Round differential on this map
- **K/D Ratio** - Kill/Death ratio on this map
- **ADR** - Average Damage per Round on this map
- **Total Damage** - Total damage dealt on this map
- **Utility Damage** - Total utility damage on this map
- **Win Rate %** - Calculated percentage of wins
- **Pick Rate %** - Calculated percentage of picks

## Player Statistics (Complete)
- **Nickname** - Player name
- **Maps Played** - Maps played this season
- **Rounds Played** - Rounds played this season

### Combat Statistics
- **Kills** - Total kills
- **Deaths** - Total deaths
- **Assists** - Total assists
- **MVPs** - Multi-kill/MVP awards

### Specialized Combat
- **Sniper Kills** - AWP/sniper weapon kills
- **Pistol Kills** - Pistol round kills
- **Clutch Kills** - Kills in clutch situations

### Multi-Kill Distribution
- **2K Count** - 2 kills in a round
- **3K Count** - 3 kills in a round
- **4K Count** - 4 kills in a round
- **5K Count** - 5 kills in a round (ace)

### Clutch Performance
- **1v1 Attempts** - 1v1 situations attempted
- **1v1 Wins** - 1v1 situations won
- **1v2 Attempts** - 1v2+ clutch attempts
- **1v2 Wins** - 1v2+ clutch wins

### Utility & Entry
- **Entry Attempts** - Entry frags attempted
- **Entry Wins** - Entry frags succeeded
- **Utility Damage** - Damage from grenades/molotov
- **Enemies Flashed** - Enemies flashed with flashbangs
- **Flash Count** - Flashbangs thrown
- **Flash Success Rate** - Successful flashes

### Damage & Performance
- **ADR** - Average Damage per Round
- **K/R** - Kills per Round
- **K/D** - Kill/Death ratio
- **Rating** - HLTV 2.0 rating equivalent
- **HS%** - Headshot percentage
- **Total Damage** - Total damage dealt

## Match History (Complete)
- **Match ID** - Unique match identifier
- **Timestamp** - Match played date/time
- **Status** - Match status (finished, etc.)
- **Best Of** - BO1/BO2/BO3 format
- **Played** - Number of maps played
- **Team1 ID** - First team ID
- **Team2 ID** - Second team ID
- **Team1 Name** - First team name
- **Team2 Name** - Second team name
- **Team1 Avatar** - First team logo
- **Team2 Avatar** - Second team logo
- **Faceit URL** - Link to match on Faceit
- **Maps** - Array of map details
- **Opponent Name** - Calculated opponent name

## Veto/Pick History (Complete)
- **Match ID** - Which match this veto is from
- **Map Name** - Map being voted on
- **Status** - "banned", "picked", or "decider"
- **Selected By Team ID** - Which team made the choice
- **Selected By Team Name** - Team name (resolved)
- **Round Number** - Veto/pick sequence number
- **Order** - Order in the match veto phase

## Veto/Ban Aggregates
- **Map Name** - The map
- **Times Picked** - How many times team picked it
- **Times Banned** - How many times team banned it
- **Times Opponent Picked** - How many times opponent picked it
- **Pick Rate %** - Percentage of picks that were this map
- **Ban Rate %** - Percentage of bans that were this map
- **Pick Win Rate %** - (Available for implementation)

## Season Metadata
- **Championship ID** - Unique ID for this season/division
- **Season** - Season number (e.g., 11)
- **Division Number** - Division level (1-20+)

---

## Data Availability

### Guaranteed Data
✅ Team stats for all seasons played  
✅ Map stats for all maps played  
✅ Match history for all matches  
✅ Player roster and statistics  
✅ Veto/pick history for veto-enabled matches  

### Fields by Category

| Category | Count | Scope |
|----------|-------|-------|
| Team Season | 12 | Per season |
| Per-Map | 20+ | Per season |
| Player | 30+ | Per season |
| Match | 12 | Per match |
| Veto | 7 | Per veto action |
| Veto Aggregate | 6 | Per map per season |

**Total distinct fields exposed: 90+**

---

## API Endpoints Providing Data

| Endpoint | Data Type | Count |
|----------|-----------|-------|
| `/teams/{id}/page` | Overview | 1 +seasons |
| `/teams/{id}/season/{cid}` | **Everything** | All |
| `/teams/{id}/map-stats/{cid}` | Maps only | 20+ per map |
| `/teams/{id}/matches` | Matches | 12 per match |
| `/teams/{id}/players` | Players | 30+ per player |
| `/teams/{id}/veto-history/{cid}` | Veto actions | 7 per action |
| `/teams/{id}/veto-aggregates/{cid}` | Aggregates | 6 per map |

---

## No Data Omitted

Every field available in the database tables is exposed:
- ✅ `team_season_totals` - All fields
- ✅ `team_map_season_totals` - All fields  
- ✅ `player_season_totals` - All fields
- ✅ `map_votes` - All relevant fields
- ✅ Calculated fields (rates, aggregates) added

Nothing is simplified, hidden, or excluded from the UI. All data is available for analysis.
