# Team Status Management Tool - Usage Guide

## Overview
`manage_team_status.py` is an interactive CLI tool to manage banned and quit teams for Pappaliiga divisions.

## Features
- **Interactive Mode**: Step-by-step prompts to select divisions and teams
- **CLI Arguments**: Scriptable commands for automation
- **Both Status Types**: Handles both `banned` and `quit` teams uniformly

## Usage Modes

### 1. Interactive Mode (Recommended)
Run without arguments to enter interactive mode:
```bash
python manage_team_status.py
```

This will:
1. Show all active divisions
2. Let you select a division
3. Show all teams in that division
4. Let you select a team
5. Ask if the team is banned or quit
6. Ask for a reason (optional)
7. Confirm and save the entry

### 2. List Current Overrides
```bash
python manage_team_status.py list
```

Shows all currently configured banned/quit teams across all divisions.

### 3. Add Entry via Arguments
```bash
python manage_team_status.py add \
  --championship-id <championship_id> \
  --team-id <team_id> \
  --team-name "Team Name" \
  --status banned \
  --reason "Rule violation" \
  --avatar "https://..."
```

**Status options**: `banned` or `quit`

### 4. Remove Entry
```bash
# Remove from all status lists
python manage_team_status.py remove \
  --championship-id <championship_id> \
  --team-id <team_id>

# Remove only from specific status
python manage_team_status.py remove \
  --championship-id <championship_id> \
  --team-id <team_id> \
  --status banned
```

## Data Structure

The tool manages `division_overrides.json`:

```json
{
  "<championship_id>": {
    "banned_teams": [
      {
        "team_id": "...",
        "team_name": "...",
        "reason": "...",
        "banned_at": "YYYY-MM-DD",
        "avatar": "...",
        "status": "banned"
      }
    ],
    "quit_teams": [
      {
        "team_id": "...",
        "team_name": "...",
        "reason": "...",
        "quit_at": "YYYY-MM-DD",
        "avatar": "...",
        "status": "quit"
      }
    ]
  }
}
```

## Integration with Sync/HTML Generation

Both `sync.py` and `html_gen.py` automatically:
- Read `division_overrides.json`
- Apply season-specific exclusions (bans/quits only affect their specific championship)
- Exclude banned/quit teams from statistics calculations
- Display status annotations in generated HTML pages (grayed out with status text)

## Important Notes

- Teams must exist in the database (run `sync.py` first)
- Interactive mode requires database access to list teams
- Manual JSON editing is supported but the tool is safer
- Both status types (`banned` and `quit`) are treated identically for stats purposes
- The tool automatically sorts entries by team name

## Examples

### Example 1: Mark a team as banned
```bash
python manage_team_status.py
# Select division: 6
# Select team: MOTI esports
# Status: banned
# Reason: Player eligibility violation
# Confirm: y
```

### Example 2: Mark a team as quit
```bash
python manage_team_status.py add \
  --championship-id b00756dc-dd5b-4f31-a64c-5f6f4db8b8f6 \
  --team-id abc123 \
  --team-name "Roster Collapse" \
  --status quit \
  --reason "Unable to field lineup"
```

### Example 3: Remove an entry
```bash
python manage_team_status.py remove \
  --championship-id b00756dc-dd5b-4f31-a64c-5f6f4db8b8f6 \
  --team-id abc123
```

## Error Handling

- **No teams found**: Ensure database is synced with `python sync.py`
- **Invalid JSON**: The tool validates JSON before loading
- **Missing division**: Check that `divisions.json` is up to date
- Press `q` or `Ctrl+C` to cancel interactive mode at any time
