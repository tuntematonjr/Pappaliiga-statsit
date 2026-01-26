"""
Standings calculation utilities for Pappaliiga.

Based on official Pappaliiga CS2 rules (section 6.1 - Liigamuoto):

Regular season tiebreaker priority:
1. Matches won (primary)
2. Round differential (Kierroserotus)
3. Head-to-head maps (Keskinäiset kartat)
4. Head-to-head round differential (Keskinäisten karttojen kierroserotus)
5. Random draw (arvonta) - using alphabetical team name as fallback

Note: Head-to-head comparisons are only applied between teams tied on
wins and overall round differential.
"""

from typing import Any, Optional


def calculate_standings(
    teams: list[dict[str, Any]],
    primary_key: str = 'matches_won',
    tiebreakers: Optional[list[str]] = None,
    h2h_data: Optional[dict[str, dict[str, Any]]] = None
) -> list[dict[str, Any]]:
    """
    Calculate team standings following Pappaliiga official rules.
    
    Args:
        teams: List of team dictionaries with stats
        primary_key: Primary sort field (default: 'matches_won')
        tiebreakers: List of fields for tiebreaking. Default:
                    ['round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name']
                    Supports both snake_case and camelCase field names
        h2h_data: Optional head-to-head data for tied teams.
                 Format: {team_id: {'h2h_maps_won': X, 'h2h_round_diff': Y}}
                 Required for h2h tiebreakers to work
    
    Returns:
        Sorted list of teams with 'position' field added
    
    Note: When h2h tiebreakers are specified but h2h_data is not provided,
          those tiebreakers are skipped (falls through to next tiebreaker).
    """
    if tiebreakers is None:
        # Official Pappaliiga tiebreaker order
        tiebreakers = ['round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name']
    
    if not teams:
        return []
    
    def get_field_value(team: dict, field: str) -> Any:
        """Get field value supporting snake_case, camelCase, and h2h data."""
        # Check for head-to-head fields
        if field.startswith('h2h_') and h2h_data:
            team_id = team.get('team_id') or team.get('teamId')
            if team_id and team_id in h2h_data:
                # Try snake_case h2h field
                if field in h2h_data[team_id]:
                    return h2h_data[team_id][field]
                # Try camelCase h2h field
                camel = ''.join(word.capitalize() if i > 0 else word 
                               for i, word in enumerate(field.split('_')))
                if camel in h2h_data[team_id]:
                    return h2h_data[team_id][camel]
            return 0  # No h2h data for this team
        
        # Regular field lookup
        if field in team:
            return team[field]
        # Try camelCase version
        camel = ''.join(word.capitalize() if i > 0 else word 
                       for i, word in enumerate(field.split('_')))
        return team.get(camel, 0)
    
    def comparison_key(team: dict) -> tuple:
        """Generate sort key tuple for a team."""
        # Primary key (descending - negate for numeric, reverse for strings)
        primary_val = get_field_value(team, primary_key)
        if isinstance(primary_val, (int, float)):
            key_list = [-primary_val]
        else:
            key_list = [str(primary_val)]
        
        # Tiebreakers
        for tb in tiebreakers:
            val = get_field_value(team, tb)
            if tb == 'team_name':
                # Ascending alphabetical for team names
                key_list.append(str(val).lower())
            elif isinstance(val, (int, float)):
                # Descending for numeric fields
                key_list.append(-val)
            else:
                # Ascending for other string fields
                key_list.append(str(val).lower())
        
        return tuple(key_list)
    
    # Sort teams
    sorted_teams = sorted(teams, key=comparison_key)
    
    # Add position numbers
    for idx, team in enumerate(sorted_teams, start=1):
        team['position'] = idx
    
    return sorted_teams


def calculate_round_differential(
    rounds_won: int,
    rounds_lost: int,
    forfeit_maps_won: int = 0,
    forfeit_maps_lost: int = 0
) -> int:
    """
    Calculate round differential including forfeit rounds.
    
    Per Pappaliiga rules, forfeited maps count as 13-0.
    
    Args:
        rounds_won: Rounds won in played maps
        rounds_lost: Rounds lost in played maps
        forfeit_maps_won: Number of maps won by forfeit
        forfeit_maps_lost: Number of maps lost by forfeit
    
    Returns:
        Total round differential
    """
    forfeit_rounds_won = forfeit_maps_won * 13
    forfeit_rounds_lost = forfeit_maps_lost * 13
    
    total_rounds_won = rounds_won + forfeit_rounds_won
    total_rounds_lost = rounds_lost + forfeit_rounds_lost
    
    return total_rounds_won - total_rounds_lost


def get_team_position(
    team_id: str,
    standings: list[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """
    Find a specific team in the standings.
    
    Args:
        team_id: ID of the team to find
        standings: Sorted standings list with positions
    
    Returns:
        Team dictionary with position, or None if not found
    """
    for team in standings:
        if team.get('team_id') == team_id or team.get('teamId') == team_id:
            return team
    return None


def compare_teams(
    team1: dict[str, Any],
    team2: dict[str, Any],
    criteria: Optional[list[str]] = None
) -> int:
    """
    Compare two teams using specified criteria.
    
    Args:
        team1: First team dictionary
        team2: Second team dictionary
        criteria: List of fields to compare (default: official Pappaliiga order)
    
    Returns:
        -1 if team1 ranks higher, 1 if team2 ranks higher, 0 if tied
    """
    if criteria is None:
        criteria = ['matches_won', 'round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name']
    
    for field in criteria:
        val1 = team1.get(field, 0)
        val2 = team2.get(field, 0)
        
        if field == 'team_name':
            # Ascending alphabetical
            val1 = str(val1).lower()
            val2 = str(val2).lower()
            if val1 < val2:
                return -1
            elif val1 > val2:
                return 1
        else:
            # Descending numeric
            if val1 > val2:
                return -1
            elif val1 < val2:
                return 1
    
    return 0


# Predefined sort configurations
SORT_BY_MATCHES = {
    'primary_key': 'matches_won',
    'tiebreakers': ['round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name']
}

SORT_BY_MAPS = {
    'primary_key': 'maps_won',
    'tiebreakers': ['round_diff', 'h2h_maps', 'h2h_round_diff', 'team_name']
}

SORT_BY_ROUNDS = {
    'primary_key': 'rounds_won',
    'tiebreakers': ['round_diff', 'h2h_round_diff', 'team_name']
}
