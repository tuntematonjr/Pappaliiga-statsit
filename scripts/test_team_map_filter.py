#!/usr/bin/env python3
"""Test script to verify that maps with zero plays don't show stats."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.services.teams_service import fetch_team_map_stats_comprehensive


async def test_team_map_stats():
    """Test that maps with zero plays are filtered out."""
    
    # Test with a team that doesn't play all maps
    champ_id = "8d243c3b-336b-4bac-899f-004358e64ee1"  # 4 Divisioona S11
    team_id = "da163e83-7643-489d-9bc3-2ba9bfb4202c"  # ++ crew
    
    print(f"Testing team map stats for team {team_id} in championship {champ_id}")
    
    map_stats = await fetch_team_map_stats_comprehensive(champ_id, team_id)
    
    print(f"\nReturned {len(map_stats)} maps (should only include played maps):")
    for m in sorted(map_stats, key=lambda x: x.get("played", 0), reverse=True):
        print(f"  {m['map_name']:20} - played: {m.get('played', 0)}, wins: {m.get('wins', 0)}, kills: {m.get('kills', 0)}, ADR: {m.get('adr', 0):.1f}")
    
    # Check that no map with played=0 is in the result
    zero_played_maps = [m for m in map_stats if m.get("played", 0) == 0]
    if zero_played_maps:
        print(f"\n❌ FAIL: Found {len(zero_played_maps)} maps with played=0:")
        for m in zero_played_maps:
            print(f"  - {m['map_name']}")
        return False
    else:
        print("\n✅ PASS: No maps with played=0 in the result")
        return True


async def main():
    """Main entry point."""
    success = await test_team_map_stats()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
