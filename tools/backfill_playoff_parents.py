#!/usr/bin/env python3
"""Backfill parent_championship_id for playoff divisions.

This script infers the parent division for playoff championships by:
1. Slug base matching (strip -po, -playoffs, -pudotuspelit suffixes)
2. Season + division_num matching
3. Explicit parent_championship_id if already set

Usage:
    python tools/backfill_playoff_parents.py --dry-run  # Preview changes
    python tools/backfill_playoff_parents.py --apply    # Apply changes
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from db_async import connection, readonly_connection  # noqa: E402
from env_loader import load_env  # noqa: E402

load_env(PROJECT_ROOT)


def derive_slug_base(slug: str) -> str:
    """Strip playoff suffixes from slug to get base slug."""
    if not slug:
        return ""
    base = slug.lower().strip()
    # Remove playoff-related suffixes
    base = re.sub(r'-(?:playoffs?|po|pudotuspelit).*$', '', base)
    base = re.sub(r'-(?:rk|runko|regular).*$', '', base)
    base = re.sub(r'-+', '-', base)
    base = base.strip('-')
    return base


async def find_parent_championship(
    playoff_champ: dict,
    all_championships: list[dict]
) -> str | None:
    """Find the most likely parent championship for a playoff division.
    
    Returns parent championship_id or None if no match found.
    """
    playoff_id = playoff_champ['championship_id']
    playoff_slug = playoff_champ.get('slug', '')
    playoff_season = playoff_champ.get('season')
    playoff_div_num = playoff_champ.get('division_num')
    
    playoff_slug_base = derive_slug_base(playoff_slug)
    
    candidates = []
    
    for champ in all_championships:
        # Skip self
        if champ['championship_id'] == playoff_id:
            continue
        
        # Skip other playoffs
        if champ.get('is_playoffs'):
            continue
        
        champ_slug = champ.get('slug', '')
        champ_slug_base = derive_slug_base(champ_slug)
        champ_season = champ.get('season')
        champ_div_num = champ.get('division_num')
        
        score = 0
        
        # Slug base match (strongest signal)
        if playoff_slug_base and champ_slug_base == playoff_slug_base:
            score += 100
        
        # Season match
        if playoff_season is not None and champ_season == playoff_season:
            score += 50
        
        # Division number match
        if playoff_div_num is not None and champ_div_num == playoff_div_num:
            score += 30
        
        if score > 0:
            candidates.append((score, champ['championship_id'], champ.get('name', 'Unknown')))
    
    if not candidates:
        return None
    
    # Return highest scoring candidate
    candidates.sort(reverse=True, key=lambda x: x[0])
    return candidates[0][1]


async def main(dry_run: bool = True):
    """Main backfill logic."""
    print(f"Starting playoff parent backfill (dry_run={dry_run})")
    print("-" * 60)
    
    # Fetch all championships
    async with readonly_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    championship_id,
                    season,
                    division_num,
                    name,
                    is_playoffs,
                    slug,
                    parent_championship_id
                FROM championships
                ORDER BY season DESC, division_num ASC
            """)
            rows = await cur.fetchall()
            
    if not rows:
        print("No championships found in database.")
        return
    
    # Convert to dict list
    columns = ['championship_id', 'season', 'division_num', 'name', 
               'is_playoffs', 'slug', 'parent_championship_id']
    all_champs = [dict(zip(columns, row)) for row in rows]
    
    # Find playoff divisions
    playoff_champs = [c for c in all_champs if c.get('is_playoffs')]
    
    print(f"Found {len(all_champs)} total championships")
    print(f"Found {len(playoff_champs)} playoff divisions")
    print()
    
    if not playoff_champs:
        print("No playoff divisions to process.")
        return
    
    updates = []
    orphans = []
    already_set = []
    
    for playoff_champ in playoff_champs:
        playoff_id = playoff_champ['championship_id']
        current_parent = playoff_champ.get('parent_championship_id')
        
        if current_parent:
            already_set.append((playoff_id, playoff_champ['name'], current_parent))
            continue
        
        # Find parent
        parent_id = await find_parent_championship(playoff_champ, all_champs)
        
        if parent_id:
            parent_name = next(
                (c['name'] for c in all_champs if c['championship_id'] == parent_id),
                'Unknown'
            )
            updates.append((playoff_id, playoff_champ['name'], parent_id, parent_name))
        else:
            orphans.append((playoff_id, playoff_champ['name'], playoff_champ.get('slug', '')))
    
    # Report findings
    print(f"Already have parent set: {len(already_set)}")
    if already_set and not dry_run:
        for pid, pname, parent_id in already_set[:5]:
            print(f"  - {pname} ({pid}) -> {parent_id}")
        if len(already_set) > 5:
            print(f"  ... and {len(already_set) - 5} more")
    print()
    
    print(f"Can set parent: {len(updates)}")
    for pid, pname, parent_id, parent_name in updates:
        print(f"  - {pname} ({pid})")
        print(f"    -> {parent_name} ({parent_id})")
    print()
    
    print(f"Orphans (no parent found): {len(orphans)}")
    for pid, pname, slug in orphans:
        print(f"  - {pname} ({pid}, slug={slug})")
    print()
    
    if not updates:
        print("No updates to apply.")
        return
    
    if dry_run:
        print("Dry-run mode: No changes applied.")
        print("Run with --apply to update the database.")
        return
    
    # Apply updates
    print(f"Applying {len(updates)} updates...")
    async with connection() as conn:
        async with conn.cursor() as cur:
            for pid, pname, parent_id, parent_name in updates:
                await cur.execute(
                    """
                    UPDATE championships 
                    SET parent_championship_id = %s
                    WHERE championship_id = %s
                    """,
                    (parent_id, pid)
                )
                print(f"  ✓ Updated {pname}")
    
    print()
    print(f"Successfully updated {len(updates)} playoff divisions.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill parent_championship_id for playoff divisions')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Preview changes without applying (default)')
    parser.add_argument('--apply', action='store_true',
                        help='Apply changes to database')
    
    args = parser.parse_args()
    
    # If --apply is set, turn off dry-run
    dry_run = not args.apply
    
    asyncio.run(main(dry_run=dry_run))
