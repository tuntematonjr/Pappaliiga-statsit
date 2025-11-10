"""Quick test script for V3 API"""
import requests
import json

# Test summary endpoint
print("=" * 60)
print("Testing /api/v3/summary/11")
print("=" * 60)
r = requests.get('http://localhost:8000/api/v3/summary/11')
summary = r.json()
print(json.dumps(summary, indent=2))

# Test divisions endpoint
print("\n" + "=" * 60)
print("Testing /api/v3/divisions/11")
print("=" * 60)
r = requests.get('http://localhost:8000/api/v3/divisions/11')
divisions = r.json()
print(f"Total divisions returned: {len(divisions)}")
print("\nFirst 3 divisions:")
for div in divisions[:3]:
    print(f"\nTier {div['tier']}: {div['name']}")
    print(f"  Status: {div['status']}")
    print(f"  Season: {div['season']['teams']} teams, {div['season']['matches_played']}/{div['season']['matches_total']} matches")
    print(f"  Playoffs: {div['playoffs']['status']}, {div['playoffs']['matches_played']}/{div['playoffs']['matches_total']} matches")
    if div['playoffs']['winner_team']:
        print(f"  Winner: {div['playoffs']['winner_team']}")

# Count by status
print("\n" + "=" * 60)
print("Division Status Breakdown:")
print("=" * 60)
from collections import Counter
status_count = Counter(d['status'] for d in divisions)
for status, count in status_count.items():
    print(f"  {status}: {count}")

# Check for duplicates
print("\n" + "=" * 60)
print("Checking for duplicate divisions:")
print("=" * 60)
division_ids = [d['division_id'] for d in divisions]
if len(division_ids) == len(set(division_ids)):
    print("✓ No duplicates found - each division appears exactly once")
else:
    print("✗ Duplicates found!")
    from collections import Counter
    dupes = [id for id, count in Counter(division_ids).items() if count > 1]
    print(f"  Duplicate IDs: {dupes}")

# Check tier distribution
print("\n" + "=" * 60)
print("Tier Distribution:")
print("=" * 60)
tier_count = Counter(d['tier'] for d in divisions)
for tier in sorted(tier_count.keys()):
    print(f"  Tier {tier}: {tier_count[tier]} divisions")
