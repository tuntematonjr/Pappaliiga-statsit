"""Check for divisions with missing data"""
import requests
import json

r = requests.get('http://localhost:8000/api/v3/divisions/11')
divisions = r.json()

print("Checking for divisions with missing/empty data:\n")
print("=" * 80)

empty_divisions = []
for d in divisions:
    has_issue = False
    issues = []
    
    if d['season']['teams'] == 0:
        issues.append("No teams")
        has_issue = True
    
    if d['season']['matches_total'] == 0:
        issues.append("No matches_total")
        has_issue = True
    
    if d['season']['matches_played'] == 0 and d['season']['matches_total'] > 0:
        issues.append("No matches played yet")
    
    if has_issue:
        empty_divisions.append({
            'tier': d['tier'],
            'name': d['name'],
            'division_id': d['division_id'],
            'issues': issues,
            'season': d['season']
        })

if empty_divisions:
    print(f"Found {len(empty_divisions)} divisions with issues:\n")
    for div in empty_divisions:
        print(f"Tier {div['tier']}: {div['name']}")
        print(f"  Division ID: {div['division_id']}")
        print(f"  Issues: {', '.join(div['issues'])}")
        print(f"  Season data: {div['season']}")
        print()
else:
    print("✓ All divisions have complete data!")

print("=" * 80)
print(f"\nTotal divisions: {len(divisions)}")
print(f"Divisions with issues: {len(empty_divisions)}")
print(f"Divisions OK: {len(divisions) - len(empty_divisions)}")
