# debug_raw.py
import argparse, json, sys
from faceit_client import get_match_details, get_match_stats, get_democracy_history

def jdump(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))

def main():
    ap = argparse.ArgumentParser(description="Print raw Faceit data for a match (details, stats, democracy).")
    ap.add_argument("--match", required=True, help="Faceit match_id (e.g. 1-xxxx-...)")
    ap.add_argument("--no-details", action="store_true", help="Skip match details")
    ap.add_argument("--no-stats", action="store_true", help="Skip match stats")
    ap.add_argument("--no-votes", action="store_true", help="Skip democracy history (votes)")
    args = ap.parse_args()

    mid = args.match

    if not args.no_details:
        print("\n=== RAW: match details ===")
        try:
            details = get_match_details(mid)
            jdump(details)
        except Exception as e:
            print(f"[ERR] details fetch failed: {e}", file=sys.stderr)

    if not args.no_stats:
        print("\n=== RAW: match stats ===")
        try:
            stats = get_match_stats(mid)
            jdump(stats)
            # Lyhyt nosto: montako roundia statsissa
            rounds = stats.get("rounds") or []
            print(f"\n[INFO] rounds in stats: {len(rounds)}")
        except Exception as e:
            print(f"[ERR] stats fetch failed: {e}", file=sys.stderr)

    if not args.no_votes:
        print("\n=== RAW: democracy history (votes) ===")
        try:
            votes = get_democracy_history(mid)
            jdump(votes)
        except Exception as e:
            print(f"[ERR] votes fetch failed: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()

# python debug_raw.py --match 1-23c2671c-5d08-4ee1-b84c-5a09fd79749d