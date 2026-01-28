import argparse
import json
from faceit_client import list_championship_matches


def main() -> None:
    p = argparse.ArgumentParser(description="Debug: list matches for a championship")
    p.add_argument("--championship-id", required=True, help="Faceit championship UUID")
    p.add_argument("--type", default="all", choices=["all", "past", "ongoing", "upcoming"], help="Match type")
    p.add_argument("--limit", type=int, default=20, help="Page size for API requests")
    p.add_argument("--output", default="debug_matches_output.json", help="Output JSON file (default: debug_matches_output.json)")
    args = p.parse_args()

    matches = list_championship_matches(args.championship_id, match_type=args.type, limit=args.limit)
    payload = {
        "count": len(matches),
        "championship_id": args.championship_id,
        "match_type": args.type,
        "items": matches,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"✓ Wrote {len(matches)} matches to {args.output}")


if __name__ == "__main__":
    main()
