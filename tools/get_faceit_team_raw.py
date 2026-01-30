import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from faceit_client_async import _get_clients, _request_json  # noqa: E402


async def fetch_team(team_id: str) -> dict | None:
    open_client, _ = await _get_clients()
    return await _request_json(open_client, "GET", f"/teams/{team_id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch raw Faceit team JSON.")
    parser.add_argument("team_id", help="Faceit team id")
    parser.add_argument(
        "--out",
        default="raw_team.json",
        help="Output file path (default: raw_team.json)",
    )
    return parser


async def main_async(args: argparse.Namespace) -> int:
    data = await fetch_team(args.team_id)
    if data is None:
        print("No data returned (check team id or API credentials).")
        return 1

    print(json.dumps(data, indent=2, ensure_ascii=False))
    out_path = Path(args.out)
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out_path}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
