# get_championship_raw.py
"""
Fetch and print all raw data for a Faceit championship (division) by ID.
Reads API key from .env file (FACEIT_API_KEY).
"""
import os
import requests
from dotenv import load_dotenv
import argparse

load_dotenv()

API_KEY = os.getenv("FACEIT_API_KEY")


def fetch_championship_raw(championship_id: str):
    if not API_KEY:
        raise RuntimeError("FACEIT_API_KEY not set in .env file.")
    url = f"https://open.faceit.com/data/v4/championships/{championship_id}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Fetch raw Faceit championship data by ID.")
    parser.add_argument("championship_id", help="Faceit championship ID (UUID)")
    parser.add_argument("--out", help="Output file (optional, prints to stdout if omitted)")
    args = parser.parse_args()

    data = fetch_championship_raw(args.championship_id)
    import json
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved to {args.out}")
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
