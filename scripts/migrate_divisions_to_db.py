#!/usr/bin/env python3
"""One-time migration: seed the DB ``championships`` table from ``divisions.json``.

Run this once to transfer any locally-edited ``divisions.json`` entries into
the database.  After it completes successfully you can delete ``divisions.json``.

Usage::

    python scripts/migrate_divisions_to_db.py [--divisions-json PATH] [--dry-run]

The script is idempotent: rows already present in the DB are updated only for
fields that the upsert helper allows (see ``db_async.upsert_championships_async``
which protects existing ``name`` / ``slug`` values via ``ON DUPLICATE KEY UPDATE``).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Ensure the repo root is on sys.path when run from scripts/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

import db_async
from env_loader import load_env

load_env()


async def _run(divisions_json: Path, dry_run: bool) -> int:
    if not divisions_json.exists():
        print(f"ERROR: {divisions_json} not found.")
        return 1

    try:
        entries: list = json.loads(divisions_json.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"ERROR: could not parse {divisions_json}: {exc}")
        return 1

    if not isinstance(entries, list):
        print(f"ERROR: expected JSON array in {divisions_json}, got {type(entries).__name__}")
        return 1

    print(f"Loaded {len(entries)} entries from {divisions_json}")

    if dry_run:
        print("Dry run — no changes written to DB.")
        for e in entries[:5]:
            print(" ", e)
        if len(entries) > 5:
            print(f"  ... and {len(entries) - 5} more")
        return 0

    await db_async.get_pool()
    async with db_async.connection(label="migrate_divisions") as conn:
        await db_async.upsert_championships_async(conn, entries)

    print(f"Upserted {len(entries)} entries into the championships table.")
    await db_async.close_pool()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate divisions.json → DB championships table")
    parser.add_argument(
        "--divisions-json",
        default=str(_REPO_ROOT / "divisions.json"),
        help="Path to divisions.json (default: repo root)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only; do not write to DB")
    args = parser.parse_args()

    rc = asyncio.run(_run(Path(args.divisions_json), args.dry_run))
    sys.exit(rc)


if __name__ == "__main__":
    main()
