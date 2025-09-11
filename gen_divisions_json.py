# gen_divisions_json.py
# Safely update divisions.json from Faceit championships (Pappaliiga).
# - CS-only (cs2)
# - Non-destructive: never overwrite existing values, only fill missing ones
# - Add 'division_num' from leading number in name
# - Stable, unique slug: div{division_num}-s{season}[-po], with collision suffix if needed
# - For new entries, allocate a unique integer 'division_id' (do not touch existing)
# All comments in English per user preference.

from __future__ import annotations
import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from faceit_client import list_championships_for_organizer
from faceit_config import PAPPALIGA_ORG_ID

DIV_RX     = re.compile(r"(divisioona|division|mestaruussarja)", re.IGNORECASE)
LEAD_NUM   = re.compile(r"^\s*(\d{1,3})\s*[\.\-]?\s*")
SEASON_RX  = re.compile(r"(?:S|Season)\s*([0-9]{1,2})", re.IGNORECASE)
POFF_RX    = re.compile(r"playoff", re.IGNORECASE)
MESTAR_RX  = re.compile(r"mestaruussarja", re.IGNORECASE)

CS_TAGS = {"cs2"}


def parse_leading_divnum(name: str) -> Optional[int]:
    m = LEAD_NUM.match(name or "")
    if m:
        return int(m.group(1))
    # Mestaruussarja: ei numeroa → käytetään aina 0, jotta pysyy listan kärjessä
    if MESTAR_RX.search(name or ""):
        return 0
    return None

def parse_season(name: str) -> int:
    m = SEASON_RX.search(name or "")
    return int(m.group(1)) if m else 0


def is_playoffs(name: str) -> bool:
    return bool(POFF_RX.search(name or ""))


def is_cs_championship(ch: Dict[str, Any]) -> bool:
    # Prefer explicit 'game'/'game_id'
    game = (ch.get("game") or ch.get("game_id") or "").strip().lower()
    if game in CS_TAGS:
        return True

    return False


def base_slug(division_num: Optional[int], season: int, po: bool) -> str:
    if division_num is not None and season:
        return f"div{division_num}-s{season}{'-po' if po else ''}"
    # Fallback (should be rare)
    core = f"div{division_num}" if division_num is not None else "division"
    return f"{core}{'-po' if po else ''}"


def make_unique_slug(proposed: str, cid: str, already: set[str]) -> str:
    s = proposed
    if s in already:
        short = cid.replace("-", "")[:6]
        s = f"{s}-{short}"
    return s

def discover_cs_divisions(organizer_id: str, min_season: int = 0) -> List[Dict[str, Any]]:
    champs = list_championships_for_organizer(organizer_id)
    out: List[Dict[str, Any]] = []
    seen_cids: set[str] = set()

    for c in champs:
        cid  = c.get("championship_id") or c.get("id")
        name = (c.get("name") or "").strip()
        if not cid or not name:
            continue
        if cid in seen_cids:
            continue
        if not is_cs_championship(c):
            continue
        if not DIV_RX.search(name):
            continue

        dnum   = parse_leading_divnum(name)
        season = parse_season(name)
        po     = is_playoffs(name)

        # NEW: Mestaruussarja fallback (jos parse ei jo palauttanut 0)
        if dnum is None and MESTAR_RX.search(name):
            dnum = 0

        if season < min_season:
            continue

        game   = (c.get("game") or c.get("game_id") or "cs2").strip().lower() or "cs2"

        item = {
            "championship_id": cid,
            "name": name,
            "season": season,
            "division_num": dnum if dnum is not None else 0,
            "slug": base_slug(dnum if dnum is not None else 0, season, po),
            "game": game if game in CS_TAGS else "cs2",
            "is_playoffs": 1 if po else 0,
        }
        out.append(item)
        seen_cids.add(cid)

    out.sort(key=lambda d: (-int(d.get("season", 0)), int(d.get("division_num", 0))))
    return out

def load_existing(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        raise ValueError("divisions.json must be a JSON array")


def next_unique_division_id(existing: List[Dict[str, Any]]) -> int:
    nums = [int(e["division_id"]) for e in existing if isinstance(e.get("division_id"), int)]
    return (max(nums) + 1) if nums else 101  # start at 101 to avoid clashing with historical small ints


def non_destructive_merge(existing: List[Dict[str, Any]],
                          discovered: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Build maps for existing
    by_cid: Dict[str, Dict[str, Any]] = {}
    used_slugs: set[str] = set()
    for e in existing:
        if "slug" in e and isinstance(e["slug"], str):
            used_slugs.add(e["slug"])
        cid = e.get("championship_id")
        if cid:
            by_cid[cid] = dict(e)

    # Helper for division_id allocation
    def alloc_id() -> int:
        cur = list(by_cid.values())
        return next_unique_division_id(cur)

    # Merge discovered
    for d in discovered:
        cid = d["championship_id"]
        proposed_slug = d["slug"]
        if cid in by_cid:
            cur = by_cid[cid]
            # Finalize slug uniqueness if current row has none/empty
            if not cur.get("slug"):
                cur_slug = make_unique_slug(proposed_slug, cid, used_slugs)
                cur["slug"] = cur_slug
                used_slugs.add(cur_slug)
            else:
                used_slugs.add(cur["slug"])

            # Complement only missing/empty fields (do NOT overwrite existing values)
            for k, v in d.items():
                if k == "division_id":
                    continue  # never alter existing division_id
                if k not in cur:
                    cur[k] = v
                else:
                    curv = cur[k]
                    if isinstance(v, str):
                        if curv is None or (isinstance(curv, str) and curv.strip() == ""):
                            cur[k] = v
                    elif isinstance(v, int):
                        if curv is None or (isinstance(curv, int) and curv == 0 and v > 0):
                            cur[k] = v
                    else:
                        if curv is None:
                            cur[k] = v

            by_cid[cid] = cur
        else:
            # New championship → allocate unique division_id and finalize unique slug
            new_row = dict(d)
            new_row["division_id"] = alloc_id()
            uniq_slug = make_unique_slug(proposed_slug, cid, used_slugs)
            new_row["slug"] = uniq_slug
            used_slugs.add(uniq_slug)
            by_cid[cid] = new_row

    merged = list(by_cid.values())
    merged.sort(key=lambda x: (-int(x.get("season", 0)),
                               int(x.get("division_num", 0)),
                               int(x.get("division_id", 0)) if isinstance(x.get("division_id"), int) else 0))
    return merged


def main(out_path: str, dry_run: bool, min_season: int) -> None:
    out = Path(out_path)
    existing = load_existing(out)
    discovered = discover_cs_divisions(PAPPALIGA_ORG_ID, min_season=min_season)
    final = non_destructive_merge(existing, discovered)

    if dry_run:
        print(json.dumps(final, ensure_ascii=False, indent=2))
        print(f"\n[DRY-RUN] Would update {out.resolve()} – "
              f"{len(discovered)} discovered, {len(final)} total (no overwrites).")
        return

    out.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Updated {out.resolve()} – {len(discovered)} discovered, {len(final)} total (no overwrites).")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Safely update divisions.json with CS divisions (non-destructive).")
    p.add_argument("--out", default="divisions.json", help="Output JSON path (default: divisions.json)")
    p.add_argument("--dry-run", action="store_true", help="Print result without writing the file")
    p.add_argument("--min-season", type=int, default=11,
                    help="Skip adding divisions older than this season (default: 11 = include all)")
                    #    python gen_divisions_json.py --min-season 10
    args = p.parse_args()
    main(out_path=args.out, dry_run=args.dry_run, min_season=args.min_season)

