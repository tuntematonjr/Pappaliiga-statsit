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
from faceit_config import PAPPALIIGA_ORG_ID

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
    # Mestaruussarja has no number → always use 0 so it stays at the top of lists
    if MESTAR_RX.search(name or ""):
        return 0
    return None

def parse_season(name: str, description: str = "") -> int:
    # Try to find season in name first
    m = SEASON_RX.search(name or "")
    if m:
        return int(m.group(1))
    
    # If not found in name, try description
    m = SEASON_RX.search(description or "")
    if m:
        return int(m.group(1))
    
    # Default to 0 if not found anywhere
    return 0


def is_playoffs(name: str) -> bool:
    return bool(POFF_RX.search(name or ""))


def is_cs_championship(ch: Dict[str, Any]) -> bool:
    # All championships in divisions.json are CS2-only
    return True


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

def discover_cs_divisions(organizer_id: str, min_season: int = 0) -> tuple[List[Dict[str, Any]], set[str]]:
    champs = list_championships_for_organizer(organizer_id)
    out: List[Dict[str, Any]] = []
    seen_cids: set[str] = set()
    cancelled_ids: set[str] = set()

    for c in champs:
        cid  = c.get("championship_id") or c.get("id")
        name = (c.get("name") or "").strip()
        if not cid or not name:
            continue
        if cid in seen_cids:
            continue
        if not is_cs_championship(c):
            continue
        status = (c.get("status") or "").strip().lower()
        if status == "cancelled":
            cancelled_ids.add(cid)
            continue
        if not DIV_RX.search(name):
            continue

        dnum   = parse_leading_divnum(name)
        season = parse_season(name, c.get("description", ""))
        po     = is_playoffs(name)

        # NEW: Mestaruussarja fallback (if parsing did not already return 0)
        if dnum is None and MESTAR_RX.search(name):
            dnum = 0

        if season < min_season:
            continue

        item = {
            "championship_id": cid,
            "name": name,
            "season": season,
            "division_num": dnum if dnum is not None else 0,
            "slug": base_slug(dnum if dnum is not None else 0, season, po),
            "is_playoffs": 1 if po else 0,
        }
        out.append(item)
        seen_cids.add(cid)

    out.sort(key=lambda d: (-int(d.get("season", 0)), int(d.get("division_num", 0))))
    return out, cancelled_ids

def load_existing(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, list):
            return data
        raise ValueError("divisions.json must be a JSON array")


def non_destructive_merge(existing: List[Dict[str, Any]],
                          discovered: List[Dict[str, Any]],
                          cancelled_ids: set[str] | None = None,
                          prune_cancelled: bool = False) -> List[Dict[str, Any]]:
    # Build maps for existing
    by_cid: Dict[str, Dict[str, Any]] = {}
    used_slugs: set[str] = set()
    for e in existing:
        cid = e.get("championship_id")
        if prune_cancelled and cancelled_ids and cid in cancelled_ids:
            continue
        if "slug" in e and isinstance(e["slug"], str):
            used_slugs.add(e["slug"])
        if cid:
            by_cid[cid] = dict(e)

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
            # New championship → finalize unique slug
            new_row = dict(d)
            uniq_slug = make_unique_slug(proposed_slug, cid, used_slugs)
            new_row["slug"] = uniq_slug
            used_slugs.add(uniq_slug)
            by_cid[cid] = new_row

    merged = list(by_cid.values())
    merged.sort(key=lambda x: (-int(x.get("season", 0)),
                               int(x.get("division_num", 0))))
    return merged


def main(out_path: str, dry_run: bool, min_season: int, max_season: int, prune_cancelled: bool) -> None:
    out = Path(out_path)
    existing = load_existing(out)
    discovered, cancelled_ids = discover_cs_divisions(PAPPALIIGA_ORG_ID, min_season=min_season)
    
    # Filter by max_season if specified
    if max_season > 0:
        discovered = [d for d in discovered if d.get("season", 0) <= max_season]
    
    final = non_destructive_merge(existing, discovered,
                                 cancelled_ids=cancelled_ids,
                                 prune_cancelled=prune_cancelled)

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
    p.add_argument("--min-season", type=int, default=12,
                    help="Skip adding divisions older than this season (default: 12 = include all)")
    p.add_argument("--max-season", type=int, default=0,
                    help="Skip adding divisions newer than this season (default: 0 = no limit)")
    p.add_argument("--prune-cancelled", action="store_true",
                    help="Remove cancelled championships from existing divisions.json")
                    #    python gen_divisions_json.py --min-season 7 --max-season 7
    args = p.parse_args()
    main(out_path=args.out, dry_run=args.dry_run, min_season=args.min_season,
         max_season=args.max_season, prune_cancelled=args.prune_cancelled)

