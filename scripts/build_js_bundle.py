#!/usr/bin/env python3
"""Build JS bundle for production deployment.

Reads the BUNDLE_VENDOR_START/END and BUNDLE_APP_START/END marker sections in
frontend/index.html, collects all referenced /static/*.js files in document
order, concatenates them, and writes:

  frontend/static/dist/app.bundle.js   — concatenated JS
  frontend/index.prod.html             — index.html with single bundle <script> tag

Run directly:
    python scripts/build_js_bundle.py

Or let the pre-commit hook run it automatically on JS file changes.
"""
from __future__ import annotations

import hashlib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEX_HTML = ROOT / "frontend" / "index.html"
STATIC_DIR = ROOT / "frontend" / "static"
DIST_DIR = STATIC_DIR / "dist"
BUNDLE_PATH = DIST_DIR / "app.bundle.js"
PROD_INDEX_PATH = ROOT / "frontend" / "index.prod.html"

_SCRIPT_SRC_RE = re.compile(
    r'<script\s+src="(/static/[^"?]+\.js)(?:\?[^"]*)?"'
)
_MARKER_RE = re.compile(
    r"<!-- BUNDLE_VENDOR_START -->.*?<!-- BUNDLE_VENDOR_END -->|"
    r"<!-- BUNDLE_APP_START -->.*?<!-- BUNDLE_APP_END -->",
    re.DOTALL,
)


def _extract_js_paths(section: str) -> list[str]:
    """Return all /static/*.js src paths found in a marker section."""
    return _SCRIPT_SRC_RE.findall(section)


def build() -> int:
    if not INDEX_HTML.exists():
        print(f"[error] Not found: {INDEX_HTML}", file=sys.stderr)
        return 1

    html = INDEX_HTML.read_text(encoding="utf-8")

    # Collect /static/*.js paths from both marker sections in document order
    js_paths: list[str] = []
    for match in _MARKER_RE.finditer(html):
        js_paths.extend(_extract_js_paths(match.group()))

    if not js_paths:
        print("[error] No /static/*.js paths found in marker sections", file=sys.stderr)
        return 1

    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # Concatenate JS files
    parts: list[str] = []
    missing: list[str] = []
    for src_path in js_paths:
        file_path = ROOT / "frontend" / src_path.lstrip("/")
        if not file_path.exists():
            missing.append(src_path)
            print(f"[warn] Missing: {src_path}", file=sys.stderr)
            continue
        content = file_path.read_text(encoding="utf-8")
        parts.append(f"/* === {src_path} === */")
        parts.append(content)
        parts.append("")

    bundle_content = "\n".join(parts)
    content_hash = hashlib.md5(bundle_content.encode()).hexdigest()[:8]

    bundle_changed = True
    if BUNDLE_PATH.exists() and BUNDLE_PATH.read_text(encoding="utf-8") == bundle_content:
        print(
            f"[info] Bundle unchanged ({len(bundle_content):,} bytes, "
            f"{len(js_paths)} files, hash={content_hash})"
        )
        bundle_changed = False
    else:
        BUNDLE_PATH.write_text(bundle_content, encoding="utf-8")
        print(
            f"[info] Bundle written: {BUNDLE_PATH.relative_to(ROOT)} "
            f"({len(bundle_content):,} bytes, {len(js_paths)} files, hash={content_hash})"
        )

    # Build index.prod.html:
    # Remove both marker sections, keep everything else.
    # Then insert bundle <script> tag before </body>.
    bundle_tag = f'    <script src="/static/dist/app.bundle.js?v={content_hash}"></script>'
    prod_html = _MARKER_RE.sub("", html)
    # Clean up excess blank lines left by removal
    prod_html = re.sub(r"\n{3,}", "\n\n", prod_html)
    # Insert bundle tag before </body>
    if bundle_tag not in prod_html:
        prod_html = prod_html.replace("</body>", f"{bundle_tag}\n</body>")

    prod_changed = True
    if PROD_INDEX_PATH.exists() and PROD_INDEX_PATH.read_text(encoding="utf-8") == prod_html:
        print("[info] index.prod.html unchanged")
        prod_changed = False
    else:
        PROD_INDEX_PATH.write_text(prod_html, encoding="utf-8")
        print(f"[info] Wrote: {PROD_INDEX_PATH.relative_to(ROOT)}")

    # Stage both output files so the commit includes them
    files_to_stage = []
    if bundle_changed:
        files_to_stage.append(str(BUNDLE_PATH))
    if prod_changed:
        files_to_stage.append(str(PROD_INDEX_PATH))

    if files_to_stage:
        try:
            subprocess.run(
                ["git", "add", "--"] + files_to_stage,
                check=True,
                cwd=str(ROOT),
            )
            print(f"[info] Staged: {', '.join(Path(f).name for f in files_to_stage)}")
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            print(f"[warn] git add failed: {exc}", file=sys.stderr)

    if missing:
        print(f"[warn] {len(missing)} file(s) were missing and skipped", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(build())
