#!/usr/bin/env python3
"""Lightweight livereload server for the frontend using python-livereload.

Run this instead of `frontend/spa_server.py` to get automatic browser reloads
when files under `frontend/static/` change.
"""
from __future__ import annotations

import sys
from pathlib import Path

from livereload import Server


def main(port: int = 8001):
    root = Path(__file__).resolve().parents[1] / "frontend"
    static_dir = root / "static"
    server = Server()
    # watch static files and templates
    server.watch(str(static_dir))
    # serve frontend directory (index.html) with history fallback handled client-side
    server.serve(root=str(root), host="0.0.0.0", port=port)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    main(port)
