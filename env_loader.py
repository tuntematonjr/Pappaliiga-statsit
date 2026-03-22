"""Utility helpers for loading a simple .env file without external deps."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Tuple


def _parse_env_lines(text: str) -> Iterable[Tuple[str, str]]:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        yield key.strip(), value.strip().strip('"').strip("'")


def load_env(path: str | Path | None = None, *, override: bool = False) -> None:
    """Load key/value pairs from a .env file into ``os.environ``.

    Args:
        path: Optional path to a .env file or its containing directory. When omitted,
            defaults to ``repo_root/.env`` (alongside this module).
        override: When True, overwrite existing environment variables. Mimics the
            behaviour of ``dotenv`` with ``override=True``. Defaults to ``False``
            which leaves existing values untouched.
    """

    env_path = Path(path) if path is not None else Path(__file__).with_name(".env")
    if env_path.is_dir():
        env_path = env_path / ".env"

    try:
        content = env_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return

    for key, value in _parse_env_lines(content):
        if override:
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)
