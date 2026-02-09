from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
import logging

DEFAULT_LOG_MAX_AGE_DAYS = 7
DEFAULT_LOG_MAX_TOTAL_BYTES = 100 * 1024 * 1024
DEFAULT_TIMESTAMP_FORMAT = "%Y_%m_%d-%H_%M_%S"


def build_timestamped_log_path(
    log_dir: Path,
    *,
    prefix: str,
    timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT,
) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime(timestamp_format)
    return log_dir / f"{prefix}-{timestamp}.log"


def prune_log_files(
    *,
    log_dir: Path,
    file_glob: str,
    active_log_path: Path,
    max_age_days: int = DEFAULT_LOG_MAX_AGE_DAYS,
    max_total_bytes: int = DEFAULT_LOG_MAX_TOTAL_BYTES,
    logger: logging.Logger | None = None,
) -> None:
    max_age_days = max(0, int(max_age_days))
    max_total_bytes = int(max_total_bytes)
    now = time.time()
    cutoff_ts = now - max_age_days * 24 * 60 * 60
    log = logger or logging.getLogger(__name__)

    log_files = [path for path in log_dir.glob(file_glob) if path.is_file()]

    for path in log_files:
        if path == active_log_path:
            continue
        try:
            if path.stat().st_mtime < cutoff_ts:
                path.unlink()
        except Exception:
            log.warning("Failed to remove old log file %s", path)

    sized_files: list[tuple[Path, float, int]] = []
    total_size = 0
    for path in log_dir.glob(file_glob):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except Exception:
            continue
        size = max(0, int(stat.st_size))
        total_size += size
        sized_files.append((path, float(stat.st_mtime), size))

    if max_total_bytes <= 0 or total_size <= max_total_bytes:
        return

    for path, _, size in sorted(sized_files, key=lambda item: item[1]):
        if total_size <= max_total_bytes:
            break
        if path == active_log_path:
            continue
        try:
            path.unlink()
            total_size -= size
        except Exception:
            log.warning("Failed to remove oversized log file %s", path)
