from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Mapping, MutableMapping, Optional


def format_hms(seconds: float) -> str:
    """Return a compact hours/minutes/seconds representation."""
    if seconds < 1:
        return f"{seconds:.2f}s"
    seconds_int = int(seconds)
    hours = seconds_int // 3600
    minutes = (seconds_int % 3600) // 60
    secs = seconds_int % 60
    if hours:
        return f"{hours:02}:{minutes:02}:{secs:02}"
    return f"{minutes:02}:{secs:02}"


def _serialize_metrics(metrics: Mapping[str, object] | None) -> str:
    if not metrics:
        return ""
    parts = []
    for key, value in metrics.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    return " ".join(parts)


def log_stage(
    logger: logging.Logger,
    stage: str,
    elapsed: float,
    *,
    counts: Mapping[str, int] | None = None,
    extra: Mapping[str, object] | None = None,
    prefix: str | None = None,
    level: int = logging.INFO,
) -> None:
    """Emit a structured log entry for a pipeline stage."""
    metrics: dict[str, object] = {"duration": f"{elapsed:.3f}s", "human": format_hms(elapsed)}
    if counts:
        metrics.update({f"{key}_count": value for key, value in counts.items()})
    if extra:
        metrics.update(extra)
    metrics_str = _serialize_metrics(metrics)
    if prefix:
        logger.log(level, "%s stage=%s %s", prefix, stage, metrics_str)
    else:
        logger.log(level, "stage=%s %s", stage, metrics_str)


@dataclass
class StageTimer:
    """Context manager that measures elapsed time and logs automatically."""

    stage: str
    logger: logging.Logger | None = None
    level: int = logging.INFO
    prefix: Optional[str] = None
    counts: MutableMapping[str, int] | None = None

    def __post_init__(self) -> None:
        self._start = time.perf_counter()
        self._elapsed: Optional[float] = None

    def __enter__(self) -> "StageTimer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    @property
    def elapsed(self) -> float:
        if self._elapsed is not None:
            return self._elapsed
        return time.perf_counter() - self._start

    def stop(self, *, counts: Mapping[str, int] | None = None, extra: Mapping[str, object] | None = None) -> float:
        """Stop the timer and log the stage (once)."""
        if self._elapsed is None:
            self._elapsed = time.perf_counter() - self._start
            if self.logger:
                combined_counts: dict[str, int] = {}
                if self.counts:
                    combined_counts.update(self.counts)
                if counts:
                    combined_counts.update(counts)
                log_stage(
                    self.logger,
                    self.stage,
                    self._elapsed,
                    counts=combined_counts,
                    extra=extra,
                    prefix=self.prefix,
                    level=self.level,
                )
        return self._elapsed
