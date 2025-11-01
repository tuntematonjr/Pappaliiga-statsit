"""Expose all API router modules for convenience imports."""

from . import (
    championships,
    divisions,
    image_proxy,
    maps_catalog,
    matches,
    players,
    stats,
    teams,
)

__all__ = [
    "championships",
    "divisions",
    "matches",
    "players",
    "stats",
    "teams",
    "maps_catalog",
    "image_proxy",
]
