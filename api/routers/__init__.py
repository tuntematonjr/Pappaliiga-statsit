"""Expose all API router modules for convenience imports."""

from . import (
    championships,
    divisions,
    image_proxy,
    maps_catalog,
    matches,
    players,
    season_view,
    stats,
    teams,
)

__all__ = [
    "championships",
    "divisions",
    "matches",
    "players",
    "season_view",
    "stats",
    "teams",
    "maps_catalog",
    "image_proxy",
]
