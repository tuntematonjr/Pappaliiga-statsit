"""Expose all API router modules for convenience imports."""

from . import (
    championships,
    divisions,
    image_proxy,
    maps_catalog,
    matches,
    players,
    share_preview,
    season_view,
    stats,
    teams,
)

__all__ = [
    "championships",
    "divisions",
    "matches",
    "players",
    "share_preview",
    "season_view",
    "stats",
    "teams",
    "maps_catalog",
    "image_proxy",
]
