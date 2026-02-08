"""Expose all API router modules for convenience imports."""

from . import (
    championships,
    divisions,
    faceit_webhooks,
    image_proxy,
    maps_catalog,
    matches,
    players,
    share_preview,
    season_view,
    sync_events,
    stats,
    teams,
)

__all__ = [
    "championships",
    "divisions",
    "faceit_webhooks",
    "matches",
    "players",
    "share_preview",
    "season_view",
    "sync_events",
    "stats",
    "teams",
    "maps_catalog",
    "image_proxy",
]
