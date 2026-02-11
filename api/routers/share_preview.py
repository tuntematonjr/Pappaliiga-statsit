"""Crawler-friendly link previews for SPA routes."""
from __future__ import annotations

from html import escape
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse, HTMLResponse

from api.exceptions import NotFoundError
from api.services import divisions_service, players_service
from api.services import teams_service

router = APIRouter()
logger = logging.getLogger(__name__)

frontend_dir = Path(__file__).parent.parent.parent / "frontend"
index_path = frontend_dir / "index.html"

BOT_UA_MARKERS = (
    "discordbot",
    "twitterbot",
    "slackbot",
    "facebookexternalhit",
    "linkedinbot",
    "whatsapp",
    "telegrambot",
    "skypeuripreview",
    "googlebot",
    "bingbot",
    "crawler",
    "spider",
)

DEFAULT_IMAGE = "https://pappaliiga.fi/app/themes/pappaliiga/images/src/pappaliiga-logo-white-bg.png"
UNOFFICIAL_NOTE = "Epävirallinen Pappaliigan CS stasti sivu Armafinlandin toimesta."
DEFAULT_PUBLIC_BASES = (
    "https://pappa.aukko.net",
    "https://papan.xn--per-sla.aukko.net",
)


def _is_preview_crawler(user_agent: str) -> bool:
    ua = (user_agent or "").lower()
    return any(marker in ua for marker in BOT_UA_MARKERS)


def is_preview_crawler_request(request: Request) -> bool:
    return _is_preview_crawler(request.headers.get("user-agent", ""))


def _canonical_host(host: str) -> str:
    value = (host or "").strip().rstrip(".").lower()
    if not value:
        return ""
    try:
        return value.encode("idna").decode("ascii")
    except UnicodeError:
        return value


def _configured_public_bases() -> list[str]:
    """Return validated public bases from env or secure defaults."""
    values: list[str] = []
    raw_many = (os.getenv("PUBLIC_BASE_URLS") or "").strip()
    raw_single = (os.getenv("PUBLIC_BASE_URL") or "").strip()

    if raw_many:
        values.extend(part.strip() for part in raw_many.split(","))
    elif raw_single:
        values.append(raw_single)
    else:
        values.extend(DEFAULT_PUBLIC_BASES)

    bases: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not raw:
            continue
        parsed = urlparse(raw)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            logger.warning("Ignoring invalid public base URL value: %r", raw)
            continue
        base = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        if base in seen:
            continue
        seen.add(base)
        bases.append(base)
    return bases


def _request_base(request: Request) -> str:
    bases = _configured_public_bases()
    if not bases:
        # Last-resort safe fallback if all env values are invalid.
        return DEFAULT_PUBLIC_BASES[0]

    request_host = _canonical_host(request.url.hostname or "")
    for base in bases:
        parsed = urlparse(base)
        base_host = _canonical_host(parsed.hostname or "")
        if request_host and request_host == base_host:
            return base
    return bases[0]


def _absolute_url(request: Request, maybe_relative: str) -> str:
    if not maybe_relative:
        return ""
    if maybe_relative.startswith(("http://", "https://")):
        return maybe_relative
    base = _request_base(request)
    return urljoin(f"{base}/", maybe_relative.lstrip("/"))


def _build_team_description(stats: Optional[dict]) -> str:
    if not stats:
        return f"Joukkueen tilastot, kartat, pelaajat ja otteluhistoria. {UNOFFICIAL_NOTE}"

    season = stats.get("season")
    division_num = stats.get("division_num")
    matches_played = int(stats.get("matches_played") or 0)
    wins = int(stats.get("wins") or 0)
    losses = int(stats.get("losses") or 0)
    win_rate_raw = float(stats.get("win_rate") or 0.0)
    win_rate = win_rate_raw * 100.0 if win_rate_raw <= 1.0 else win_rate_raw
    return (
        f"Kausi {season}, divisioona {division_num}. "
        f"Ottelut {matches_played}, voitot {wins}, tappiot {losses}, voittoprosentti {win_rate:.1f}%. "
        f"{UNOFFICIAL_NOTE}"
    )


def _canonical_for_path(request: Request, full_path: str) -> str:
    base = _request_base(request)
    if not full_path:
        return f"{base}/"
    return f"{base}/{full_path.lstrip('/')}"


def _fallback_preview_meta(full_path: str) -> tuple[str, str]:
    normalized = (full_path or "").strip("/")
    if not normalized:
        return (
            "Pappaliiga Stats - Etusivu",
            f"Pappaliiga CS2 tilastot: joukkueet, pelaajat, divisioonat ja ottelut. {UNOFFICIAL_NOTE}",
        )
    if normalized == "seasons":
        return (
            "Pappaliiga Stats - Kaudet",
            f"Selaa kausia, divisioonia ja niiden etenemistä. {UNOFFICIAL_NOTE}",
        )
    if normalized == "season/current/upcoming":
        return (
            "Pappaliiga Stats - Tulevat Ottelut",
            f"Katso ajankohtaiset tulevat ottelut ja aikataulut. {UNOFFICIAL_NOTE}",
        )
    return (
        "Pappaliiga Stats",
        f"Pappaliiga CS2 tilastot: joukkueet, pelaajat, divisioonat ja ottelut. {UNOFFICIAL_NOTE}",
    )


async def build_preview_for_spa_path(request: Request, full_path: str) -> HTMLResponse:
    normalized = (full_path or "").strip("/")
    parts = [p for p in normalized.split("/") if p]

    title = "Pappaliiga Stats"
    description = f"Pappaliiga CS2 tilastot: joukkueet, pelaajat, divisioonat ja ottelut. {UNOFFICIAL_NOTE}"
    image_url = _absolute_url(request, DEFAULT_IMAGE)

    if len(parts) >= 2 and parts[0] == "team":
        team_id = parts[-1]
        championship_id = parts[1] if len(parts) >= 3 else request.query_params.get("championship")
        try:
            team = await teams_service.fetch_team(team_id)
            team_name = team.get("display_name") or team.get("team_name") or f"Team {team_id}"
            title = f"{team_name} - Pappaliiga Stats"
            image_url = _absolute_url(request, team.get("avatar") or DEFAULT_IMAGE)

            try:
                seasons = await teams_service.fetch_team_season_stats(team_id)
            except NotFoundError:
                seasons = []

            selected_stats = None
            if seasons:
                if championship_id:
                    selected_stats = next(
                        (row for row in seasons if str(row.get("championship_id")) == str(championship_id)),
                        None,
                    )
                if selected_stats is None:
                    selected_stats = seasons[0]

            description = _build_team_description(selected_stats)
        except NotFoundError:
            title = f"Team {team_id} - Pappaliiga Stats"
            description = f"Joukkueen tilastosivu. {UNOFFICIAL_NOTE}"
    elif len(parts) >= 2 and parts[0] == "division":
        championship_id = parts[1]
        try:
            champ = await divisions_service.fetch_division_by_id(championship_id)
            division_name = champ.get("name") or f"Divisioona {champ.get('division_num')}"
            season = champ.get("season")
            title = f"{division_name} - Pappaliiga Stats"
            description = (
                f"Kausi {season}, divisioona {champ.get('division_num')} tilastot ja sarjataulukko. "
                f"{UNOFFICIAL_NOTE}"
            )
        except NotFoundError:
            title = f"Division {championship_id} - Pappaliiga Stats"
            description = f"Divisioonan tilastosivu. {UNOFFICIAL_NOTE}"
    elif len(parts) >= 2 and parts[0] == "player":
        player_id = parts[1]
        try:
            player = await players_service.fetch_player(player_id)
            nickname = player.get("nickname") or player_id
            title = f"{nickname} - Pappaliiga Stats"
            description = f"Pelaajan tilastot, karttadata ja kausihistoria. {UNOFFICIAL_NOTE}"
            image_url = _absolute_url(request, player.get("avatar") or DEFAULT_IMAGE)
        except NotFoundError:
            title = f"Player {player_id} - Pappaliiga Stats"
            description = f"Pelaajan tilastosivu. {UNOFFICIAL_NOTE}"
    else:
        title, description = _fallback_preview_meta(normalized)

    canonical_url = _canonical_for_path(request, normalized)
    html = _build_preview_html(
        title=title,
        description=description,
        image_url=image_url,
        canonical_url=canonical_url,
    )
    return HTMLResponse(content=html)


def _build_preview_html(*, title: str, description: str, image_url: str, canonical_url: str) -> str:
    safe_title = escape(title, quote=True)
    safe_description = escape(description, quote=True)
    safe_image = escape(image_url, quote=True)
    safe_url = escape(canonical_url, quote=True)
    return f"""<!doctype html>
<html lang="fi">
<head>
  <meta charset="utf-8">
  <title>{safe_title}</title>
  <meta name="description" content="{safe_description}">
  <link rel="canonical" href="{safe_url}">
  <meta property="og:type" content="website">
  <meta property="og:site_name" content="Pappaliiga Stats (Armafinland, unofficial)">
  <meta property="og:locale" content="fi_FI">
  <meta property="og:title" content="{safe_title}">
  <meta property="og:description" content="{safe_description}">
  <meta property="og:image" content="{safe_image}">
  <meta property="og:url" content="{safe_url}">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{safe_title}">
  <meta name="twitter:description" content="{safe_description}">
  <meta name="twitter:image" content="{safe_image}">
</head>
<body>
  <p>{safe_title}</p>
</body>
</html>
"""


async def _team_preview_response(
    request: Request,
    *,
    team_id: str,
    championship_id: Optional[str] = None,
):
    if not is_preview_crawler_request(request):
        if index_path.exists():
            return FileResponse(str(index_path))
        return HTMLResponse("Frontend not found", status_code=404)

    path = f"team/{team_id}" if not championship_id else f"team/{championship_id}/{team_id}"
    return await build_preview_for_spa_path(request, path)


@router.get("/team/{team_id}", include_in_schema=False)
async def team_preview(team_id: str, request: Request, championship: Optional[str] = Query(default=None)):
    """Serve OG tags for crawlers and SPA shell for browsers."""
    return await _team_preview_response(request, team_id=team_id, championship_id=championship)


@router.get("/team/{championship_id}/{team_id}", include_in_schema=False)
async def team_preview_with_championship(championship_id: str, team_id: str, request: Request):
    """Serve OG tags for crawler requests to championship-scoped team URLs."""
    return await _team_preview_response(request, team_id=team_id, championship_id=championship_id)
