"""Image proxy router to avoid OpaqueResponseBlocking for external avatars.

Provides an endpoint `/api/proxy-image?url=...` which fetches images server-side
from a small whitelist of hosts, caches them in memory for a short TTL, and
returns them with safe cache headers. Protects against SSRF by validating
the host and enforcing size/time limits.
"""
from __future__ import annotations

import asyncio
import logging
import time
import mimetypes
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

logger = logging.getLogger(__name__)

# Simple in-memory cache structure: url -> (expiry_ts, content_type, bytes, source)
_CACHE: dict[str, tuple[float, str, bytes, str]] = {}
_CACHE_LOCK = asyncio.Lock()

# Configuration
WHITELISTED_HOSTS = {"distribution.faceit-cdn.net", "assets.faceit-cdn.net"}
MAX_BYTES = 2 * 1024 * 1024  # 2 MB
FETCH_TIMEOUT = 10  # seconds
CACHE_TTL = 60 * 10  # 10 minutes
STATIC_DIR = Path(__file__).resolve().parents[2] / "frontend" / "static"
DEFAULT_FALLBACK_PATH = STATIC_DIR / "pappaliiga-logo-white-bg.png"
_DEFAULT_FALLBACK_CACHE: Optional[tuple[str, bytes]] = None

router = APIRouter()


def _is_allowed_host(host: str) -> bool:
    host = host.lower()
    if host in WHITELISTED_HOSTS:
        return True
    # Allow additional Faceit CDN subdomains without enumerating every variant explicitly
    return host.endswith(".faceit-cdn.net") or host == "faceit-cdn.net"


def _load_default_fallback() -> Optional[tuple[str, bytes]]:
    """Load the bundled fallback asset once and keep it in memory."""
    global _DEFAULT_FALLBACK_CACHE
    if _DEFAULT_FALLBACK_CACHE is not None:
        return _DEFAULT_FALLBACK_CACHE

    try:
        data = DEFAULT_FALLBACK_PATH.read_bytes()
    except FileNotFoundError:
        logger.warning("proxy-image: default fallback asset missing at %s", DEFAULT_FALLBACK_PATH)
        _DEFAULT_FALLBACK_CACHE = None
        return None
    except OSError as exc:
        logger.warning("proxy-image: error reading fallback asset at %s: %s", DEFAULT_FALLBACK_PATH, exc)
        _DEFAULT_FALLBACK_CACHE = None
        return None

    content_type = mimetypes.guess_type(DEFAULT_FALLBACK_PATH.name)[0] or "image/png"
    _DEFAULT_FALLBACK_CACHE = (content_type, data)
    return _DEFAULT_FALLBACK_CACHE


async def _store_and_respond(cache_key: str, content_type: str, data: bytes, source: str) -> Response:
    expiry = time.time() + CACHE_TTL
    async with _CACHE_LOCK:
        _CACHE[cache_key] = (expiry, content_type, data, source)
    return Response(
        content=data,
        media_type=content_type,
        headers={
            "Cache-Control": f"public, max-age={CACHE_TTL}",
            "X-Proxy-Image-Source": source,
        },
    )


async def _serve_remote_fallback(
    client: httpx.AsyncClient,
    cache_key: str,
    fallback: str,
) -> Optional[Response]:
    try:
        fb_decoded = unquote(fallback)
    except Exception:
        fb_decoded = fallback

    fb_parsed = urlparse(fb_decoded)
    fb_host = (fb_parsed.hostname or "").lower()
    if not fb_host:
        return None
    # Allow same whitelist as main request plus known first-party host
    if not (_is_allowed_host(fb_host) or fb_host.endswith("pappaliiga.fi")):
        return None

    try:
        fb_resp = await client.get(fb_decoded, headers={"User-Agent": "Mozilla/5.0"})
    except Exception as exc:
        logger.info("proxy-image: fallback fetch error for %s: %s", fb_decoded, exc)
        return None

    logger.debug("proxy-image: fallback fetch status=%s for %s", fb_resp.status_code, fb_decoded)
    if fb_resp.status_code != 200:
        return None

    content_type = fb_resp.headers.get("Content-Type", "application/octet-stream")
    data = fb_resp.content
    if len(data) > MAX_BYTES:
        return None

    logger.debug("proxy-image: served remote fallback for %s", cache_key)
    return await _store_and_respond(cache_key, content_type, data, "fallback_remote")


async def _serve_default_fallback(cache_key: str) -> Optional[Response]:
    fallback = _load_default_fallback()
    if not fallback:
        return None

    content_type, data = fallback
    logger.debug("proxy-image: served local fallback for %s", cache_key)
    return await _store_and_respond(cache_key, content_type, data, "fallback_local")


@router.get("/proxy-image")
async def proxy_image(url: str = Query(..., description="Remote image URL to proxy"), fallback: Optional[str] = Query(None, description="Optional fallback image URL to use if upstream fails")):
    """Fetch an image from a whitelisted host and return it.

    Example: /api/proxy-image?url=https://distribution.faceit-cdn.net/images/...
    """

    # Basic validation
    # Accept percent-encoded values (e.g. when url is passed through a querystring)
    try:
        decoded = unquote(url)
    except Exception:
        decoded = url

    parsed = urlparse(decoded)
    if not parsed.scheme or parsed.scheme.lower() not in ("http", "https"):
        logger.debug("proxy-image: rejected scheme for url=%s parsed=%s", url, parsed)
        raise HTTPException(status_code=400, detail="Only http/https URLs allowed")

    host = (parsed.hostname or "").lower()
    if not host:
        logger.debug("proxy-image: missing host for url=%s parsed=%s", url, parsed)
        raise HTTPException(status_code=400, detail="Invalid URL: missing host")

    if not _is_allowed_host(host):
        logger.debug("proxy-image: host not allowed host=%s url=%s", host, url)
        raise HTTPException(status_code=403, detail="Host not allowed")

    # Check cache
    async with _CACHE_LOCK:
        entry = _CACHE.get(url)
        if entry and entry[0] >= time.time():
            _, content_type, data, source = entry
            cache_source = f"cache:{source}"
            return Response(
                content=data,
                media_type=content_type,
                headers={
                    "Cache-Control": f"public, max-age={CACHE_TTL}",
                    "X-Proxy-Image-Source": cache_source,
                },
            )

    # Fetch from remote
    async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True) as client:
        try:
            # Use the decoded URL when requesting. Provide minimal browser-like headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept": "image/*,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            resp = await client.get(decoded, headers=headers)
        except httpx.HTTPError as e:
            logger.warning("proxy-image: httpx error fetching url=%s: %s", decoded, e)
            raise HTTPException(status_code=502, detail=f"Error fetching image: {e}")

        if resp.status_code != 200:
            # Log upstream response preview for debugging (don't leak large bodies into logs)
            body_preview = resp.text[:1000] if resp.text else ""
            # Treat 400 from CDN as not-found (common when image id doesn't exist) and prefer fallback
            if resp.status_code == 400:
                logger.debug("proxy-image: upstream 400 treated as not-found for url=%s", decoded)
            else:
                logger.info("proxy-image: upstream returned non-200 url=%s status=%s preview=%s", decoded, resp.status_code, body_preview)

            # If a fallback URL is provided, attempt to fetch and serve it instead
            if fallback:
                fb_response = await _serve_remote_fallback(client, url, fallback)
                if fb_response:
                    return fb_response

            default_response = await _serve_default_fallback(url)
            if default_response:
                return default_response
            # Map 400 -> 404 to indicate not-found; otherwise 502 for upstream errors
            if resp.status_code == 400:
                raise HTTPException(status_code=404, detail="Image not found")
            raise HTTPException(status_code=502, detail=f"Upstream returned {resp.status_code}")

        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        data = resp.content

        if len(data) > MAX_BYTES:
            raise HTTPException(status_code=413, detail="Image too large")

        return await _store_and_respond(url, content_type, data, "faceit")
