"""Image proxy router to avoid OpaqueResponseBlocking for external avatars.

Provides an endpoint `/api/proxy-image?url=...` which fetches images server-side
from a small whitelist of hosts, caches them in memory for a short TTL, and
returns them with safe cache headers. Protects against SSRF by validating
the host and enforcing size/time limits.
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
import ipaddress
import logging
import mimetypes
import os
import socket
import time
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urljoin, urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

logger = logging.getLogger(__name__)

# Simple in-memory cache structure: url -> (expiry_ts, content_type, bytes, source)
_CACHE: "OrderedDict[str, tuple[float, str, bytes, str]]" = OrderedDict()
_CACHE_LOCK = asyncio.Lock()

# Configuration
WHITELISTED_HOSTS = {"distribution.faceit-cdn.net", "assets.faceit-cdn.net"}
MAX_BYTES = 2 * 1024 * 1024  # 2 MB
FETCH_TIMEOUT = 10  # seconds
CACHE_TTL = 60 * 10  # 10 minutes
MAX_CACHE_ENTRIES = max(32, int(os.getenv("PROXY_IMAGE_MAX_CACHE_ENTRIES", "512")))
MAX_CACHE_KEY_LENGTH = max(256, int(os.getenv("PROXY_IMAGE_MAX_CACHE_KEY_LENGTH", "2048")))
MAX_REDIRECTS = 3
STATIC_DIR = Path(__file__).resolve().parents[2] / "frontend" / "static"
DEFAULT_FALLBACK_PATH = STATIC_DIR / "pappaliiga-logo-white-bg.png"
_DEFAULT_FALLBACK_CACHE: Optional[tuple[str, bytes]] = None
DEFAULT_PUBLIC_BASES = (
    "https://pappa.aukko.net",
    "https://papan.xn--per-sla.aukko.net",
)

router = APIRouter()


def _is_allowed_host(host: str) -> bool:
    host = host.lower()
    if host in WHITELISTED_HOSTS:
        return True
    # Allow additional Faceit CDN subdomains without enumerating every variant explicitly
    return host.endswith(".faceit-cdn.net") or host == "faceit-cdn.net"


def _canonical_host(host: str) -> str:
    value = (host or "").strip().rstrip(".").lower()
    if not value:
        return ""
    try:
        return value.encode("idna").decode("ascii")
    except UnicodeError:
        return value


def _configured_public_hosts() -> set[str]:
    values: list[str] = []
    raw_many = (os.getenv("PUBLIC_BASE_URLS") or "").strip()
    raw_single = (os.getenv("PUBLIC_BASE_URL") or "").strip()

    if raw_many:
        values.extend(part.strip() for part in raw_many.split(","))
    elif raw_single:
        values.append(raw_single)
    else:
        values.extend(DEFAULT_PUBLIC_BASES)

    hosts: set[str] = set()
    for raw in values:
        if not raw:
            continue
        parsed = urlparse(raw)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            continue
        host = _canonical_host(parsed.hostname or "")
        if host:
            hosts.add(host)
    return hosts


def _is_first_party_host(host: str) -> bool:
    host_norm = _canonical_host(host)
    if not host_norm:
        return False
    for public_host in _configured_public_hosts():
        if host_norm == public_host or host_norm.endswith(f".{public_host}"):
            return True
    return False


def _is_public_ip(ip_text: str) -> bool:
    ip = ipaddress.ip_address(ip_text)
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _host_has_only_public_ips(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    except OSError:
        return False

    saw_ip = False
    for info in infos:
        address = info[4][0]
        saw_ip = True
        try:
            if not _is_public_ip(address):
                return False
        except ValueError:
            return False
    return saw_ip


async def _ensure_public_host(host: str) -> None:
    host_is_public = await asyncio.to_thread(_host_has_only_public_ips, host)
    if not host_is_public:
        logger.warning("proxy-image: host resolved to disallowed address host=%s", host)
        raise HTTPException(status_code=403, detail="Host not allowed")


def _parse_and_validate_url(raw_url: str, *, allow_first_party: bool = False) -> tuple[str, str]:
    try:
        decoded = unquote(raw_url)
    except Exception:
        decoded = raw_url

    parsed = urlparse(decoded)
    if not parsed.scheme or parsed.scheme.lower() not in ("http", "https"):
        raise HTTPException(status_code=400, detail="Only http/https URLs allowed")

    host = (parsed.hostname or "").lower()
    if not host:
        raise HTTPException(status_code=400, detail="Invalid URL: missing host")

    if _is_allowed_host(host) or (allow_first_party and _is_first_party_host(host)):
        return decoded, host

    raise HTTPException(status_code=403, detail="Host not allowed")


def _response_headers(source: str) -> dict[str, str]:
    return {
        "Cache-Control": f"public, max-age={CACHE_TTL}",
        "X-Content-Type-Options": "nosniff",
        "X-Proxy-Image-Source": source,
    }


def _purge_expired_locked(now: float) -> None:
    expired_keys = [key for key, entry in _CACHE.items() if entry[0] < now]
    for key in expired_keys:
        _CACHE.pop(key, None)


def _set_cache_entry_locked(key: str, entry: tuple[float, str, bytes, str]) -> None:
    if key in _CACHE:
        _CACHE.pop(key, None)
    _CACHE[key] = entry
    _CACHE.move_to_end(key)
    while len(_CACHE) > MAX_CACHE_ENTRIES:
        _CACHE.popitem(last=False)


def _is_image_content_type(content_type: str) -> bool:
    return content_type.lower().split(";", 1)[0].strip().startswith("image/")


async def _fetch_with_validated_redirects(
    client: httpx.AsyncClient,
    initial_url: str,
    *,
    allow_first_party: bool = False,
    headers: Optional[dict[str, str]] = None,
) -> tuple[httpx.Response, str]:
    current_url = initial_url
    for hop in range(MAX_REDIRECTS + 1):
        decoded, host = _parse_and_validate_url(current_url, allow_first_party=allow_first_party)
        await _ensure_public_host(host)
        try:
            response = await client.get(decoded, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("proxy-image: upstream fetch error url=%s err=%s", decoded, exc)
            raise HTTPException(status_code=502, detail="Error fetching image") from exc

        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location")
            if not location:
                raise HTTPException(status_code=502, detail="Invalid redirect from upstream")
            if hop >= MAX_REDIRECTS:
                raise HTTPException(status_code=502, detail="Too many redirects")
            current_url = urljoin(decoded, location)
            continue

        return response, decoded

    raise HTTPException(status_code=502, detail="Too many redirects")


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
        _purge_expired_locked(time.time())
        _set_cache_entry_locked(cache_key, (expiry, content_type, data, source))
    return Response(
        content=data,
        media_type=content_type,
        headers=_response_headers(source),
    )


async def _serve_remote_fallback(
    client: httpx.AsyncClient,
    cache_key: str,
    fallback: str,
) -> Optional[Response]:
    try:
        fb_resp, fb_final_url = await _fetch_with_validated_redirects(
            client,
            fallback,
            allow_first_party=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except HTTPException:
        return None

    logger.debug("proxy-image: fallback fetch status=%s for %s", fb_resp.status_code, fb_final_url)
    if fb_resp.status_code != 200:
        return None

    content_type = fb_resp.headers.get("Content-Type", "application/octet-stream")
    if not _is_image_content_type(content_type):
        return None

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

    decoded, host = _parse_and_validate_url(url)
    if len(decoded) > MAX_CACHE_KEY_LENGTH:
        raise HTTPException(status_code=400, detail="URL too long")
    await _ensure_public_host(host)
    cache_key = decoded

    # Check cache
    async with _CACHE_LOCK:
        now = time.time()
        _purge_expired_locked(now)
        entry = _CACHE.get(cache_key)
        if entry and entry[0] >= now:
            _CACHE.move_to_end(cache_key)
            _, content_type, data, source = entry
            cache_source = f"cache:{source}"
            return Response(
                content=data,
                media_type=content_type,
                headers=_response_headers(cache_source),
            )

    # Fetch from remote
    async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=False) as client:
        # Use the decoded URL when requesting. Provide minimal browser-like headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept": "image/*,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        resp, final_url = await _fetch_with_validated_redirects(client, decoded, headers=headers)

        if resp.status_code != 200:
            # Log upstream response preview for debugging (don't leak large bodies into logs)
            body_preview = resp.text[:1000] if resp.text else ""
            # Treat 400 from CDN as not-found (common when image id doesn't exist) and prefer fallback
            if resp.status_code == 400:
                logger.debug("proxy-image: upstream 400 treated as not-found for url=%s", final_url)
            else:
                logger.info("proxy-image: upstream returned non-200 url=%s status=%s preview=%s", final_url, resp.status_code, body_preview)

            # If a fallback URL is provided, attempt to fetch and serve it instead
            if fallback:
                fb_response = await _serve_remote_fallback(client, cache_key, fallback)
                if fb_response:
                    return fb_response

            default_response = await _serve_default_fallback(cache_key)
            if default_response:
                return default_response
            # Map 400 -> 404 to indicate not-found; otherwise 502 for upstream errors
            if resp.status_code == 400:
                raise HTTPException(status_code=404, detail="Image not found")
            raise HTTPException(status_code=502, detail=f"Upstream returned {resp.status_code}")

        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        if not _is_image_content_type(content_type):
            raise HTTPException(status_code=502, detail="Upstream did not return an image")

        data = resp.content

        if len(data) > MAX_BYTES:
            raise HTTPException(status_code=413, detail="Image too large")

        return await _store_and_respond(cache_key, content_type, data, "faceit")
