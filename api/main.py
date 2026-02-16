#!/usr/bin/env python3
"""FastAPI application for Pappaliiga Stats API.

Serves dynamic data for teams, players, divisions, and matches.
Replaces static HTML generation with REST API endpoints.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import asyncio
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from db_async import close_pool, get_pool

from .routers import debug, divisions, matches, players, stats, teams, seasons
from .routers import maps_catalog, image_proxy, season_view
from .routers import share_preview
from api.exceptions import BadRequestError, NotFoundError
from api.services.cache_reheat import reheat_main_page

# Track app start time for uptime calculation
import time
_app_start_time = time.time()
logger = logging.getLogger(__name__)


# Load environment variables from .env file if present
env_path = Path(__file__).parent.parent / ".env"
try:
    from dotenv import load_dotenv
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[info] Loaded environment from {env_path}")
except ImportError:
    # python-dotenv not installed, fallback to lightweight local loader
    try:
        import env_loader
        if env_path.exists():
            env_loader.load_env(env_path)
            print(f"[info] Loaded environment from {env_path} using env_loader")
    except Exception:
        # best-effort only; if this fails the environment should be provided externally
        pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup database pool."""
    # Startup: ensure pool is ready
    await get_pool()
    print("[info] Database pool initialized")

    try:
        asyncio.create_task(reheat_main_page())
    except Exception as exc:
        print(f"[warn] Cache reheat failed to start: {exc}")

    yield
    
    # Shutdown: close pool
    await close_pool()
    print("[info] Database pool closed")


app = FastAPI(
    title="Pappaliiga Stats API",
    description="REST API for CS2 tournament statistics",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS configuration for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


# Mount static files
frontend_dir = Path(__file__).parent.parent / "frontend"
if (frontend_dir / "static").exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir / "static")), name="static")
    print(f"[info] Mounted static files from {frontend_dir / 'static'}")

# Include routers
app.include_router(seasons.router, prefix="/api/seasons", tags=["seasons"])
app.include_router(divisions.router, prefix="/api/divisions", tags=["divisions"])
app.include_router(teams.router, prefix="/api/teams", tags=["teams"])
app.include_router(players.router, prefix="/api/players", tags=["players"])
app.include_router(matches.router, prefix="/api/matches", tags=["matches"])
app.include_router(stats.router, prefix="/api/stats", tags=["stats"])
app.include_router(debug.router, prefix="/api/debug", tags=["debug"])
app.include_router(maps_catalog.router, prefix="/api/maps", tags=["maps"])
app.include_router(image_proxy.router, prefix="/api", tags=["images"])
app.include_router(season_view.router, prefix="/api", tags=["season-view"])


@app.get("/")
async def root(request: Request):
    """Serve the frontend index.html."""
    frontend_dir = Path(__file__).parent.parent / "frontend"
    index_path = frontend_dir / "index.html"

    if share_preview.is_preview_crawler_request(request):
        return await share_preview.build_preview_for_spa_path(request, "")

    if index_path.exists():
        return FileResponse(str(index_path))
    else:
        return {
            "message": "Pappaliiga Stats API",
            "version": "1.0.0",
            "docs": "/docs",
            "frontend": "not found - expected at frontend/index.html"
        }


async def _health_check_payload():
    """Health check endpoint for monitoring."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
        
        uptime = int(time.time() - _app_start_time)
        
        return {
            "ok": True,
            "version": "v3.1",
            "uptime": uptime,
            "database": "connected"
        }
    except Exception:
        logger.exception("Health check failed")
        raise HTTPException(status_code=503, detail="Database unhealthy")


@app.get("/api/health")
async def api_health():
    return await _health_check_payload()


# SPA fallback - must be last route!
# This catches all routes not matched by API or static files
# and returns index.html for Vue Router to handle
@app.get("/{full_path:path}")
async def spa_fallback(full_path: str, request: Request):
    """Serve index.html for all routes (SPA fallback for Vue Router)."""
    # Don't intercept API routes or static files
    if full_path.startswith("api/") or full_path.startswith("static/"):
        raise HTTPException(status_code=404, detail="Not found")

    if share_preview.is_preview_crawler_request(request):
        return await share_preview.build_preview_for_spa_path(request, full_path)

    frontend_dir = Path(__file__).parent.parent / "frontend"
    index_path = frontend_dir / "index.html"

    if index_path.exists():
        return FileResponse(str(index_path))
    else:
        raise HTTPException(status_code=404, detail="Frontend not found")


# Global exception handlers
@app.exception_handler(NotFoundError)
async def not_found_handler(request, exc: NotFoundError):
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(BadRequestError)
async def bad_request_handler(request, exc: BadRequestError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Catch-all exception handler."""
    logger.exception("Unhandled exception while serving %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )

