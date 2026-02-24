"""Standalone FACEIT webhook listener.

This is a convenience runner for testing/debugging webhook ingestion outside the main API.
Imports webhook logic from api.routers.webhook to avoid duplication.

In production, use api.main:app which includes webhook routes.

Usage:
    python faceit_webhook_listener.py           # port 8010 (default)
    python -c "import uvicorn; uvicorn.run('faceit_webhook_listener:app', port=8010)"
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path

from fastapi import FastAPI
from api.services.sync_event_queue import get_sync_event_queue

logger = logging.getLogger("faceit.webhook.listener")
logging.basicConfig(level=os.getenv("WEBHOOK_LOG_LEVEL", "INFO"))

# Load environment
ENV_PATH = Path(__file__).with_name(".env")
try:
    from dotenv import load_dotenv

    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
except ImportError:
    try:
        import env_loader

        if ENV_PATH.exists():
            env_loader.load_env(ENV_PATH)
    except Exception:
        pass

from api.routers.webhook import router as webhook_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start/stop sync event queue worker."""
    queue = get_sync_event_queue()
    await queue.start()
    logger.info("Webhook listener started with sync worker")
    try:
        yield
    finally:
        await queue.stop()
        logger.info("Webhook listener stopped")


# Create app and include webhook router from api.routers.webhook (no duplication)
app = FastAPI(
    title="FACEIT Webhook Listener (Standalone)",
    version="1.0.0",
    description="Standalone endpoint for testing webhook ingestion. "
    "Routes are imported from api.routers.webhook for consistency.",
    lifespan=lifespan,
)

app.include_router(webhook_router, tags=["webhook"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "faceit_webhook_listener:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8010")),
        reload=False,
        log_level=os.getenv("UVICORN_LOG_LEVEL", "info"),
    )
