# Sync Events

This document explains how to trigger targeted sync jobs through the API.

## Overview

Sync events let you enqueue a sync job for:
- one match (`match_id`)
- one championship (`championship_id`)

The API returns quickly after enqueueing. A background worker processes jobs.

Routes:
- `POST /api/sync/events`
- `GET /api/sync/events/status`

Code references:
- `api/routers/sync_events.py`
- `api/services/sync_event_queue.py`
- `api/main.py`

## Enable Routes

Set environment variables:

```bash
ENABLE_SYNC_EVENT_ROUTES=true
SYNC_EVENT_TOKEN=your-strong-secret-token
```

If `SYNC_EVENT_TOKEN` is missing, routes are not enabled.

## Authentication

All sync event routes require header:

```text
X-Sync-Event-Token: <SYNC_EVENT_TOKEN>
```

## Enqueue Match Sync

```bash
curl -X POST "http://localhost:8000/api/sync/events" \
  -H "Content-Type: application/json" \
  -H "X-Sync-Event-Token: your-strong-secret-token" \
  -d '{"match_id":"1-a1b2c3d4-e5f6-7890-abcd-1234567890ef"}'
```

## Enqueue Championship Sync

Incremental:

```bash
curl -X POST "http://localhost:8000/api/sync/events" \
  -H "Content-Type: application/json" \
  -H "X-Sync-Event-Token: your-strong-secret-token" \
  -d '{"championship_id":"abc123"}'
```

Full:

```bash
curl -X POST "http://localhost:8000/api/sync/events" \
  -H "Content-Type: application/json" \
  -H "X-Sync-Event-Token: your-strong-secret-token" \
  -d '{"championship_id":"abc123","full":true}'
```

## Queue Status

```bash
curl "http://localhost:8000/api/sync/events/status" \
  -H "X-Sync-Event-Token: your-strong-secret-token"
```

Example response fields:
- `queue_size`
- `queued_keys`
- `processing_keys`
- `recent_keys`
- `worker_running`

## Request Validation Rules

For `POST /api/sync/events`:
- provide exactly one of:
  - `match_id`
  - `championship_id`
- if both are set or both are missing, API returns `400`.

## Dedupe Behavior

Jobs are deduped by key:
- `match:<match_id>`
- `championship:<championship_id>`

A job is rejected (`accepted=false`, `deduped=true`) if the same key is:
- already queued
- currently processing
- recently processed within TTL

TTL is controlled by:

```bash
SYNC_EVENT_DEDUPE_TTL=120
```

## Optional Tuning

```bash
# Worker DB concurrency for championship jobs
SYNC_EVENT_DB_CONCURRENCY=6

# Match concurrency inside one championship job
SYNC_EVENT_MAX_MATCH_CONCURRENCY=4

# Validate team avatar URLs during event-triggered sync
SYNC_EVENT_VALIDATE_AVATARS=false
```

## Notes

- Queue is in-memory (not persistent across process restart).
- Jobs are processed by a background worker started during API startup.
- On processing errors, details are written to server logs.
