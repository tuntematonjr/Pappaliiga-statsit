# Webhook Event Handling

## Supported Events

Webhook endpoint `/webhook/faceit` accepts POST requests and triggers `sync` for these FACEIT event types:

| Event Type | Trigger | Reason |
|------------|---------|--------|
| `match_status_finished` | Match completed, stats ready | Guarantees final data is available; avoids duplicate syncs |

### Why only `match_status_finished`?

FACEIT may send multiple events for the same match:
- `match_status_ready` — match is queued/preparing (incomplete data)
- `match_demo_ready` — demo file available (stats may not be ready yet)
- `match_status_finished` — match complete (** all stats finalized**)

**Filtering to only `match_status_finished` prevents:**
- Duplicate syncs on the same match
- Incomplete data writes (if we sync before stats are ready)
- Wasted API calls to upstream/DB

### Unsupported Events

Any other event type will be **logged** but **NOT queued for sync**.

Examples:
- `match_status_ready` — incomplete match data
- `match_demo_ready` — demo may be ready before stats
- Other FACEIT events

## Logging

All webhook requests are logged to `logs/faceit_webhooks/{date}.ndjson` with fields:
- `received_at` — ISO8601 UTC timestamp
- `event` — event type from body
- `method` — HTTP method
- `headers` — request headers (redacted)
- `body_bytes`, `body_json`, `body_text` — payload info
- `accepted` — whether sync was queued
- `sync_reason` — why sync was/wasn't queued

### Sync Reason Codes

| Reason | Meaning |
|--------|---------|
| *(empty)* | Sync succeeded |
| `deduped_or_already_processing` | Match already queued/processing |
| `unsupported_event_type:TYPE` | Event type not in supported list |
| `no_match_id_in_payload` | Could not extract `match_id` from payload |
| `enqueue_failed:EXCEPTION` | Queue enqueue failed (check logs) |
| `unsupported_event` | *(legacy)* Old code rejected event type |

## Configuration

Environment variables:
- `FACEIT_WEBHOOK_SYNC_ENABLED=1` — enable/disable sync on webhook (default: 1)
- `FACEIT_WEBHOOK_TOKEN=secret` — require token on webhook (optional)
- `FACEIT_WEBHOOK_LOG_DIR=logs/faceit_webhooks` — log destination
- `FACEIT_WEBHOOK_MAX_BODY_BYTES=2097152` — max request body (default: 2 MB)

## Implementation

Two parallel endpoints:
1. **API Router** (`api/routers/webhook.py`) — mounted on `api.main:app` (Linux deployment)
   - Exposed as `/webhook/faceit` and `/api/webhook/faceit`
   - Includes sync event queue integration
2. **Standalone Listener** (`faceit_webhook_listener.py`) — separate app on port 8010
   - Can be run independently for testing/debugging

Both use identical event filtering and sync logic.
