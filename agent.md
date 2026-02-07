# Agent Guide

## Scope
This file applies to the whole repository.

## Goal
Keep the codebase working, docs accurate, and changes minimal and testable.

## Local Run
- Start API + frontend: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`
- Scripted start (Linux/macOS): `./scripts/dev_start_simple.sh 8000 .venv/bin/activate`
- Scripted start (PowerShell): `.\scripts\dev_start_simple.ps1 -Port 8000 -VenvPath ".\venv\Scripts\Activate.ps1"`

## Docs Policy
- Keep Markdown files short and current.
- Remove stale implementation-status docs when superseded.
- Prefer quick-reference docs over long narrative docs.
- Validate file paths and commands mentioned in docs.

## Edit Rules
- Do not revert unrelated user changes.
- Avoid destructive git commands unless explicitly requested.
- Keep changes focused to the request.
- Update docs when behavior or commands change.
- Use the database efficiently (select only needed fields, avoid unnecessary queries, and prefer indexed/filter-first patterns).
- Avoid repeating code; prefer shared/global functions when they are not meaningfully slower than local custom logic.
- All API calls should be cached by default; add a clear comment/reason for any endpoint that must bypass caching.

## API Rules
- Keep response shapes stable; if fields change, update all consumers in the same change.
- Use explicit timeout and retry policy for outbound API calls.
- Return structured error responses with stable `code` and `message` fields.

## Caching Rules
- Define cache key format and TTL per endpoint type.
- Add invalidation trigger/path when DB writes affect cached reads.
- Keep an explicit no-cache exception list (for example health checks or real-time admin endpoints).

## DB Rules
- Add/verify indexes for new filter/sort/query paths.
- Avoid `SELECT *` in API-facing query paths; fetch only needed columns.
- Prefer batched queries over N+1 query loops.

## Performance Budgets
- Keep hot endpoints within agreed latency targets (for example p95).
- Require pagination for large list endpoints.
- Keep payload size proportional to UI needs; avoid overfetching.

## Testing Requirements
- API behavior changes must include updated or new integration tests when feasible.
- Query refactors must verify result parity on representative data.
- Cache changes must verify hit behavior and invalidation behavior.

## Observability
- Log cache hit/miss for key endpoints where practical.
- Log slow queries above a defined threshold.
- Include request identifiers in API logs for traceability.

## Security
- Validate and sanitize all inputs used in query filters/parameters.
- Never commit secrets, tokens, or `.env` values.
- Redact sensitive values from logs and error payloads.

## Done Criteria
- Code updated and aligned with these rules.
- Relevant docs updated.
- Tests/checks run, or explicitly noted as not run.
- Risk/rollback impact stated in handoff for non-trivial changes.

## Validation
- Run relevant checks/tests when possible.
- If tests are not run, state that clearly in the handoff.
