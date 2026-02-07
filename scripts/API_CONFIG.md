API base URL configuration

`frontend/static/api-client.js` resolves the API base URL in this order:

1) `window.PL_API_URL`
2) `window.__API_BASE__`
3) Fallback: `${window.location.origin}/api`

If you run API + frontend from the same origin (default local setup with uvicorn), no extra config is needed.

## Runtime override (recommended for split origins)

Add a script before `/static/api-client.js` in `frontend/index.html`:

```html
<script>window.PL_API_URL = 'http://localhost:8000/api';</script>
```

Use this when the frontend and backend run on different hosts/ports.

## Static config file option

Create `frontend/static/config.js` during deploy:

```bash
echo "window.PL_API_URL='https://api.example.com/api';" > frontend/static/config.js
```

Then include it before `api-client.js`:

```html
<script src="/static/config.js"></script>
```

## Notes

- `api/main.py` currently serves `frontend/index.html` as-is and does not inject API base values at runtime.
- Keep URL scripts before `/static/api-client.js` so the client picks the value during initialization.
