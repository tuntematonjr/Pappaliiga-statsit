API configuration: env vs runtime injection

When running the SPA and backend on different origins (typical during local development), the frontend needs to know where to call the API.

Options

1) Runtime injection (recommended)
   - The backend injects a small script into `index.html` that sets `window.__API_BASE__` before the SPA bundles load.
   - Pros: simple, works with static files, easy to change per environment, no rebuild needed.
   - Cons: requires backend to serve index.html (api.main already does this).

   Example (in `api/main.py` SPA fallback):

   ```python
   # read index.html bytes
   text = index_path.read_text(encoding='utf-8')
   injection = f"<script>window.__API_BASE__ = '{api_base}';</script>"
   text = text.replace('<!-- API Client -->', injection)
   return Response(text, media_type='text/html')
   ```

   Or insert the script before the first `/static/api-client.js` script tag so the client picks it up automatically.

2) Build-time / env var
   - Pass an environment variable into the build or create a small `config.js` file during the build.
   - Pros: no runtime replacement, works well for static hosting.
   - Cons: requires rebuild to change the value.

   Example: create `frontend/static/config.js` during CI with:
   ```bash
   echo "window.__API_BASE__='https://api.example.com/api';" > frontend/static/config.js
   ```
   Then include `<script src="/static/config.js"></script>` before `api-client.js` in `index.html`.

3) Proxy `/api` from frontend dev server
   - Modify `serve_frontend.py` to forward `/api` to `http://localhost:8000`.
   - Pros: frontend code doesn't need changes. Cons: adds proxying logic to the dev server.

Recommendation
- Use runtime injection for the backend-served SPA (easy to implement in `api/main.py`), and keep the hostname heuristic as a development fallback.
- For static hosting (S3, Netlify), prefer build-time `config.js` or environment variable substitution during deployment.
