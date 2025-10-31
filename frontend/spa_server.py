#!/usr/bin/env python3
"""Simple SPA static server with history-mode fallback.

Usage:
    python spa_server.py [port]

Serves files from this directory (the `frontend/` folder). If a requested
path doesn't exist as a file, the server responds with index.html so the
client-side router (history mode) can handle the route.
"""
import http.server
import socketserver
import os
import sys


PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class SPARequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Translate the request path to a local filesystem path
        requested_path = self.translate_path(self.path)

        # If the path exists and is a file, serve normally
        if os.path.exists(requested_path) and not os.path.isdir(requested_path):
            return super().do_GET()

        # Otherwise, serve index.html (SPA entry) for history-mode routing
        self.path = '/index.html'
        return super().do_GET()


def run(port: int = PORT):
    os.chdir(BASE_DIR)
    handler = SPARequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"Serving SPA at http://localhost:{port} (base: {BASE_DIR})")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nShutting down server')
            httpd.server_close()


if __name__ == '__main__':
    run()
