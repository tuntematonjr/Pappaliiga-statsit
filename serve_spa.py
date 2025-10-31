"""Simple SPA static server with fallback to index.html.

Usage:
    python serve_spa.py [--port 8000] [--dir frontend]

This is for local preview only. It serves the `frontend/` build and returns
`index.html` for unknown paths so history-mode routing works.
"""
import argparse
import http.server
import socketserver
import os


class SPARequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # If the requested path corresponds to an existing file, serve it
        requested_path = self.translate_path(self.path)
        if os.path.exists(requested_path) and not os.path.isdir(requested_path):
            return super().do_GET()

        # Otherwise, fall back to the SPA entrypoint so client-side routing works
        self.path = '/index.html'
        return super().do_GET()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--dir', default='frontend')
    args = parser.parse_args()

    web_dir = os.path.abspath(args.dir)
    if not os.path.isdir(web_dir):
        print(f"Error: directory '{web_dir}' not found")
        return

    os.chdir(web_dir)
    handler = SPARequestHandler
    handler.directory = web_dir

    with socketserver.TCPServer(('0.0.0.0', args.port), handler) as httpd:
        print(f"Serving SPA from {web_dir} at http://localhost:{args.port}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('Shutting down')


if __name__ == '__main__':
    main()
