#!/usr/bin/env python3
"""
Simple log server that accepts POST /log with a JSON body and prints to stdout.
Run: python log_server.py
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != '/log':
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get('content-length', 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode('utf-8'))
        except Exception:
            payload = {'raw': raw.decode('utf-8', errors='replace')}
        print('[CLIENT LOG]', json.dumps(payload, ensure_ascii=False))
        self.send_response(204)
        self.end_headers()

if __name__ == '__main__':
    # Bind to LAN IP to accept logs from devices on the local network
    server = HTTPServer(('192.168.0.13', 8765), Handler)
    print('Log server listening on http://192.168.0.13:8765/log')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down')
        server.server_close()
