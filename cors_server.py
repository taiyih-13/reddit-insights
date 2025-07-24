#!/usr/bin/env python3
"""
Simple HTTP server with CORS support for testing dashboard
"""

import http.server
import socketserver
from urllib.parse import urlparse

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

if __name__ == "__main__":
    PORT = 8000
    handler = CORSRequestHandler
    
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f"✅ CORS-enabled server running on http://localhost:{PORT}")
        print(f"📊 Dashboard: http://localhost:{PORT}/reddit_dashboard.html")
        print(f"🧪 Test page: http://localhost:{PORT}/test_summarize.html")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Server stopped")
            httpd.shutdown()