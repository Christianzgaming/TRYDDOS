import sys
import os
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from multi_stresser_vercel import run_test

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Parse query parameters
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        
        target = params.get('target', ['https://167.104.100.205/'])[0]
        concurrency = int(params.get('concurrency', ['10'])[0])
        duration = int(params.get('duration', ['5'])[0])
        
        # Run stress test
        try:
            result = asyncio.run(run_test(target, concurrency, duration))
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result, indent=2).encode())
            
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

# Vercel serverless function
def main(request):
    # Simple handler for Vercel
    return {"statusCode": 200, "body": "Stress test completed"}