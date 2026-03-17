"""
Minimal EventBridge stub for the local devstack.

AWS Step Functions Local does not emulate EventBridge. When metaflow deploys
a flow to SFN, it always calls schedule() which tries to create/disable an
EventBridge rule. This stub handles those calls so that devstack deploys work
without a real EventBridge connection.

Behaviour:
  - DisableRule  → 400 ResourceNotFoundException (silently ignored by EventBridgeClient._disable)
  - PutRule      → 200 with a fake RuleArn
  - PutTargets   → 200 with empty FailedEntries
  - Any other    → 200 empty JSON

Run on port 7777 (set AWS_ENDPOINT_URL_EVENTBRIDGE=http://localhost:7777).
"""

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer as HTTPServer

PORT = 7777


class EventBridgeHandler(BaseHTTPRequestHandler):
    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def _send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.1")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        # Health check
        self._send_json(200, {"status": "ok"})

    def do_POST(self):
        self._read_body()
        target = self.headers.get("X-Amz-Target", "")

        if target.endswith("DisableRule"):
            # EventBridgeClient._disable() catches ResourceNotFoundException and ignores it.
            self._send_json(
                400,
                {
                    "__type": "ResourceNotFoundException",
                    "message": "Rule not found",
                },
            )
        elif target.endswith("PutRule"):
            self._send_json(
                200,
                {"RuleArn": "arn:aws:events:us-east-1:123456789012:rule/devstack-stub"},
            )
        elif target.endswith("PutTargets"):
            self._send_json(200, {"FailedEntryCount": 0, "FailedEntries": []})
        else:
            self._send_json(200, {})

    def log_message(self, fmt, *args):
        # Suppress per-request log noise; only print startup message.
        pass


if __name__ == "__main__":
    server = HTTPServer(("", PORT), EventBridgeHandler)
    print(f"EventBridge stub listening on port {PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        sys.exit(0)
