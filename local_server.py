"""
local_server.py
===============
Static server for the dashboard, plus the two things a static page cannot do
for itself:

  /config.json   the browser-side settings (which MQTT WebSocket endpoint and
                 topic to connect to, where the UrbanSim API is), read from the
                 same .env as every other process — the page has no defaults of
                 its own, so it and the brokers can never disagree.
  /proxy/<url>   a CORS stripper for the UrbanSim API, restricted to
                 URBANSIM_API_BASE so this is not an open proxy.
"""

import json
import urllib.request
import urllib.parse
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

import config

PORT = config.require("FRONTEND_PORT", int)
# /proxy/ fetches whatever this host can reach at the API base, so it stays on
# loopback unless explicitly told otherwise (docker-compose.yml sets
# FRONTEND_BIND=0.0.0.0 inside the container and maps the port back to
# 127.0.0.1 on the host).
BIND = config.require("FRONTEND_BIND")
API_BASE = config.require("URBANSIM_API_BASE").rstrip("/")
BROWSER_CONFIG = {
    "mqttHost": config.require("MQTT_WS_HOST"),
    "mqttPort": config.require("MQTT_WS_PORT", int),
    "topic": config.require("TOPIC"),
    "sensorsTopic": config.require("SENSORS_TOPIC"),
    "urbansimApiBase": API_BASE,
    "urbansimProjectId": config.optional("URBANSIM_PROJECT_ID") or "",
}


class CORSProxyHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # Allow cross-origin requests for all files served locally
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

    def do_GET(self):
        if self.path == '/config.json':
            body = json.dumps(BROWSER_CONFIG).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith('/proxy/'):
            target_url = urllib.parse.unquote(self.path.split('/proxy/', 1)[1])
            if not target_url.startswith(API_BASE + "/"):
                self.send_error(403, f"proxy only serves {API_BASE}")
                return
            try:
                req = urllib.request.Request(target_url)
                with urllib.request.urlopen(req) as res:
                    self.send_response(res.status)
                    self.send_header('Access-Control-Allow-Origin', '*')
                    for k, v in res.getheaders():
                        if k.lower() not in ['access-control-allow-origin', 'transfer-encoding', 'connection']:
                            self.send_header(k, v)
                    self.end_headers()
                    self.wfile.write(res.read())
            except Exception as e:
                print(f"Proxy error for {target_url}: {e}")
                self.send_error(500, str(e))
        else:
            super().do_GET()


if __name__ == '__main__':
    print(f"Dashboard on http://localhost:{PORT}/mqtt_web_tester.html (bound to {BIND}); "
          f"browser connects to ws://{BROWSER_CONFIG['mqttHost']}:{BROWSER_CONFIG['mqttPort']}, "
          f"proxying {API_BASE}")
    # Threaded so a slow upstream GeoJSON fetch does not block the static files.
    ThreadingHTTPServer((BIND, PORT), CORSProxyHandler).serve_forever()
