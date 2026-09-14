import os
import urllib.request
import urllib.parse
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

class CORSProxyHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        # Allow cross-origin requests for all files served locally
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

    def do_GET(self):
        if self.path.startswith('/proxy/'):
            # Extract and decode the target URL
            target_url = urllib.parse.unquote(self.path.split('/proxy/', 1)[1])
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
    port = int(os.environ.get('PORT', 8002))
    # /proxy/ will fetch any URL this host can reach, so it stays on loopback
    # unless explicitly told otherwise (docker-compose.yml sets BIND=0.0.0.0
    # inside the container and maps the port back to 127.0.0.1 on the host).
    bind = os.environ.get('BIND', '127.0.0.1')
    print(f"Starting local server with CORS proxy on http://localhost:{port} (bound to {bind}) ...")
    print("Use this to open your dashboard without CORS errors!")
    # Threaded so a slow upstream GeoJSON fetch does not block the static files.
    ThreadingHTTPServer((bind, port), CORSProxyHandler).serve_forever()
