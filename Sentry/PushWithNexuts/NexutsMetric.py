import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class MetricsHTTPServer(threading.Thread):
    def __init__(self, pusher, host="0.0.0.0", port=9100):
        super().__init__(daemon=True)
        self.pusher = pusher
        self.host = host
        self.port = port

    def run(self):
        server = HTTPServer((self.host, self.port), self._make_handler())
        server.serve_forever()

    def _make_handler(self):
        pusher = self.pusher

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/metrics":
                    self.send_response(404)
                    self.end_headers()
                    return

                metrics = pusher.get_metrics()
                response = self._format_prometheus(metrics)
                response_bytes = response.encode("utf-8")

                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(response_bytes)))
                self.end_headers()
                self.wfile.write(response_bytes)

            @staticmethod
            def _format_prometheus(m):
                return (
                    "# HELP sentry_queue_length Length of Redis queue\n"
                    "# TYPE sentry_queue_length gauge\n"
                    f"sentry_queue_length {m['queue_length']}\n\n"

                    "# HELP sentry_ops_id_sentry Generated ops id\n"
                    "# TYPE sentry_ops_id_sentry counter\n"
                    f"sentry_ops_id_sentry {m['ops_id_sentry']}\n\n"

                    "# HELP sentry_ops_id_sentry_finish Finished ops id\n"
                    "# TYPE sentry_ops_id_sentry_finish counter\n"
                    f"sentry_ops_id_sentry_finish {m['ops_id_sentry_finish']}\n\n"

                    "# HELP sentry_pending_ops Pending ops id\n"
                    "# TYPE sentry_pending_ops gauge\n"
                    f"sentry_pending_ops {m['pending']}\n\n"

                    "# HELP sentry_buffer_size Internal callback buffer size\n"
                    "# TYPE sentry_buffer_size gauge\n"
                    f"sentry_buffer_size {m['buffer_size']}\n"
                )

        return Handler