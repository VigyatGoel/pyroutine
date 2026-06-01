"""Tests for pyroutine.http cooperative client."""

import socket
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytest

import pyroutine as pr
from pyroutine import spawn


# ---------- Mock HTTP Server ----------


class MockHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/chunked":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"5\r\nhello\r\n")
            self.wfile.write(b"6\r\n world\r\n0\r\n\r\n")
        elif self.path == "/404":
            self.send_response(404)
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            body = f"GET {self.path}".encode()
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def mock_server():
    server = HTTPServer(("127.0.0.1", 0), MockHTTPHandler)
    host, port = server.server_address
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield host, port
    server.shutdown()


# ---------- Tests ----------


def test_cooperative_get(mock_server):
    host, port = mock_server
    url = f"http://{host}:{port}/test-path"

    def runner():
        return pr.http.get(url)

    res = spawn(runner).result()
    assert res.status_code == 200
    assert res.headers.get("content-type") == "text/plain"
    assert res.text == "GET /test-path"


def test_cooperative_get_404(mock_server):
    host, port = mock_server
    url = f"http://{host}:{port}/404"

    def runner():
        return pr.http.get(url)

    res = spawn(runner).result()
    assert res.status_code == 404


def test_cooperative_post(mock_server):
    host, port = mock_server
    url = f"http://{host}:{port}/post-path"
    post_data = "hello from client post"

    def runner():
        return pr.http.post(url, data=post_data)

    res = spawn(runner).result()
    assert res.status_code == 200
    assert res.text == post_data


def test_cooperative_chunked_response(mock_server):
    host, port = mock_server
    url = f"http://{host}:{port}/chunked"

    def runner():
        return pr.http.get(url)

    res = spawn(runner).result()
    assert res.status_code == 200
    assert res.text == "hello world"


def test_cooperative_https_get():
    # Fetch a public website to test SSL/TLS wrap and handshake.
    # Wrap in try/except in case the environment is offline or DNS fails.
    def runner():
        try:
            return pr.http.get("https://www.google.com")
        except Exception as e:
            return e

    res = spawn(runner).result()
    if isinstance(res, Exception):
        # Gracefully skip if it's a connection/DNS error (offline sandbox)
        if isinstance(res, (OSError, socket.gaierror)):
            pytest.skip(
                f"Skipping public HTTPS test (network offline/DNS failed: {res})"
            )
        else:
            raise res

    assert res.status_code == 200
