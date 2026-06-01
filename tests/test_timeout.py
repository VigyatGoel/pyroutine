"""Tests for Socket and HTTP client timeout handling."""

import pytest
import socket
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import pyroutine as pr
from pyroutine import spawn, Socket


# ---------- Tests ----------


def test_socket_recv_timeout():
    a, b = socket.socketpair()
    sa = Socket(_sock=a)

    # Set a small timeout (50ms)
    sa.settimeout(0.05)
    assert sa.gettimeout() == 0.05

    got_error = False

    def reader():
        nonlocal got_error
        try:
            sa.recv(64)
        except TimeoutError:
            got_error = True

    spawn(reader).join()
    assert got_error is True

    # Make sure we can still read if data arrives after clearing timeout
    sa.settimeout(None)
    b.sendall(b"data")

    got_data = []

    def reader2():
        got_data.append(sa.recv(64))

    spawn(reader2).join()
    assert got_data == [b"data"]

    a.close()
    b.close()


def test_socket_recv_no_timeout_when_data_arrives():
    a, b = socket.socketpair()
    sa = Socket(_sock=a)
    sa.settimeout(1.0)  # generous timeout

    got_data = []

    def reader():
        got_data.append(sa.recv(64))

    h = spawn(reader)
    time.sleep(0.05)
    b.sendall(b"hello")
    h.join()

    assert got_data == [b"hello"]
    a.close()
    b.close()


class HangingHTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Hang forever (simulate slow request)
        time.sleep(2.0)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def hanging_server():
    server = HTTPServer(("127.0.0.1", 0), HangingHTTPHandler)
    host, port = server.server_address
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield host, port
    server.shutdown()


def test_http_client_timeout(hanging_server):
    host, port = hanging_server
    url = f"http://{host}:{port}/"

    got_error = False

    def client():
        nonlocal got_error
        try:
            pr.http.get(url, timeout=0.1)
        except TimeoutError:
            got_error = True

    spawn(client).join()
    assert got_error is True
