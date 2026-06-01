"""Tests for pyroutine.http Pythonic web server framework."""

import socket
import time
import pytest

import pyroutine as pr
from pyroutine import spawn, TaskGroup, Socket


# Helper to find a free local port
def get_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# Create the HTTP App
app = pr.http.App()


@app.get("/hello")
def hello(r):
    return "Hello, World!"


@app.post("/post")
def post_route(r):
    return r.text, 200, {"Content-Type": "application/json"}


@app.get("/delay")
def delay(r):
    pr.sleep(0.1)
    return "done"


@app.get("/multiple-writes")
def multiple_writes(r):
    return "Chunk 1, Chunk 2"


@app.post("/chunked-request")
def chunked_request(r):
    return f"Received: {r.text}"


@pytest.fixture(scope="module")
def local_server():
    port = get_free_port()
    address = f"127.0.0.1:{port}"

    # Start App router in a background thread/routine
    server, h = app.start(address)
    time.sleep(0.05)  # Give server socket time to bind and listen

    yield address

    server.close()
    h.join()


# ---------- Tests ----------


def test_server_get(local_server):
    address = local_server

    def client():
        return pr.http.get(f"http://{address}/hello")

    res = spawn(client).result()
    assert res.status_code == 200
    assert "text/plain" in res.headers.get("content-type", "")
    assert res.text == "Hello, World!"


def test_server_post(local_server):
    address = local_server
    payload = "post body data"

    def client():
        return pr.http.post(f"http://{address}/post", data=payload)

    res = spawn(client).result()
    assert res.status_code == 200
    assert "application/json" in res.headers.get("content-type", "")
    assert res.text == payload


def test_server_404(local_server):
    address = local_server

    def client():
        return pr.http.get(f"http://{address}/unknown")

    res = spawn(client).result()
    assert res.status_code == 404
    assert res.text == "Not Found"


def test_server_concurrency(local_server):
    address = local_server
    results = [None] * 2

    def client(i):
        res = pr.http.get(f"http://{address}/delay")
        results[i] = res.text

    t0 = time.monotonic()

    with TaskGroup() as tg:
        tg.spawn(client, 0)
        tg.spawn(client, 1)

    dt = time.monotonic() - t0

    assert results == ["done", "done"]
    # If the requests were serialized, total time would be >= 0.2s.
    # Running concurrently, they should complete in parallel in ~0.1s + overhead.
    assert dt < 0.18, f"Requests took too long: {dt:.3f}s"


def test_server_multiple_writes(local_server):
    address = local_server

    def client():
        return pr.http.get(f"http://{address}/multiple-writes")

    res = spawn(client).result()
    assert res.status_code == 200
    assert res.headers.get("content-length") == "16"
    assert res.text == "Chunk 1, Chunk 2"


def test_server_keep_alive(local_server):
    address = local_server
    host, port_str = address.split(":", 1)
    port = int(port_str)

    got = []

    def client():
        # Open a single raw Socket connection
        s = Socket()
        s.connect((host, port))

        # 1. Send first request (HTTP/1.1 keep-alive by default)
        req1 = "GET /multiple-writes HTTP/1.1\r\nHost: localhost\r\n\r\n"
        s.sendall(req1.encode())

        # Read response 1 headers & body
        resp_bytes = b""
        while b"\r\n\r\n" not in resp_bytes:
            resp_bytes += s.recv(1024)

        idx = resp_bytes.find(b"\r\n\r\n")
        headers = resp_bytes[:idx].decode()
        body = resp_bytes[idx + 4 :]

        # Content length is 16
        while len(body) < 16:
            body += s.recv(1024)

        got.append((headers, body[:16]))

        # 2. Send second request on the SAME socket
        req2 = "GET /multiple-writes HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        s.sendall(req2.encode())

        # Read response 2 headers & body
        resp_bytes2 = b""
        while b"\r\n\r\n" not in resp_bytes2:
            resp_bytes2 += s.recv(1024)

        idx2 = resp_bytes2.find(b"\r\n\r\n")
        headers2 = resp_bytes2[:idx2].decode()
        body2 = resp_bytes2[idx2 + 4 :]

        while len(body2) < 16:
            body2 += s.recv(1024)

        got.append((headers2, body2[:16]))
        s.close()

    spawn(client).join()

    assert len(got) == 2
    # Verify response 1
    h1, b1 = got[0]
    assert "Connection: keep-alive" in h1 or "connection: keep-alive" in h1
    assert b1 == b"Chunk 1, Chunk 2"

    # Verify response 2
    h2, b2 = got[1]
    assert "Connection: close" in h2 or "connection: close" in h2
    assert b2 == b"Chunk 1, Chunk 2"


def test_server_chunked_request(local_server):
    address = local_server
    host, port_str = address.split(":", 1)
    port = int(port_str)

    got = []

    def client():
        s = Socket()
        s.connect((host, port))

        # Send chunked request body
        req = (
            "POST /chunked-request HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n\r\n"
            "5\r\nhello\r\n"
            "6\r\n world\r\n"
            "0\r\n\r\n"
        )
        s.sendall(req.encode())

        # Read response
        resp_bytes = b""
        while True:
            chunk = s.recv(1024)
            if not chunk:
                break
            resp_bytes += chunk

        got.append(resp_bytes)
        s.close()

    spawn(client).join()

    assert len(got) == 1
    resp = got[0].decode()
    assert "200" in resp
    assert "Received: hello world" in resp
