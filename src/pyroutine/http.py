"""Cooperative HTTP and HTTPS client for pyroutine.

Exposes:
- `get(url, headers=None)`
- `post(url, data=None, headers=None)`
- `request(method, url, data=None, headers=None)`

Uses pyroutine.Socket under the hood. Hostname resolution is resolved via run_blocking().
HTTPS/TLS connections are wrapped using a cooperative SSL handshake and transfer layer.
"""

import socket
import ssl
import urllib.parse
from ._net import Socket
from ._runtime import poll_wait, run_blocking, READ, WRITE
from ._pyroutine import spawn


class Response:
    """Represents an HTTP response."""

    def __init__(self, status_code, headers, body):
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self._text = None

    @property
    def text(self):
        """Decode and cache the response body as text."""
        if self._text is None:
            charset = "utf-8"
            content_type = self.headers.get("content-type", "")
            if "charset=" in content_type:
                try:
                    charset = content_type.split("charset=")[-1].strip()
                except Exception:
                    pass
            try:
                self._text = self.body.decode(charset, errors="replace")
            except Exception:
                self._text = self.body.decode("utf-8", errors="replace")
        return self._text

    def __repr__(self):
        return f"<Response [{self.status_code}]>"


class CooperativeSSLSocket:
    """A wrapper around Python's SSLSocket that performs handshakes and I/O cooperatively."""

    def __init__(self, raw_sock, ssl_context, server_hostname, timeout=None):
        self._ssl_sock = ssl_context.wrap_socket(
            raw_sock,
            server_hostname=server_hostname,
            do_handshake_on_connect=False,
        )
        self._timeout = timeout

    def do_handshake(self):
        """Perform the SSL handshake cooperatively."""
        while True:
            try:
                self._ssl_sock.do_handshake()
                break
            except ssl.SSLWantReadError:
                poll_wait(self._ssl_sock.fileno(), READ, timeout=self._timeout)
            except ssl.SSLWantWriteError:
                poll_wait(self._ssl_sock.fileno(), WRITE, timeout=self._timeout)

    def sendall(self, data):
        """Transmit data cooperatively over the SSL connection."""
        view = memoryview(data)
        while view:
            try:
                sent = self._ssl_sock.send(view)
                view = view[sent:]
            except ssl.SSLWantReadError:
                poll_wait(self._ssl_sock.fileno(), READ, timeout=self._timeout)
            except ssl.SSLWantWriteError:
                poll_wait(self._ssl_sock.fileno(), WRITE, timeout=self._timeout)

    def recv(self, bufsize):
        """Receive data cooperatively from the SSL connection."""
        while True:
            try:
                return self._ssl_sock.recv(bufsize)
            except ssl.SSLWantReadError:
                poll_wait(self._ssl_sock.fileno(), READ, timeout=self._timeout)
            except ssl.SSLWantWriteError:
                poll_wait(self._ssl_sock.fileno(), WRITE, timeout=self._timeout)

    def close(self):
        """Close the SSL socket."""
        try:
            self._ssl_sock.close()
        except Exception:
            pass


def request(method, url, data=None, headers=None, timeout=None):
    """Make a cooperative HTTP/HTTPS request."""
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Unsupported scheme: {scheme}")

    host = parsed.hostname
    if not host:
        raise ValueError("Invalid URL: missing hostname")

    port = parsed.port
    if port is None:
        port = 443 if scheme == "https" else 80

    path = parsed.path
    if not path:
        path = "/"
    if parsed.query:
        path += "?" + parsed.query

    # 1. DNS Resolution (cooperative via run_blocking)
    addrinfo = run_blocking(
        socket.getaddrinfo, host, port, socket.AF_INET, socket.SOCK_STREAM
    )
    if not addrinfo:
        raise OSError(f"Could not resolve host: {host}")
    family, socktype, proto, canonname, sockaddr = addrinfo[0]

    # 2. Connection
    raw_socket = Socket(family, socktype, proto)
    if timeout is not None:
        raw_socket.settimeout(timeout)
    raw_socket.connect(sockaddr)

    # 3. SSL/TLS Wrapping
    if scheme == "https":
        ssl_context = ssl.create_default_context()
        conn = CooperativeSSLSocket(
            raw_socket._sock, ssl_context, host, timeout=timeout
        )
        conn.do_handshake()
    else:
        conn = raw_socket

    try:
        # 4. Format & Send HTTP Request
        req_headers = {
            "Host": f"{host}:{port}" if parsed.port else host,
            "User-Agent": "pyroutine-http/0.1.0",
            "Connection": "close",
        }
        if headers:
            for k, v in headers.items():
                req_headers[k] = v

        if data is not None:
            if isinstance(data, str):
                body_bytes = data.encode("utf-8")
            else:
                body_bytes = data
            req_headers["Content-Length"] = str(len(body_bytes))
        else:
            body_bytes = b""

        req_lines = [f"{method.upper()} {path} HTTP/1.1"]
        for k, v in req_headers.items():
            req_lines.append(f"{k}: {v}")
        req_lines.append("")
        req_lines.append("")

        header_bytes = "\r\n".join(req_lines).encode("utf-8")
        conn.sendall(header_bytes + body_bytes)

        # 5. Read HTTP Response Headers
        response_bytes = b""
        headers_end = -1
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            response_bytes += chunk
            headers_end = response_bytes.find(b"\r\n\r\n")
            if headers_end != -1:
                break

        if headers_end == -1:
            raise OSError(
                "Server closed connection without valid HTTP response headers"
            )

        header_part = response_bytes[:headers_end]
        remaining_body = response_bytes[headers_end + 4 :]

        header_lines = header_part.decode("utf-8", errors="replace").split("\r\n")
        status_line = header_lines[0]
        try:
            proto, status_code_str, *reason = status_line.split(" ", 2)
            status_code = int(status_code_str)
        except Exception:
            raise ValueError(f"Invalid status line: {status_line}")

        resp_headers = {}
        for line in header_lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                resp_headers[k.strip().lower()] = v.strip()

        # 6. Read Response Body
        body_bytes = b""
        is_chunked = resp_headers.get("transfer-encoding", "").lower() == "chunked"
        content_length = resp_headers.get("content-length")

        if is_chunked:
            buffer = remaining_body
            while True:
                idx = buffer.find(b"\r\n")
                if idx == -1:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk
                    continue

                size_str = buffer[:idx].split(b";")[0].strip()
                try:
                    chunk_size = int(size_str, 16)
                except ValueError:
                    raise ValueError(f"Invalid chunk size: {size_str}")

                if chunk_size == 0:
                    break

                needed = idx + 2 + chunk_size + 2
                while len(buffer) < needed:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk

                body_bytes += buffer[idx + 2 : idx + 2 + chunk_size]
                buffer = buffer[idx + 2 + chunk_size + 2 :]
        elif content_length is not None:
            content_length = int(content_length)
            body_bytes = remaining_body
            while len(body_bytes) < content_length:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                body_bytes += chunk
            body_bytes = body_bytes[:content_length]
        else:
            # Read until EOF
            body_bytes = remaining_body
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                body_bytes += chunk

        return Response(status_code, resp_headers, body_bytes)
    finally:
        conn.close()


def get(url, headers=None, timeout=None):
    """Perform a cooperative HTTP/HTTPS GET request."""
    return request("GET", url, headers=headers, timeout=timeout)


def post(url, data=None, headers=None, timeout=None):
    """Perform a cooperative HTTP/HTTPS POST request."""
    return request("POST", url, data=data, headers=headers, timeout=timeout)


# ---------- Go-Style Concurrent HTTP Server ----------


class Request:
    """Represents an incoming HTTP request."""

    def __init__(self, method, path, headers, body):
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body

    @property
    def text(self):
        """Decode and return the request body as text."""
        return self.body.decode("utf-8", errors="replace")


class ResponseWriter:
    """Analogous to Go's http.ResponseWriter."""

    def __init__(self, conn):
        self._conn = conn
        self.status_code = 200
        self.headers = {
            "Content-Type": "text/plain",
            "Server": "pyroutine-http/0.1.0",
        }
        self._body_parts = []
        self._headers_written = False

    def set_header(self, key, value):
        """Set an HTTP response header."""
        self.headers[key] = value

    def write_header(self, status_code):
        """Send the HTTP status line and response headers."""
        if self._headers_written:
            return
        self.status_code = status_code
        lines = [f"HTTP/1.1 {self.status_code}"]
        for k, v in self.headers.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append("")
        self._conn.sendall("\r\n".join(lines).encode("utf-8"))
        self._headers_written = True

    def write(self, data):
        """Buffer response data."""
        if isinstance(data, str):
            data_bytes = data.encode("utf-8")
        else:
            data_bytes = data
        self._body_parts.append(data_bytes)

    def flush(self):
        """Send any buffered headers and body."""
        body_bytes = b"".join(self._body_parts)
        if not self._headers_written:
            if (
                "Content-Length" not in self.headers
                and "content-length" not in self.headers
            ):
                self.headers["Content-Length"] = str(len(body_bytes))
            self.write_header(self.status_code)
        if body_bytes:
            self._conn.sendall(body_bytes)
            self._body_parts.clear()


def _handle_server_connection(conn, handler):
    try:
        # Default keep-alive timeout for idle connection
        idle_timeout = 5.0
        # Request read timeout once request starts
        request_timeout = 30.0

        conn.settimeout(idle_timeout)
        buffer = b""

        while True:
            # 1. Read request headers
            headers_end = buffer.find(b"\r\n\r\n")
            while headers_end == -1:
                # If we have no data yet, use idle timeout. Otherwise, use request timeout.
                if buffer:
                    conn.settimeout(request_timeout)
                else:
                    conn.settimeout(idle_timeout)

                try:
                    chunk = conn.recv(4096)
                except TimeoutError:
                    return  # Silent return on timeout (standard server behavior)

                if not chunk:
                    return  # EOF client closed connection
                buffer += chunk
                headers_end = buffer.find(b"\r\n\r\n")

            header_part = buffer[:headers_end]
            buffer = buffer[headers_end + 4 :]

            # 2. Parse request headers
            header_lines = header_part.decode("utf-8", errors="replace").split("\r\n")
            status_line = header_lines[0]
            try:
                method, path, proto_str = status_line.split(" ", 2)
            except Exception:
                return

            headers = {}
            for line in header_lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            # 3. Read request body (supporting Content-Length and Chunked)
            conn.settimeout(request_timeout)
            body = b""
            content_length_str = headers.get("content-length")
            transfer_encoding = headers.get("transfer-encoding", "").lower()

            try:
                if transfer_encoding == "chunked":
                    while True:
                        idx = buffer.find(b"\r\n")
                        while idx == -1:
                            chunk = conn.recv(4096)
                            if not chunk:
                                break
                            buffer += chunk
                            idx = buffer.find(b"\r\n")

                        if idx == -1:
                            break

                        size_str = buffer[:idx].split(b";")[0].strip()
                        try:
                            chunk_size = int(size_str, 16)
                        except ValueError:
                            return

                        if chunk_size == 0:
                            needed = idx + 4
                            while len(buffer) < needed:
                                chunk = conn.recv(4096)
                                if not chunk:
                                    break
                                buffer += chunk
                            buffer = buffer[idx + 4 :]
                            break

                        needed = idx + 2 + chunk_size + 2
                        while len(buffer) < needed:
                            chunk = conn.recv(4096)
                            if not chunk:
                                break
                            buffer += chunk

                        body += buffer[idx + 2 : idx + 2 + chunk_size]
                        buffer = buffer[idx + 2 + chunk_size + 2 :]
                elif content_length_str:
                    try:
                        content_length = int(content_length_str)
                    except ValueError:
                        content_length = 0

                    while len(buffer) < content_length:
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        buffer += chunk
                    body = buffer[:content_length]
                    buffer = buffer[content_length:]
            except TimeoutError:
                return
            except Exception:
                return

            # 4. Handle request
            req = Request(method, path, headers, body)
            w = ResponseWriter(conn)

            # Determine keep-alive status
            is_http11 = "1.1" in proto_str
            conn_header = headers.get("connection", "").lower()
            if conn_header == "close":
                keep_alive = False
            elif conn_header == "keep-alive":
                keep_alive = True
            else:
                keep_alive = is_http11

            if keep_alive:
                w.set_header("Connection", "keep-alive")
            else:
                w.set_header("Connection", "close")

            try:
                handler(w, req)
                w.flush()
            except Exception:
                keep_alive = False
                break

            if not keep_alive:
                break
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


class Server:
    """A concurrent HTTP server (internal to App)."""

    def __init__(self, address, handler):
        if ":" not in address:
            raise ValueError("Address must be in 'host:port' format")
        self.address = address
        self.handler = handler
        self._socket = None
        self._running = False

    def serve(self):
        """Run the server accept loop. Blocks cooperatively."""
        host, port_str = self.address.split(":", 1)
        port = int(port_str)

        self._socket = Socket()
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, port))
        self._socket.listen(4096)

        # Update address in case port 0 (ephemeral) was used
        real_host, real_port = self._socket.getsockname()
        self.address = f"{real_host}:{real_port}"
        self._running = True

        try:
            while self._running:
                try:
                    conn, addr = self._socket.accept()
                except Exception:
                    break

                if not self._running:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    break

                spawn(_handle_server_connection, conn, self.handler)
        finally:
            self._running = False
            try:
                self._socket.close()
            except Exception:
                pass

    def start(self):
        """Start the server concurrently as a pyroutine and return its Task."""
        return spawn(self.serve)

    def close(self):
        """Stop the server by waking up the accept loop and closing its socket."""
        if not self._running:
            return
        self._running = False

        # Trigger a dummy connection to wake up the parked accept() call
        try:
            host, port_str = self.address.split(":", 1)
            port = int(port_str)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.1)
            s.connect((host, port))
            s.close()
        except Exception:
            pass


class App:
    """A Flask/FastAPI-style Pythonic routing framework."""

    def __init__(self):
        self.routes = {}

    def route(self, path, methods=["GET"]):
        """Decorator to register a handler for a given path and methods."""

        def decorator(func):
            for method in methods:
                self.routes.setdefault(path, {})[method.upper()] = func
            return func

        return decorator

    def get(self, path):
        """Decorator for GET requests."""
        return self.route(path, methods=["GET"])

    def post(self, path):
        """Decorator for POST requests."""
        return self.route(path, methods=["POST"])

    def put(self, path):
        """Decorator for PUT requests."""
        return self.route(path, methods=["PUT"])

    def delete(self, path):
        """Decorator for DELETE requests."""
        return self.route(path, methods=["DELETE"])

    def __call__(self, request):
        method = request.method.upper()
        path = request.path

        method_routes = self.routes.get(path)
        if not method_routes or method not in method_routes:
            return Response(404, {"Content-Type": "text/plain"}, b"Not Found")

        handler = method_routes[method]
        try:
            rv = handler(request)
            return self._make_response(rv)
        except Exception as e:
            err_msg = f"Internal Server Error: {e}".encode("utf-8")
            return Response(500, {"Content-Type": "text/plain"}, err_msg)

    def _make_response(self, rv):
        if isinstance(rv, Response):
            return rv

        status = 200
        headers = {}
        body = b""
        rv_headers = {}

        if isinstance(rv, tuple):
            if len(rv) == 3:
                rv_body, status, rv_headers = rv
            elif len(rv) == 2:
                rv_body, status = rv
                rv_headers = {}
            else:
                rv_body = rv[0]
                status = 200
                rv_headers = {}
        else:
            rv_body = rv

        if isinstance(rv_headers, dict):
            for k, v in rv_headers.items():
                headers[k] = v

        # Set default content type if not present
        content_type_key = next(
            (k for k in headers if k.lower() == "content-type"), None
        )

        if isinstance(rv_body, str):
            body = rv_body.encode("utf-8")
            if content_type_key is None:
                headers["Content-Type"] = "text/plain; charset=utf-8"
        elif isinstance(rv_body, bytes):
            body = rv_body
            if content_type_key is None:
                headers["Content-Type"] = "application/octet-stream"
        elif isinstance(rv_body, (dict, list)):
            import json

            body = json.dumps(rv_body).encode("utf-8")
            if content_type_key is None:
                headers["Content-Type"] = "application/json; charset=utf-8"
        else:
            body = str(rv_body).encode("utf-8")
            if content_type_key is None:
                headers["Content-Type"] = "text/plain; charset=utf-8"

        return Response(status, headers, body)

    def start(self, address):
        """Start the App router concurrently in a pyroutine and return the (Server, Task)."""

        def app_handler(w, r):
            response = self(r)
            w.status_code = response.status_code
            for k, v in response.headers.items():
                w.set_header(k, v)
            w.write(response.body)

        server = Server(address, app_handler)
        h = server.start()
        return server, h

    def serve(self, address):
        """Block cooperatively and serve requests using this App."""

        def app_handler(w, r):
            response = self(r)
            w.status_code = response.status_code
            for k, v in response.headers.items():
                w.set_header(k, v)
            w.write(response.body)

        server = Server(address, app_handler)
        server.serve()

    def run(self, address):
        """Run the App router server."""
        self.serve(address)
