"""A cooperative, non-blocking socket that parks pyroutines on the netpoller.

Use it like a normal blocking ``socket``; under the hood every operation that would
block parks the pyroutine (via :func:`~pyroutine._netpoll.poll_wait`) and resumes
when the fd is ready -- so thousands of connections share a handful of OS threads.

    from pyroutine import Socket

    srv = Socket(); srv.bind(("127.0.0.1", 0)); srv.listen()
    conn, addr = srv.accept()       # parks until a client connects
    data = conn.recv(1024)          # parks until bytes arrive
    conn.sendall(data)

DNS is not cooperative yet -- connect with an IP, or resolve via run_blocking().
"""

import errno
import socket as _socket

from ._runtime import poll_wait, READ, WRITE

_RETRY = (errno.EWOULDBLOCK, errno.EAGAIN, errno.EINPROGRESS)


class Socket:
    def __init__(
        self, family=_socket.AF_INET, type=_socket.SOCK_STREAM, proto=0, *, _sock=None
    ):
        self._sock = _sock if _sock is not None else _socket.socket(family, type, proto)
        self._sock.setblocking(False)
        self._timeout = None

    def settimeout(self, value):
        self._timeout = value

    def gettimeout(self):
        return self._timeout

    # -- I/O (each retries on would-block, parking on the netpoller) -------

    def recv(self, bufsize):
        while True:
            try:
                return self._sock.recv(bufsize)
            except BlockingIOError, InterruptedError:
                poll_wait(self._sock.fileno(), READ, timeout=self._timeout)

    def send(self, data):
        while True:
            try:
                return self._sock.send(data)
            except BlockingIOError, InterruptedError:
                poll_wait(self._sock.fileno(), WRITE, timeout=self._timeout)

    def sendall(self, data):
        view = memoryview(data)
        while view:
            sent = self.send(view)
            view = view[sent:]

    def recvfrom(self, bufsize):
        while True:
            try:
                return self._sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                poll_wait(self._sock.fileno(), READ, timeout=self._timeout)

    def sendto(self, data, address):
        while True:
            try:
                return self._sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                poll_wait(self._sock.fileno(), WRITE, timeout=self._timeout)

    def connect(self, address):
        err = self._sock.connect_ex(address)
        if err in _RETRY:
            poll_wait(self._sock.fileno(), WRITE, timeout=self._timeout)
            err = self._sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
        if err:
            raise OSError(err, errno.errorcode.get(err, str(err)))

    def accept(self):
        while True:
            try:
                conn, addr = self._sock.accept()
                sock = Socket(_sock=conn)
                sock.settimeout(self._timeout)
                return sock, addr
            except BlockingIOError, InterruptedError:
                poll_wait(self._sock.fileno(), READ, timeout=self._timeout)

    # -- pass-throughs -----------------------------------------------------

    def bind(self, address):
        self._sock.bind(address)

    def listen(self, backlog=128):
        self._sock.listen(backlog)

    def setsockopt(self, *args):
        self._sock.setsockopt(*args)

    def getsockname(self):
        return self._sock.getsockname()

    def fileno(self):
        return self._sock.fileno()

    def close(self):
        self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
