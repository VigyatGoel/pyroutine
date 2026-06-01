"""A cooperative TCP echo server + many clients, all on a handful of OS threads.

Each connection is a pyroutine parked on the netpoller while it waits for I/O.

    uv run --no-sync python examples/echo_server.py
"""

import socket
import sys
import threading
import time

from pyroutine import spawn, Socket, TaskGroup


def main():
    assert not sys._is_gil_enabled(), "run me with python3.14t"

    server = Socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1024)
    host, port = server.getsockname()

    n_clients = 1000
    ok = [0]
    ok_lock = threading.Lock()

    def handle(conn):
        data = conn.recv(1024)  # parks until bytes arrive
        conn.sendall(data)  # echo back
        conn.close()

    def acceptor():
        for _ in range(n_clients):
            conn, _addr = server.accept()  # parks until a client connects
            spawn(handle, conn)

    spawn(acceptor)

    start = time.monotonic()

    with TaskGroup() as tg:

        def client(i):
            s = Socket()
            s.connect((host, port))
            s.sendall(f"hello-{i}".encode())
            reply = s.recv(1024)
            s.close()
            if reply == f"hello-{i}".encode():
                with ok_lock:
                    ok[0] += 1

        for i in range(n_clients):
            tg.spawn(client, i)

    time.sleep(0.05)
    peak_threads = threading.active_count()

    server.close()
    print(
        f"{n_clients} clients echoed correctly: {ok[0] == n_clients} ({ok[0]}/{n_clients})"
    )
    print(
        f"in {time.monotonic() - start:.2f}s on {peak_threads} OS threads "
        f"(1-thread-per-connection would need ~{n_clients})"
    )


if __name__ == "__main__":
    main()
