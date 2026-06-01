"""Netpoller: pyroutines doing socket I/O park on the poller (holding no worker),
and wake when the fd is ready — so many connections share a few OS threads."""

import socket
import threading
import time

from pyroutine import spawn, gather, Socket, TaskGroup


def test_socketpair_park_and_wake():
    a, b = socket.socketpair()
    sa = Socket(_sock=a)
    got = []

    def reader():
        got.append(sa.recv(64))  # parks on the netpoller until data arrives

    h = spawn(reader)
    time.sleep(0.05)  # reader should be parked, nothing received yet
    assert got == []
    b.sendall(b"hello")
    h.join()
    assert got == [b"hello"]
    b.close()


def test_many_concurrent_socket_reads_few_threads():
    n = 400
    pairs = [socket.socketpair() for _ in range(n)]
    results = [None] * n

    def reader(i, sock):
        results[i] = sock.recv(64)

    tasks = []
    for i, (a, _b) in enumerate(pairs):
        tasks.append(spawn(reader, i, Socket(_sock=a)))

    time.sleep(0.2)  # all readers now parked on the single poller
    peak_threads = threading.active_count()

    for i, (_a, b) in enumerate(pairs):
        b.sendall(f"msg{i}".encode())

    gather(*tasks)

    assert results == [f"msg{i}".encode() for i in range(n)]
    # 400 connections parked, yet thread count stays tiny (workers + poller + main).
    assert peak_threads < 30, peak_threads
    for a, b in pairs:
        a.close()
        b.close()


def test_tcp_echo_server_and_clients():
    server = Socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen()
    host, port = server.getsockname()

    n_clients = 50
    replies = [None] * n_clients

    def handle(conn):
        data = conn.recv(1024)
        conn.sendall(data)  # echo
        conn.close()

    def acceptor():
        for _ in range(n_clients):
            conn, _addr = server.accept()
            spawn(handle, conn)

    spawn(acceptor)

    with TaskGroup() as tg:

        def client(i):
            s = Socket()
            s.connect((host, port))
            s.sendall(f"ping-{i}".encode())
            replies[i] = s.recv(1024)
            s.close()

        for i in range(n_clients):
            tg.spawn(client, i)

    server.close()
    assert replies == [f"ping-{i}".encode() for i in range(n_clients)]
