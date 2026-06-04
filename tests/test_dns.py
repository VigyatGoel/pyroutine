"""Tests for pyroutine cooperative DNS resolution."""

import socket
import struct
import threading
from unittest.mock import patch, mock_open

import pytest
import pyroutine as pr
from pyroutine import spawn
from pyroutine._dns import cooperative_getaddrinfo


# ---------- Dual UDP/TCP Mock DNS Server ----------


def udp_dns_server_loop(udp_sock, response_ip="9.9.9.9", response_ipv6="2001:db8::9"):
    try:
        while True:
            data, addr = udp_sock.recvfrom(512)
            if not data:
                break
            tx_id = data[:2]

            # If the request domain contains "truncated", set the Truncated bit (TC) in flags (0x8380)
            # and return no answer to trigger TCP fallback.
            if b"truncated" in data:
                response = (
                    tx_id
                    + struct.pack("!HHHHH", 0x8380, 1, 0, 0, 0)
                    + data[12:]
                )
                udp_sock.sendto(response, addr)
                continue

            # Check if query is for A (1) or AAAA (28)
            # Last 4 bytes of question contain QTYPE and QCLASS
            is_aaaa = data.endswith(struct.pack("!HH", 28, 1))

            if is_aaaa:
                # AAAA Response
                response = (
                    tx_id
                    + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
                    + data[12:]
                    + struct.pack("!HHHIH", 0xC00C, 28, 1, 300, 16)
                    + socket.inet_pton(socket.AF_INET6, response_ipv6)
                )
            else:
                # A Response
                response = (
                    tx_id
                    + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
                    + data[12:]
                    + struct.pack("!HHHIH", 0xC00C, 1, 1, 300, 4)
                    + socket.inet_aton(response_ip)
                )
            udp_sock.sendto(response, addr)
    except Exception:
        pass


def tcp_dns_server_loop(tcp_sock, response_ip="9.9.9.9", response_ipv6="2001:db8::9"):
    tcp_sock.listen(5)
    try:
        while True:
            conn, addr = tcp_sock.accept()
            try:
                # Read 2 bytes length prefix
                len_bytes = conn.recv(2)
                if len(len_bytes) < 2:
                    conn.close()
                    continue
                msg_len = struct.unpack("!H", len_bytes)[0]

                # Read message
                data = b""
                while len(data) < msg_len:
                    chunk = conn.recv(msg_len - len(data))
                    if not chunk:
                        break
                    data += chunk

                if len(data) < msg_len:
                    conn.close()
                    continue

                tx_id = data[:2]
                is_aaaa = data.endswith(struct.pack("!HH", 28, 1))

                if is_aaaa:
                    payload = (
                        tx_id
                        + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
                        + data[12:]
                        + struct.pack("!HHHIH", 0xC00C, 28, 1, 300, 16)
                        + socket.inet_pton(socket.AF_INET6, response_ipv6)
                    )
                else:
                    payload = (
                        tx_id
                        + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
                        + data[12:]
                        + struct.pack("!HHHIH", 0xC00C, 1, 1, 300, 4)
                        + socket.inet_aton(response_ip)
                    )

                response = struct.pack("!H", len(payload)) + payload
                conn.sendall(response)
            except Exception:
                pass
            finally:
                conn.close()
    except Exception:
        pass


@pytest.fixture(scope="module")
def mock_dns_server():
    # Bind TCP to get ephemeral port
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind(("127.0.0.1", 0))
    _, port = tcp_sock.getsockname()

    # Bind UDP to exact same port
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind(("127.0.0.1", port))

    t_udp = threading.Thread(target=udp_dns_server_loop, args=(udp_sock,), daemon=True)
    t_tcp = threading.Thread(target=tcp_dns_server_loop, args=(tcp_sock,), daemon=True)
    
    t_udp.start()
    t_tcp.start()

    yield f"127.0.0.1:{port}"
    
    udp_sock.close()
    tcp_sock.close()


# ---------- DNS Resolver Tests ----------


def test_resolve_ip_directly():
    def runner():
        # IPv4
        addrinfo = cooperative_getaddrinfo("8.8.8.8", 80, socket.AF_INET)
        assert len(addrinfo) == 1
        family, socktype, proto, canonname, sockaddr = addrinfo[0]
        assert sockaddr == ("8.8.8.8", 80)
        assert family == socket.AF_INET

        # IPv6
        addrinfo_v6 = cooperative_getaddrinfo("2001:db8::1", 80, socket.AF_INET6)
        assert len(addrinfo_v6) == 1
        family, socktype, proto, canonname, sockaddr = addrinfo_v6[0]
        assert sockaddr == ("2001:db8::1", 80, 0, 0)
        assert family == socket.AF_INET6

    spawn(runner).result()


def test_resolve_localhost():
    def runner():
        # AF_INET -> IPv4
        addrinfo = cooperative_getaddrinfo("localhost", 443, socket.AF_INET)
        assert len(addrinfo) == 1
        assert addrinfo[0][0] == socket.AF_INET
        assert addrinfo[0][4] == ("127.0.0.1", 443)

        # AF_INET6 -> IPv6
        addrinfo_v6 = cooperative_getaddrinfo("localhost", 443, socket.AF_INET6)
        assert len(addrinfo_v6) == 1
        assert addrinfo_v6[0][0] == socket.AF_INET6
        assert addrinfo_v6[0][4] == ("::1", 443, 0, 0)

        # AF_UNSPEC -> both IPv4 and IPv6
        addrinfo_both = cooperative_getaddrinfo("localhost", 443, socket.AF_UNSPEC)
        assert len(addrinfo_both) == 2
        families = [a[0] for a in addrinfo_both]
        assert socket.AF_INET in families
        assert socket.AF_INET6 in families

    spawn(runner).result()


def test_resolve_hosts_file():
    mock_hosts_content = (
        "192.168.99.99   test.localtest.invalid\n"
        "2001:db8::99    test.localtest.invalid\n"
    )

    def runner():
        with patch("builtins.open", mock_open(read_data=mock_hosts_content)):
            # AF_INET
            addrinfo = cooperative_getaddrinfo("test.localtest.invalid", 80, socket.AF_INET)
            assert len(addrinfo) == 1
            assert addrinfo[0][4] == ("192.168.99.99", 80)

            # AF_INET6
            addrinfo_v6 = cooperative_getaddrinfo("test.localtest.invalid", 80, socket.AF_INET6)
            assert len(addrinfo_v6) == 1
            assert addrinfo_v6[0][4] == ("2001:db8::99", 80, 0, 0)

    spawn(runner).result()


def test_cooperative_dns_lookup_ipv4_and_ipv6(mock_dns_server):
    def runner():
        with patch("pyroutine._dns.get_dns_servers", return_value=[mock_dns_server]):
            # Query A record (IPv4)
            addrinfo = cooperative_getaddrinfo("example.dns", 80, socket.AF_INET)
            assert len(addrinfo) == 1
            assert addrinfo[0][4] == ("9.9.9.9", 80)

            # Query AAAA record (IPv6)
            addrinfo_v6 = cooperative_getaddrinfo("example.dns", 80, socket.AF_INET6)
            assert len(addrinfo_v6) == 1
            assert addrinfo_v6[0][4] == ("2001:db8::9", 80, 0, 0)

            # Query UNSPEC
            addrinfo_both = cooperative_getaddrinfo("example.dns", 80, socket.AF_UNSPEC)
            assert len(addrinfo_both) >= 2

    spawn(runner).result()


def test_tcp_fallback_on_truncation(mock_dns_server):
    def runner():
        with patch("pyroutine._dns.get_dns_servers", return_value=[mock_dns_server]):
            # Query domain with "truncated" to trigger UDP TC response -> TCP Fallback
            addrinfo = cooperative_getaddrinfo("truncated-host.dns", 80, socket.AF_INET)
            assert len(addrinfo) == 1
            # TCP server returns 9.9.9.9 successfully
            assert addrinfo[0][4] == ("9.9.9.9", 80)

    spawn(runner).result()


def test_resolve_unknown_host():
    def runner():
        with patch("pyroutine._dns.get_dns_servers", return_value=["127.0.0.1:0"]):
            with pytest.raises(socket.gaierror) as excinfo:
                cooperative_getaddrinfo("invalid.domain.xyz.xyz", 80)
            assert excinfo.value.errno == socket.EAI_NONAME

    spawn(runner).result()
