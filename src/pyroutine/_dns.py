"""Lightweight cooperative DNS client for pyroutine."""

import random
import socket
import struct

from ._net import Socket


def get_dns_servers() -> list[str]:
    """Parse DNS nameservers from /etc/resolv.conf, falling back to public DNS if needed."""
    servers = []
    try:
        with open("/etc/resolv.conf", "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("nameserver"):
                    parts = line.split()
                    if len(parts) >= 2:
                        servers.append(parts[1])
    except Exception:
        pass
    if not servers:
        servers = ["8.8.8.8", "1.1.1.1", "8.8.4.4"]
    return servers


def is_ipv4(host: str) -> bool:
    """Check if host is a valid IPv4 address."""
    try:
        socket.inet_aton(host)
        return True
    except OSError:
        return False


def is_ipv6(host: str) -> bool:
    """Check if host is a valid IPv6 address."""
    try:
        if hasattr(socket, "inet_pton"):
            socket.inet_pton(socket.AF_INET6, host)
            return True
    except (OSError, ValueError):
        pass
    return False


def resolve_hosts_file(host: str, family: int) -> list[tuple[str, int]]:
    """Check local /etc/hosts for the given host name, returning (ip, family) pairs."""
    results = []
    try:
        with open("/etc/hosts", "r") as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    ip = parts[0]
                    names = parts[1:]
                    if host.lower() in (n.lower() for n in names):
                        if is_ipv4(ip):
                            if family in (socket.AF_INET, socket.AF_UNSPEC):
                                results.append((ip, socket.AF_INET))
                        elif is_ipv6(ip):
                            if family in (socket.AF_INET6, socket.AF_UNSPEC):
                                results.append((ip, socket.AF_INET6))
    except Exception:
        pass
    return results


def skip_name(response: bytes, offset: int) -> int:
    """Skip a label-encoded domain name in DNS response, returning the new offset."""
    while True:
        if offset >= len(response):
            raise OSError("Malformed DNS response: offset out of bounds")
        length = response[offset]
        if (length & 0xC0) == 0xC0:
            if offset + 2 > len(response):
                raise OSError("Malformed DNS response: compression pointer truncated")
            return offset + 2
        elif length == 0:
            return offset + 1
        else:
            offset += 1 + length


def parse_dns_response(response: bytes, expected_tx_id: int, family: int) -> list[str]:
    """Parse a DNS response packet and return a list of IP addresses."""
    if len(response) < 12:
        raise OSError("Invalid DNS response: too short")

    transaction_id, flags, qdcount, ancount, nscount, arcount = struct.unpack(
        "!HHHHHH", response[:12]
    )

    if transaction_id != expected_tx_id:
        raise OSError("DNS response transaction ID mismatch")

    # If flags indicate truncation, we fail UDP lookup to trigger TCP fallback
    if flags & 0x0200:
        raise OSError("Truncated DNS response")

    rcode = flags & 0x000F
    if rcode != 0:
        raise OSError(f"DNS query failed with rcode {rcode}")

    offset = 12
    # Skip Question section
    for _ in range(qdcount):
        offset = skip_name(response, offset)
        if offset + 4 > len(response):
            raise OSError("Malformed DNS response in Question metadata")
        offset += 4

    ips = []
    # Parse Answer section
    for _ in range(ancount):
        offset = skip_name(response, offset)

        if offset + 10 > len(response):
            raise OSError("Malformed DNS response in Answer header")

        rtype, rclass, ttl, rdlength = struct.unpack(
            "!HHIH", response[offset : offset + 10]
        )
        offset += 10

        if offset + rdlength > len(response):
            raise OSError("Malformed DNS response in Answer RDATA")

        rdata = response[offset : offset + rdlength]
        offset += rdlength

        # We look for A (type=1) or AAAA (type=28) records with class IN (1)
        if rclass == 1:
            if family == socket.AF_INET and rtype == 1 and rdlength == 4:
                ip = f"{rdata[0]}.{rdata[1]}.{rdata[2]}.{rdata[3]}"
                ips.append(ip)
            elif family == socket.AF_INET6 and rtype == 28 and rdlength == 16:
                ip = socket.inet_ntop(socket.AF_INET6, rdata)
                ips.append(ip)

    return ips


def build_dns_query(hostname: str, family: int) -> tuple[bytes, int]:
    """Build a standard DNS query packet for A or AAAA records."""
    tx_id = random.randint(0, 65535)
    # Header: Transaction ID + Flags (0x0100 recursion desired) + QDCOUNT (1)
    header = struct.pack("!HHHHHH", tx_id, 0x0100, 1, 0, 0, 0)

    question = bytearray()
    try:
        idna_host = hostname.encode("idna").decode("ascii")
    except Exception:
        idna_host = hostname

    for part in idna_host.split("."):
        if not part:
            continue
        part_bytes = part.encode("ascii")
        question.append(len(part_bytes))
        question.extend(part_bytes)
    question.append(0)

    # QTYPE = 1 (A record) or 28 (AAAA record)
    qtype = 1 if family == socket.AF_INET else 28
    # QCLASS = 1 (IN class)
    question.extend(struct.pack("!HH", qtype, 1))
    return bytes(header + question), tx_id


def resolve_via_dns(host: str, family: int, dns_servers: list[str]) -> list[str]:
    """Query nameservers using UDP first, falling back to TCP if the response is truncated."""
    query_packet, tx_id = build_dns_query(host, family)

    for server in dns_servers:
        port = 53
        if ":" in server:
            parts = server.rsplit(":", 1)
            server_ip = parts[0]
            try:
                port = int(parts[1])
            except ValueError:
                pass
        else:
            server_ip = server

        # 1. Try UDP first
        use_tcp = False
        try:
            with Socket(family=socket.AF_INET, type=socket.SOCK_DGRAM) as sock:
                sock.settimeout(2.0)
                sock.sendto(query_packet, (server_ip, port))
                response, _ = sock.recvfrom(512)

                # Check if truncated flag is set
                if len(response) >= 12:
                    _, flags, *__ = struct.unpack("!HHHHHH", response[:12])
                    if flags & 0x0200:
                        use_tcp = True

                if not use_tcp:
                    ips = parse_dns_response(response, tx_id, family)
                    if ips:
                        return ips
        except OSError as e:
            if "truncated" in str(e).lower() or "truncation" in str(e).lower():
                use_tcp = True
            else:
                continue
        except Exception:
            continue

        # 2. Try TCP Fallback
        if use_tcp:
            try:
                with Socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
                    sock.settimeout(5.0)
                    sock.connect((server_ip, port))

                    # Packet format over TCP: 2-byte length prefix + payload
                    tcp_query = struct.pack("!H", len(query_packet)) + query_packet
                    sock.sendall(tcp_query)

                    # Read 2-byte response length
                    len_bytes = b""
                    while len(len_bytes) < 2:
                        chunk = sock.recv(2 - len(len_bytes))
                        if not chunk:
                            raise OSError("TCP DNS connection closed prematurely")
                        len_bytes += chunk
                    msg_len = struct.unpack("!H", len_bytes)[0]

                    # Read payload
                    response = b""
                    while len(response) < msg_len:
                        chunk = sock.recv(msg_len - len(response))
                        if not chunk:
                            raise OSError("TCP DNS connection closed before full message received")
                        response += chunk

                    # Clear the Truncated bit (TC) in flags if the server kept it set
                    if len(response) >= 12:
                        header = list(struct.unpack("!HHHHHH", response[:12]))
                        if header[1] & 0x0200:
                            header[1] &= ~0x0200
                        response = struct.pack("!HHHHHH", *header) + response[12:]

                    ips = parse_dns_response(response, tx_id, family)
                    if ips:
                        return ips
            except Exception:
                continue

    raise socket.gaierror(socket.EAI_NONAME, f"Name or service not known: {host}")


def cooperative_getaddrinfo(
    host, port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM, proto=0, flags=0
):
    """Resolve address info cooperatively supporting AF_INET, AF_INET6 and AF_UNSPEC."""
    if family not in (socket.AF_INET, socket.AF_INET6, socket.AF_UNSPEC):
        raise socket.gaierror(
            socket.EAI_FAMILY, "Address family for hostname not supported"
        )

    results = []

    # 1. Check IP addresses directly
    if is_ipv4(host):
        if family in (socket.AF_INET, socket.AF_UNSPEC):
            results.append(
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port))
            )
        if not results:
            raise socket.gaierror(
                socket.EAI_ADDRFAMILY, "Address family for hostname not supported"
            )
        return results

    if is_ipv6(host):
        if family in (socket.AF_INET6, socket.AF_UNSPEC):
            results.append(
                (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port, 0, 0))
            )
        if not results:
            raise socket.gaierror(
                socket.EAI_ADDRFAMILY, "Address family for hostname not supported"
            )
        return results

    # 2. Check localhost
    if host.lower() == "localhost":
        if family in (socket.AF_INET, socket.AF_UNSPEC):
            results.append(
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", port))
            )
        if family in (socket.AF_INET6, socket.AF_UNSPEC):
            results.append(
                (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", port, 0, 0))
            )
        return results

    # 3. Check hosts file
    hosts_results = resolve_hosts_file(host, family)
    if hosts_results:
        for ip, fam in hosts_results:
            if fam == socket.AF_INET:
                results.append(
                    (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))
                )
            else:
                results.append(
                    (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port, 0, 0))
                )
        return results

    # 4. Resolve using DNS
    dns_servers = get_dns_servers()

    families_to_query = []
    if family == socket.AF_INET:
        families_to_query = [socket.AF_INET]
    elif family == socket.AF_INET6:
        families_to_query = [socket.AF_INET6]
    else:  # AF_UNSPEC
        families_to_query = [socket.AF_INET, socket.AF_INET6]

    errors = []
    for fam in families_to_query:
        try:
            ips = resolve_via_dns(host, fam, dns_servers)
            for ip in ips:
                if fam == socket.AF_INET:
                    results.append(
                        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))
                    )
                else:
                    results.append(
                        (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port, 0, 0))
                    )
        except socket.gaierror as e:
            errors.append(e)
        except Exception as e:
            errors.append(socket.gaierror(socket.EAI_NONAME, str(e)))

    if not results:
        if errors:
            raise errors[0]
        raise socket.gaierror(socket.EAI_NONAME, f"Name or service not known: {host}")

    return results
