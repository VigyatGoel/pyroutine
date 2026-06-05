"""HTTP client throughput vs a fixed neutral server. Each client in its own process.
    python -m benchmarks.bench_http_client
    python -m benchmarks.bench_http_client --contender X URL TOTAL CONC"""

import http.server
import json
import socketserver
import subprocess
import sys
import threading
import time

from benchmarks._harness import free_port

CONTENDERS = ("pyroutine", "httpx_sync", "httpx_async", "requests", "aiohttp")


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a): pass


class _Srv(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 1024  # large listen backlog: fresh-connection clients
                               # (pyroutine.http.get, requests) open many sockets at
                               # once; the default of 5 overflows and connects time out


def _start_server(port):
    srv = _Srv(("127.0.0.1", port), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def _client_pyroutine(url, total, conc):
    import pyroutine as pr
    per = max(1, total // conc)

    def worker():
        for _ in range(per):
            pr.http.get(url)

    t0 = time.monotonic()
    pr.gather(*[pr.spawn(worker) for _ in range(conc)])
    return per * conc, time.monotonic() - t0


def _pool_client(get_one, total, conc):
    from concurrent.futures import ThreadPoolExecutor
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=conc) as ex:
        list(ex.map(lambda _: get_one(), range(total)))
    return total, time.monotonic() - t0


def _client_requests(url, total, conc):
    import requests
    return _pool_client(lambda: requests.get(url), total, conc)


def _client_httpx_sync(url, total, conc):
    import httpx
    client = httpx.Client()
    try:
        return _pool_client(lambda: client.get(url), total, conc)
    finally:
        client.close()


def _client_httpx_async(url, total, conc):
    import asyncio, httpx

    async def main():
        sem = asyncio.Semaphore(conc)
        async with httpx.AsyncClient() as c:
            async def one():
                async with sem:
                    await c.get(url)
            t0 = time.monotonic()
            await asyncio.gather(*(one() for _ in range(total)))
            return total, time.monotonic() - t0

    return asyncio.run(main())


def _client_aiohttp(url, total, conc):
    import asyncio, aiohttp

    async def main():
        sem = asyncio.Semaphore(conc)
        async with aiohttp.ClientSession() as s:
            async def one():
                async with sem:
                    async with s.get(url) as r:
                        await r.read()
            t0 = time.monotonic()
            await asyncio.gather(*(one() for _ in range(total)))
            return total, time.monotonic() - t0

    return asyncio.run(main())


_CLIENTS = {"pyroutine": _client_pyroutine, "httpx_sync": _client_httpx_sync,
            "httpx_async": _client_httpx_async, "requests": _client_requests,
            "aiohttp": _client_aiohttp}


def _run_child(contender, url, total, conc):
    done, elapsed = _CLIENTS[contender](url, total, conc)
    print(json.dumps({"name": contender, "requests": done, "seconds": elapsed,
                      "req_per_sec": (done / elapsed if elapsed else 0.0)}))


def _one_run(contender, url, total, conc):
    cmd = [sys.executable, "-m", "benchmarks.bench_http_client",
           "--contender", contender, url, str(total), str(conc)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    line = out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""
    return (json.loads(line) if line.startswith("{")
            else {"name": contender, "requests": 0, "seconds": 0.0, "req_per_sec": 0.0})


def run(total=10_000, concurrency=50, runs=3, warmup=1):
    """Each contender: `warmup` discarded launches + `runs` launches; median req/s row."""
    port = free_port()
    srv = _start_server(port)
    url = f"http://127.0.0.1:{port}/"
    rows = []
    try:
        for c in CONTENDERS:
            for _ in range(warmup):
                _one_run(c, url, total, concurrency)
            res = sorted((_one_run(c, url, total, concurrency) for _ in range(runs)),
                         key=lambda d: d["req_per_sec"])
            rows.append(res[len(res) // 2])
    finally:
        srv.shutdown()
    return {"columns": ["name", "requests", "seconds", "req_per_sec"], "rows": rows}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--contender", choices=CONTENDERS)
    p.add_argument("url", nargs="?")
    p.add_argument("total", type=int, nargs="?", default=10_000)
    p.add_argument("conc", type=int, nargs="?", default=50)
    a = p.parse_args()
    if a.contender:
        _run_child(a.contender, a.url, a.total, a.conc)
    else:
        from benchmarks._harness import render_table
        s = run()
        print(render_table("HTTP client", s["rows"], s["columns"]))
