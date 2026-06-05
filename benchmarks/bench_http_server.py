"""HTTP server benchmark: drive each server with oha, collect req/s + latency."""

import os

from benchmarks import _harness as h
from benchmarks import servers

SERVER_CONFIGS = [  # (display, launcher, workers)
    ("pyroutine", "pyroutine", os.cpu_count()),
    ("fastapi (1 worker)", "fastapi_1", 1),
    (f"fastapi ({os.cpu_count()} workers)", "fastapi_n", os.cpu_count()),
    ("flask (threaded)", "flask", 1),
    ("aiohttp", "aiohttp", 1),
]


def run(duration_s=5, connections=50, route="/json"):
    columns = ["name", "req_per_sec", "p50_ms", "p90_ms", "p99_ms", "success_rate"]
    if not h.oha_available():
        print(h.OHA_MISSING_HINT)
        return {"columns": columns, "rows": [], "skipped": True}
    rows = []
    for display, launcher, workers in SERVER_CONFIGS:
        port = h.free_port()
        url = f"http://127.0.0.1:{port}{route}"
        try:
            with h.managed_server(servers.server_cmd(launcher, port, workers),
                                  "127.0.0.1", port, ready_timeout=20.0):
                m = h.run_oha(url, duration_s=duration_s, connections=connections)
        except Exception as exc:  # a server that won't boot is recorded, not fatal
            m = {"req_per_sec": 0.0, "p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0,
                 "success_rate": 0.0, "error": str(exc)}
        m["name"] = display
        rows.append(m)
    return {"columns": columns, "rows": rows}


if __name__ == "__main__":
    s = run()
    if s.get("skipped"):
        print("server suite skipped (oha missing)")
    else:
        print(h.render_table("HTTP server (/json)", s["rows"], s["columns"]))
