"""Shared primitives for the pyroutine benchmark suite."""

import contextlib
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time


# --- stats ----------------------------------------------------------------- #

def summarize(samples):
    data = sorted(samples)
    return {"median": statistics.median(data), "min": data[0], "max": data[-1], "n": len(data)}


def percentiles(samples):
    data = sorted(samples)
    if not data:
        return {"p50": 0.0, "p90": 0.0, "p99": 0.0}

    def rank(p):
        return data[max(0, min(len(data) - 1, int(round(p / 100 * len(data))) - 1))]

    return {"p50": rank(50), "p90": rank(90), "p99": rank(99)}


def repeat(fn, runs=5, warmup=1):
    """`warmup` discarded runs, then `runs` timed runs; return median seconds."""
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(runs):
        t0 = time.monotonic()
        fn()
        samples.append(time.monotonic() - t0)
    return statistics.median(samples)


def capture_env():
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    return {
        "python": sys.version.split()[0],
        "gil_enabled": gil,
        "cpu_count": os.cpu_count(),
        "platform": sys.platform,
        "note": "4 performance + 4 efficiency cores: CPU-parallel speedup caps ~4x, not 8x.",
    }


# --- ports + subprocess servers ------------------------------------------- #

def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]; s.close()
    return port


def wait_until_ready(host, port, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server at {host}:{port} not ready in {timeout}s")


@contextlib.contextmanager
def managed_server(cmd, host, port, ready_timeout=15.0):
    proc = subprocess.Popen(cmd)
    try:
        wait_until_ready(host, port, ready_timeout)
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait(timeout=5.0)


# --- oha ------------------------------------------------------------------- #

OHA_MISSING_HINT = (
    "oha not found. Install to run the HTTP server benchmark:\n"
    "    brew install oha   (macOS)   |   cargo install oha   (any platform)\n"
    "Skipping the server suite."
)


def oha_available():
    return shutil.which("oha") is not None


def parse_oha_json(text):
    """Parse `oha --json` stdout; oha reports latencies in seconds -> we emit ms."""
    data = json.loads(text)
    summary, pct = data.get("summary", {}), data.get("latencyPercentiles", {})
    # `x or 0.0` (not .get default): oha emits explicit null for percentiles when a
    # run produced no usable latency samples, and null * 1000 would raise.
    return {
        "req_per_sec": summary.get("requestsPerSec") or 0.0,
        "success_rate": summary.get("successRate") or 0.0,
        "p50_ms": (pct.get("p50") or 0.0) * 1000,
        "p90_ms": (pct.get("p90") or 0.0) * 1000,
        "p99_ms": (pct.get("p99") or 0.0) * 1000,
    }


def run_oha(url, duration_s=5, connections=50):
    if not oha_available():
        raise RuntimeError(OHA_MISSING_HINT)
    cmd = ["oha", "--no-tui", "--output-format", "json", "-z", f"{duration_s}s",
           "-c", str(connections), url]
    out = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return parse_oha_json(out.stdout)


# --- report emitters ------------------------------------------------------- #

def _fmt(v):
    return f"{v:,.2f}" if isinstance(v, float) else str(v)


def render_table(title, rows, columns):
    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            widths[c] = max(widths[c], len(_fmt(row.get(c, ""))))
    head = " | ".join(c.ljust(widths[c]) for c in columns)
    sep = "-+-".join("-" * widths[c] for c in columns)
    body = "\n".join(" | ".join(_fmt(r.get(c, "")).ljust(widths[c]) for c in columns) for r in rows)
    return f"== {title} ==\n{head}\n{sep}\n{body}"


def _md_table(rows, columns):
    head = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = "\n".join("| " + " | ".join(_fmt(r.get(c, "")) for c in columns) + " |" for r in rows)
    return "\n".join([head, sep, body])


def write_report(report, md_path, json_path):
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    parts = ["# pyroutine Benchmark Results", "", "## Environment", ""]
    parts += [f"- **{k}**: {v}" for k, v in report["env"].items()]
    parts.append("")
    for name, surface in report["surfaces"].items():
        parts += [f"## {name}", "", _md_table(surface["rows"], surface["columns"]), ""]
    with open(md_path, "w") as f:
        f.write("\n".join(parts))
