"""Spawn/memory scaling: max parked tasks + per-task cost. Isolated subprocesses.
    python -m benchmarks.bench_scaling                  # parent: run all
    python -m benchmarks.bench_scaling --contender X N   # child: emit JSON"""

import json
import resource
import subprocess
import sys
import threading
import time

CONTENDERS = ("pyroutine", "asyncio", "threads")
THREAD_CAP = 20_000


def _rss_kb():
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return raw / 1024 if sys.platform == "darwin" else raw  # macOS=bytes, Linux=KB


def _child_pyroutine(n):
    import pyroutine as pr
    q = pr.Queue(maxsize=n)
    base = _rss_kb(); t0 = time.monotonic()
    tasks = [pr.spawn(q.get) for _ in range(n)]
    spawn_s = time.monotonic() - t0
    time.sleep(1.0)
    used = _rss_kb() - base
    for _ in range(n): q.put(1)
    pr.gather(*tasks)
    return n, spawn_s, used


def _child_asyncio(n):
    import asyncio

    async def main():
        ev = asyncio.Event()
        base = _rss_kb(); t0 = time.monotonic()
        tasks = [asyncio.create_task(ev.wait()) for _ in range(n)]
        spawn_s = time.monotonic() - t0
        await asyncio.sleep(1.0)
        used = _rss_kb() - base
        ev.set(); await asyncio.gather(*tasks)
        return n, spawn_s, used

    return asyncio.run(main())


def _child_threads(n):
    n = min(n, THREAD_CAP)
    ev = threading.Event()
    base = _rss_kb(); t0 = time.monotonic()
    threads, reached = [], 0
    try:
        for _ in range(n):
            t = threading.Thread(target=ev.wait); t.start()
            threads.append(t); reached += 1
    except RuntimeError:
        pass  # OS thread ceiling
    spawn_s = time.monotonic() - t0
    time.sleep(1.0)
    used = _rss_kb() - base
    ev.set()
    for t in threads: t.join()
    return reached, spawn_s, used


_CHILDREN = {"pyroutine": _child_pyroutine, "asyncio": _child_asyncio, "threads": _child_threads}


def _run_child(contender, n):
    reached, spawn_s, used_kb = _CHILDREN[contender](n)
    print(json.dumps({"name": contender, "reached": reached, "spawn_s": spawn_s,
                      "kb_per_task": (used_kb / reached if reached else 0.0)}))


def run(n=1_000_000):
    rows = []
    for c in CONTENDERS:
        cmd = [sys.executable, "-m", "benchmarks.bench_scaling", "--contender", c, str(n)]
        out = subprocess.run(cmd, capture_output=True, text=True)
        line = out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""
        rows.append(json.loads(line) if line.startswith("{")
                    else {"name": c, "reached": 0, "spawn_s": 0.0, "kb_per_task": 0.0})
    return {"columns": ["name", "reached", "spawn_s", "kb_per_task"], "rows": rows}


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--contender", choices=CONTENDERS)
    p.add_argument("n", type=int, nargs="?", default=1_000_000)
    a = p.parse_args()
    if a.contender:
        _run_child(a.contender, a.n)
    else:
        from benchmarks._harness import render_table
        s = run(a.n)
        print(render_table("Spawn/memory scaling", s["rows"], s["columns"]))
