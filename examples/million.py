"""The headline: a million pyroutines parked at once, cheaply.

Spawns 1,000,000 pyroutines that each park on a Queue (holding no OS thread),
shows the thread count stays tiny, then releases them all.

    uv run --no-sync python examples/million.py
"""

import resource
import sys
import threading
import time

from pyroutine import spawn, gather, Queue


def rss_mb():
    # macOS reports ru_maxrss in bytes.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024


def main():
    assert not sys._is_gil_enabled(), "run me with python3.14t"

    n = 1_000_000
    q = Queue(maxsize=n)

    def parker():
        q.get()  # park here until fed a value

    base = rss_mb()
    t0 = time.monotonic()
    tasks = [spawn(parker) for _ in range(n)]
    print(f"spawned {n:,} pyroutines in {time.monotonic() - t0:.2f}s")

    time.sleep(2.0)  # let workers park them all
    print(
        f"while {n:,} are parked: {threading.active_count()} OS threads, "
        f"~{(rss_mb() - base) * 1024 / n:.2f} KB per pyroutine"
    )

    t0 = time.monotonic()
    for i in range(n):
        q.put(i)

    gather(*tasks)
    print(f"released + finished all {n:,} in {time.monotonic() - t0:.2f}s")


if __name__ == "__main__":
    main()
