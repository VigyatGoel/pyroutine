"""Fan-out / fan-in: spawn many pyroutines and collect their results via gather.

Run with the free-threaded interpreter:

    uv run --no-sync python examples/fanout.py
"""

import sys
import time

import pyroutine as pr


def work(n):
    return sum(i * i for i in range(n))  # CPU-bound


def main():
    assert not sys._is_gil_enabled(), "run me with python3.14t for real parallelism"

    jobs = 16
    size = 2_000_000

    start = time.monotonic()

    # Spawn tasks concurrently
    tasks = [pr.spawn(work, size) for _ in range(jobs)]

    # Gather results
    results = pr.gather(*tasks)

    elapsed = time.monotonic() - start
    print(f"{jobs} CPU-bound pyroutines finished in {elapsed:.3f}s")
    print(
        f"first result = {results[0]}, all equal = {all(r == results[0] for r in results)}"
    )


if __name__ == "__main__":
    main()
