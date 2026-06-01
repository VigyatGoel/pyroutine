"""Cooperative parking: many pyroutines sleeping/parking run concurrently while
holding no OS threads; run_blocking offloads uninstrumented blocking calls."""

import threading
import time

from pyroutine import spawn, gather, sleep, run_blocking, Queue


def test_cooperative_sleep_does_not_consume_threads():
    # Far more sleeping pyroutines than workers: with cooperative sleep they all
    # park and run concurrently, so wall-clock stays near a single sleep.
    n = 500
    dur = 0.1

    def napper():
        sleep(dur)

    threads_before = threading.active_count()
    start = time.monotonic()

    tasks = [spawn(napper) for _ in range(n)]
    gather(*tasks)

    elapsed = time.monotonic() - start
    threads_peak = threading.active_count()

    assert elapsed < dur * 5, f"took {elapsed:.2f}s (serial would be {n * dur:.0f}s)"
    # No thread-per-pyroutine: worker count stays tiny (~nprocs), not ~500.
    assert threads_peak - threads_before < 20


def test_run_blocking_offloads_uninstrumented_calls():
    n = 40
    dur = 0.1

    def blocker():
        run_blocking(time.sleep, dur)  # offloaded to the helper pool

    start = time.monotonic()
    tasks = [spawn(blocker) for _ in range(n)]
    gather(*tasks)

    elapsed = time.monotonic() - start
    assert elapsed < n * dur / 4, f"took {elapsed:.2f}s"


def test_thousands_parked_on_queue_then_released():
    # Park thousands of pyroutines blocked on a get; they cost ~nothing and all
    # wake when puts arrive.
    n = 5000
    q_in = Queue()
    q_out = Queue(maxsize=n)

    def consumer():
        v = q_in.get()
        q_out.put(v * 2)

    tasks = [spawn(consumer) for _ in range(n)]

    threads_peak = threading.active_count()

    def feeder():
        for i in range(n):
            q_in.put(i)

    spawn(feeder).join()
    gather(*tasks)

    results = [q_out.get() for _ in range(n)]
    assert sum(results) == sum(i * 2 for i in range(n))
    # Even with 5000 pyroutines parked, the thread count is bounded by workers.
    assert threads_peak < 50
