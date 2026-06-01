"""Scheduler core: a dedicated Scheduler runs N >> P tasks to completion and
balances them across processors via the global queue + work-stealing."""

import threading
import time

from pyroutine import Scheduler


def test_runs_all_submitted_callables():
    sched = Scheduler(nprocs=4)
    sched.start()
    try:
        counter = 0
        lock = threading.Lock()
        n = 5000

        def task():
            nonlocal counter
            with lock:
                counter += 1

        for _ in range(n):
            sched.submit(task)
        assert sched.wait_idle(timeout=10)
        assert counter == n
    finally:
        sched.shutdown(wait=False)


def test_work_spreads_across_workers():
    sched = Scheduler(nprocs=8)
    sched.start()
    try:
        seen = set()
        lock = threading.Lock()

        def task():
            with lock:
                seen.add(threading.current_thread().name)
            time.sleep(0.01)  # hold the worker so others must serve the rest

        for _ in range(200):
            sched.submit(task)
        assert sched.wait_idle(timeout=10)
        # Work ran on more than one worker hub (true parallelism).
        assert len(seen) >= 2
    finally:
        sched.shutdown(wait=False)
