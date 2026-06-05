"""Tests for previously untested public APIs and important code paths.

Covers: yield_(), sleep(0), set_max_procs() error case, run_blocking with
return values and exceptions, Queue.get() blocking on empty, Task.join()
directly, and multi-producer/consumer Queue coordination.
"""

import time
import threading

import pyroutine as pr


# -- yield_() ----------------------------------------------------------------


def test_yield_does_not_block():
    """yield_() should return promptly without blocking or deadlocking."""

    def worker():
        for _ in range(100):
            pr.yield_()
        return "done"

    t = pr.spawn(worker)
    assert t.result(timeout=5) == "done"


def test_yield_off_worker_is_noop():
    """yield_() called from the main thread (off-worker) should be a no-op."""
    # Should not raise or block
    pr.yield_()


# -- sleep(0) ----------------------------------------------------------------


def test_sleep_zero_yields_without_blocking():
    """sleep(0) should yield to other pyroutines like yield_()."""
    completed = [False]

    def worker():
        pr.sleep(0)
        completed[0] = True

    t = pr.spawn(worker)
    t.join()
    assert completed[0] is True


def test_sleep_negative_yields_without_blocking():
    """sleep with a negative value should behave like sleep(0) / yield_()."""
    completed = [False]

    def worker():
        pr.sleep(-1)
        completed[0] = True

    t = pr.spawn(worker)
    t.join()
    assert completed[0] is True


# -- run_blocking ------------------------------------------------------------


def test_run_blocking_returns_value():
    """run_blocking should return the value from the blocking function."""

    def blocking_fn():
        time.sleep(0.01)
        return 42

    result = [None]

    def worker():
        result[0] = pr.run_blocking(blocking_fn)

    t = pr.spawn(worker)
    t.join()
    assert result[0] == 42


def test_run_blocking_propagates_exception():
    """run_blocking should propagate exceptions from the blocking function."""

    def failing_fn():
        raise ValueError("test error")

    result = [None]

    def worker():
        try:
            pr.run_blocking(failing_fn)
        except ValueError as e:
            result[0] = str(e)

    t = pr.spawn(worker)
    t.join()
    assert result[0] == "test error"


def test_run_blocking_off_worker_calls_directly():
    """run_blocking off-worker should call the function directly."""
    result = pr.run_blocking(lambda: 99)
    assert result == 99


# -- Task.join() -------------------------------------------------------------


def test_task_join_returns_true():
    """Task.join() should return True when the task finishes."""

    def fast():
        return 42

    t = pr.spawn(fast)
    assert t.join() is True
    assert t.done is True
    assert t.result() == 42


def test_task_join_on_already_done_task():
    """Task.join() on an already-done task should return True immediately."""

    def fast():
        return 1

    t = pr.spawn(fast)
    t.join()  # Wait for completion
    assert t.join() is True  # Already done, should be instant


# -- Queue.get() blocking on empty -------------------------------------------


def test_queue_get_blocks_on_empty_until_item():
    """Queue.get() should block cooperatively when empty and wake on put."""
    q = pr.Queue()
    result = [None]

    def producer():
        pr.sleep(0.1)
        q.put(99)

    def consumer():
        result[0] = q.get()  # Should block until producer puts

    pr.spawn(producer)
    t = pr.spawn(consumer)
    t.join()
    assert result[0] == 99


# -- Queue multi-producer/consumer ------------------------------------------


def test_queue_multiple_producers_consumers():
    """Multiple producers and consumers should coordinate correctly."""
    q = pr.Queue()
    n_per_producer = 25
    n_producers = 4
    n_consumers = 2
    n_total = n_producers * n_per_producer
    n_per_consumer = n_total // n_consumers

    results = []
    lock = threading.Lock()

    def producer(start, count):
        for i in range(start, start + count):
            q.put(i)

    def consumer(count):
        for _ in range(count):
            item = q.get()
            with lock:
                results.append(item)

    with pr.TaskGroup() as tg:
        for i in range(n_producers):
            tg.spawn(producer, i * n_per_producer, n_per_producer)
        for _ in range(n_consumers):
            tg.spawn(consumer, n_per_consumer)

    assert sorted(results) == list(range(n_total))


# -- set_max_procs() ---------------------------------------------------------


def test_set_max_procs_errors_after_scheduler_started():
    """set_max_procs() should raise RuntimeError after the first spawn."""
    # Ensure the global scheduler is started (other tests will have spawned)
    pr.spawn(lambda: None).join()

    try:
        pr.set_max_procs(2)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as e:
        assert "before" in str(e).lower()


# -- sleep off-worker --------------------------------------------------------


def test_sleep_off_worker_uses_real_sleep():
    """sleep() off-worker should fall back to time.sleep."""
    start = time.monotonic()
    pr.sleep(0.1)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.09, f"Expected ~0.1s sleep, got {elapsed:.3f}s"
