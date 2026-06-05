"""Tests for Waiter.block() timeout support from within pyroutines.

These are regression tests for the bug where Waiter.block(timeout) silently
ignored the timeout parameter when called from a worker greenlet, causing
Queue.get(timeout=X), Queue.put(timeout=X), and Task.join(timeout=X) to
hang forever instead of timing out.
"""

import pyroutine as pr


def test_queue_get_timeout_from_pyroutine():
    """Queue.get(timeout) should raise TimeoutError inside a pyroutine."""
    q = pr.Queue()
    result = [None]

    def worker():
        try:
            q.get(timeout=0.2)
            result[0] = "got_item"
        except TimeoutError:
            result[0] = "timed_out"

    t = pr.spawn(worker)
    t.join()
    assert result[0] == "timed_out"


def test_queue_put_timeout_from_pyroutine():
    """Queue.put(timeout) should raise TimeoutError on a full bounded queue."""
    q = pr.Queue(maxsize=1)
    q.put("fill")  # Fill the queue
    result = [None]

    def worker():
        try:
            q.put("overflow", timeout=0.2)
            result[0] = "put_item"
        except TimeoutError:
            result[0] = "timed_out"

    t = pr.spawn(worker)
    t.join()
    assert result[0] == "timed_out"


def test_task_join_timeout_from_pyroutine():
    """Task.join(timeout) should return False when the task doesn't finish."""
    def slow():
        pr.sleep(10)  # Won't finish in time

    result = [None]

    def waiter():
        slow_task = pr.spawn(slow)
        finished = slow_task.join(timeout=0.2)
        result[0] = finished

    t = pr.spawn(waiter)
    t.join()
    assert result[0] is False


def test_task_result_timeout_from_pyroutine():
    """Task.result(timeout) should raise TimeoutError inside a pyroutine."""
    def slow():
        pr.sleep(10)

    result = [None]

    def waiter():
        slow_task = pr.spawn(slow)
        try:
            slow_task.result(timeout=0.2)
            result[0] = "got_result"
        except TimeoutError:
            result[0] = "timed_out"

    t = pr.spawn(waiter)
    t.join()
    assert result[0] == "timed_out"


def test_queue_get_timeout_succeeds_when_data_arrives():
    """Queue.get(timeout) should return the item if it arrives before timeout."""
    q = pr.Queue()
    result = [None]

    def producer():
        pr.sleep(0.05)
        q.put(42)

    def consumer():
        try:
            result[0] = q.get(timeout=5.0)
        except TimeoutError:
            result[0] = "timed_out"

    pr.spawn(producer)
    t = pr.spawn(consumer)
    t.join()
    assert result[0] == 42


def test_task_join_timeout_succeeds_when_task_finishes():
    """Task.join(timeout) should return True if the task finishes in time."""
    def fast():
        pr.sleep(0.05)
        return 99

    result = [None]

    def waiter():
        fast_task = pr.spawn(fast)
        result[0] = fast_task.join(timeout=5.0)

    t = pr.spawn(waiter)
    t.join()
    assert result[0] is True


def test_queue_get_zero_timeout_returns_immediately():
    """Queue.get(timeout=0) should return False immediately on an empty queue."""
    q = pr.Queue()
    result = [None]

    def worker():
        try:
            q.get(timeout=0)
            result[0] = "got_item"
        except TimeoutError:
            result[0] = "timed_out"

    t = pr.spawn(worker)
    t.join()
    assert result[0] == "timed_out"


def test_multiple_timed_waiters_on_same_queue():
    """Multiple pyroutines waiting on the same queue with timeouts."""
    q = pr.Queue()
    results = [None, None, None]

    def timed_getter(idx, timeout):
        try:
            results[idx] = q.get(timeout=timeout)
        except TimeoutError:
            results[idx] = "timed_out"

    # Spawn 3 consumers with different timeouts
    t1 = pr.spawn(timed_getter, 0, 0.3)
    t2 = pr.spawn(timed_getter, 1, 0.3)
    t3 = pr.spawn(timed_getter, 2, 0.3)

    # Only put 1 item — one consumer should get it, two should time out
    pr.sleep(0.05)
    q.put("hello")

    t1.join()
    t2.join()
    t3.join()

    got_item = sum(1 for r in results if r == "hello")
    timed_out = sum(1 for r in results if r == "timed_out")
    assert got_item == 1, f"Expected 1 consumer to get item, got {got_item}"
    assert timed_out == 2, f"Expected 2 consumers to time out, got {timed_out}"
