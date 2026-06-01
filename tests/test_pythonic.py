"""Tests for pythonic concurrency APIs: spawn, gather, TaskGroup, and Queue."""

import pytest
import time
import sys

import pyroutine as pr


def test_spawn_returns_result():
    def square(x):
        return x * x

    t = pr.spawn(square, 7)
    assert t.result() == 49
    assert t.done is True


def test_spawn_exception_propagates():
    def boom():
        raise ValueError("kaboom")

    t = pr.spawn(boom)
    with pytest.raises(ValueError, match="kaboom"):
        t.result()


def test_gather():
    def mult(x, y):
        return x * y

    tasks = [pr.spawn(mult, i, 10) for i in range(5)]
    results = pr.gather(*tasks)
    assert results == [0, 10, 20, 30, 40]


def test_task_group_success():
    results = []

    def work(x):
        results.append(x * 2)

    with pr.TaskGroup() as tg:
        tg.spawn(work, 1)
        tg.spawn(work, 2)
        tg.spawn(work, 3)

    assert sorted(results) == [2, 4, 6]


def test_task_group_exception_propagation():
    def boom():
        raise ValueError("kaboom")

    def ok():
        return "ok"

    with pytest.raises(Exception if sys.version_info < (3, 11) else ExceptionGroup):
        with pr.TaskGroup() as tg:
            tg.spawn(boom)
            tg.spawn(ok)


def test_queue_basic():
    q = pr.Queue()
    assert q.empty() is True

    q.put(42)
    assert q.empty() is False
    assert q.qsize() == 1

    assert q.get() == 42
    assert q.empty() is True


def test_queue_blocking_cooperative():
    q = pr.Queue(maxsize=1)
    q.put("item1")
    assert q.full() is True

    got = []

    # This task will block on put because the queue is full
    def putter():
        q.put("item2")
        got.append("put_done")

    t = pr.spawn(putter)
    time.sleep(0.05)
    assert got == []  # putter is blocked

    assert q.get() == "item1"
    t.join()
    assert got == ["put_done"]
    assert q.get() == "item2"
