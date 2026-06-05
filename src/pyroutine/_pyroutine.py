"""Structured concurrency API: Task, spawn, gather, and TaskGroup.

Instead of Go-style decorators and channels, this implements clean,
standard Python task management and structured concurrency.
"""

import sys
import threading
import traceback

from ._runtime import Waiter, get_scheduler


class Task:
    """A pyroutine task. Runs as a greenlet bound to its worker.

    Acts as both the runnable unit for the scheduler and the handle
    returned to the user to wait for results.
    """

    __slots__ = (
        "fn",
        "args",
        "kwargs",
        "value",
        "exc",
        "tb",
        "done",
        "sched",
        "wid",
        "gr",
        "_joiners",
        "_jlock",
        "_retrieved",
        "_stealable",
    )

    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.value = None
        self.exc = None
        self.tb = None
        self.done = False
        self.sched = None
        self.wid = None
        self.gr = None
        self._joiners = []
        self._jlock = threading.Lock()
        self._retrieved = False
        self._stealable = True

    def _run(self):
        """Executed by the scheduler to run the task."""
        try:
            self.value = self.fn(*self.args, **self.kwargs)
        except BaseException as e:  # noqa: BLE001 - captured for the Task Handle
            self.exc = e
            self.tb = traceback.format_exception(type(e), e, e.__traceback__)
        finally:
            with self._jlock:
                self.done = True
                joiners = self._joiners
                self._joiners = []
            for w in joiners:
                w.wake(self.sched)
            self.sched._on_done(self)

    def join(self, timeout=None):
        """Block (cooperatively if on a worker) until the task finishes.
        Returns True if finished, False on timeout."""
        with self._jlock:
            if self.done:
                return True
            w = Waiter()
            self._joiners.append(w)
        return w.block(timeout)

    def result(self, timeout=None):
        """Wait for completion and return the value, re-raising any exception."""
        if not self.join(timeout):
            raise TimeoutError("task did not finish within timeout")
        self._retrieved = True
        if self.exc is not None:
            raise self.exc
        return self.value


def spawn(fn, *args, **kwargs):
    """Spawn a function as a pyroutine, returning its Task handle."""
    # Strip decorator wrapper if present
    actual_fn = getattr(fn, "__wrapped__", fn)
    task = Task(actual_fn, args, kwargs)
    get_scheduler().spawn(task)
    return task


def gather(*tasks):
    """Wait for all tasks to finish and return a list of their results."""
    for t in tasks:
        t.join()
    return [t.result() for t in tasks]


class TaskGroup:
    """A context manager to spawn and coordinate a group of pyroutines.

    Ensures structured concurrency: blocks upon exit of the context manager
    until all spawned tasks in the group are completed.
    """

    def __init__(self):
        self._tasks = []

    def spawn(self, fn, *args, **kwargs):
        """Spawn a task within the group."""
        t = spawn(fn, *args, **kwargs)
        self._tasks.append(t)
        return t

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Wait for all tasks to complete
        for t in self._tasks:
            try:
                t.join()
            except Exception:
                pass

        # Check for failures
        failures = []
        for t in self._tasks:
            if t.done and t.exc is not None:
                t._retrieved = True
                failures.append(t.exc)

        if exc_val is not None:
            # Propagate the block exception
            return False

        if failures:
            if sys.version_info >= (3, 11):
                raise ExceptionGroup("unhandled exceptions in TaskGroup", failures)
            else:
                if len(failures) == 1:
                    raise failures[0]
                raise RuntimeError(f"unhandled exceptions in TaskGroup: {failures}")
