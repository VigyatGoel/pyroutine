"""pyroutine -- Pythonic structured concurrency for free-threaded Python 3.14t.

Pyroutines are greenlet-backed: cheap to spawn (millions), parked without holding
an OS thread, and run in true parallel across cores (GIL off on python3.14t).

    from pyroutine import spawn, Queue, sleep

    def worker(q, n):
        q.put(n * n)

    q = Queue()
    spawn(worker, q, 10)
    print(q.get())        # 100

Use blocking code freely with the runtime's own primitives (Queue, sleep,
yield_). For *uninstrumented* blocking calls (e.g. requests.get) wrap them in
run_blocking() so they don't freeze a worker.
"""

from ._pyroutine import spawn, gather, TaskGroup, Task
from ._queue import Queue
from ._runtime import (
    Scheduler,
    sleep,
    yield_,
    run_blocking,
    set_max_procs,
    shutdown,
    poll_wait,
    READ,
    WRITE,
)
from ._net import Socket
from . import http

__all__ = [
    "spawn",
    "gather",
    "TaskGroup",
    "Task",
    "Queue",
    "Scheduler",
    "sleep",
    "yield_",
    "run_blocking",
    "set_max_procs",
    "shutdown",
    "Socket",
    "poll_wait",
    "READ",
    "WRITE",
    "http",
]

__version__ = "0.1.0"
