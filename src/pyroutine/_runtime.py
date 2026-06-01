"""The pyroutine runtime: N per-core cooperative event loops, greenlet-backed.

Greenlets give cheap **stackful parking** (~1 KB each, suspend anywhere mid-stack).
On a free-threaded build (``python3.14t``) greenlet keeps the GIL **off**, so we
also get real multicore parallelism.

One hard greenlet constraint shapes the architecture: **a greenlet cannot be
switched from a thread other than the one that created it**. So each worker is an
independent **event loop**:

* **Worker (M)** -- one OS thread per processor (default ``os.cpu_count()``). Its
  main greenlet is a hub running a loop that (1) resumes ready pyroutines, (2)
  starts pending ones, and (3) when idle, blocks in ``selector.select()`` waiting
  on socket readiness, timers, or a wake-up.
* **Pyroutine (G)** -- a greenlet, bound for life to the worker that started it.
* **Per-worker netpoller** -- each worker owns a ``selectors`` instance. A pyroutine
  doing socket I/O registers its fd on *its own worker's* selector and parks; the
  same worker discovers readiness and resumes it -- **no cross-thread hand-off** on
  the I/O hot path (the key difference from a single shared poller thread).
* **Wake-ups** -- timers and same-worker I/O resume greenlets directly. Cross-worker
  events (a queue put to a pyroutine on another worker, work distribution) append
  to the target worker's ready/pending queue and nudge it through a **self-pipe**
  registered in its selector.

Cooperative caveat: only cooperative operations park cheaply -- ``Queue`` ops,
:func:`sleep`, :func:`yield_`, and socket I/O via :class:`~pyroutine.Socket`. A real
blocking call (``time.sleep``, ``requests.get``) freezes its worker; wrap those in
:func:`run_blocking`.
"""

import os
import sys
import time
import heapq
import signal
import atexit
import socket
import threading
import selectors
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import greenlet

# Track if the process has received an exit signal (Ctrl+C / SIGTERM) or raised
# an unhandled exception in the main thread, to avoid hanging on atexit cleanup.
_interrupted = False
_original_sigint_handler = None
_original_sigterm_handler = None


def _exit_signal_handler(signum, frame):
    global _interrupted
    _interrupted = True
    if signum == signal.SIGINT:
        try:
            signal.signal(signal.SIGINT, _original_sigint_handler)
        except Exception:
            pass
        raise KeyboardInterrupt
    else:
        sys.exit(128 + signum)


def _setup_signal_handlers():
    global _original_sigint_handler, _original_sigterm_handler
    try:
        _original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, _exit_signal_handler)
    except ValueError, OSError:
        pass

    try:
        _original_sigterm_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, _exit_signal_handler)
    except ValueError, OSError:
        pass


_setup_signal_handlers()

_original_excepthook = sys.excepthook


def _pyroutine_excepthook(type, value, traceback_val):
    global _interrupted
    _interrupted = True
    if _original_excepthook:
        _original_excepthook(type, value, traceback_val)


sys.excepthook = _pyroutine_excepthook


READ = selectors.EVENT_READ
WRITE = selectors.EVENT_WRITE

# How long an idle worker will sleep when another worker has stealable pending
# work (bounds work-stealing latency without busy-spinning in steady state).
_STEAL_POLL = 0.001

# Per-thread worker state. Greenlets on the same worker share this thread-local.
_local = threading.local()


def _on_worker():
    return getattr(_local, "wid", None) is not None


class Waiter:
    """A one-shot wake token usable from a pyroutine or a plain thread.

    On a worker it captures the current greenlet and parks via the hub; off-worker
    it falls back to a ``threading.Event``. Carries the ``value``/``closed`` outcome
    so the woken side needs no re-check.
    """

    __slots__ = ("wid", "gr", "event", "value", "closed")

    def __init__(self):
        self.wid = getattr(_local, "wid", None)
        self.gr = greenlet.getcurrent() if self.wid is not None else None
        self.event = threading.Event() if self.gr is None else None
        self.value = None
        self.closed = False

    def block(self, timeout=None):
        if self.gr is not None:
            _local.hub.switch()
            return True
        return self.event.wait(timeout)

    def wake(self, scheduler):
        if self.gr is not None:
            scheduler._enqueue_ready(self.wid, self.gr)
        else:
            self.event.set()


class Timer:
    __slots__ = ("deadline", "gr", "fd", "event", "active")

    def __init__(self, deadline, gr, fd=None, event=None):
        self.deadline = deadline
        self.gr = gr
        self.fd = fd
        self.event = event
        self.active = True


class Scheduler:
    """N per-core event loops, each with its own run queue, timers, and netpoller."""

    def __init__(self, nprocs=None):
        self.nprocs = max(1, nprocs if nprocs is not None else _default_nprocs())

        # Per-worker queues (index = worker id), guarded by _qlock[wid].
        self._qlock = [threading.Lock() for _ in range(self.nprocs)]
        self._ready = [deque() for _ in range(self.nprocs)]  # ready greenlets
        self._pending = [
            deque() for _ in range(self.nprocs)
        ]  # not-yet-started _G (stealable)

        # Timers are touched only by their owning worker (no lock needed).
        self._timers = [[] for _ in range(self.nprocs)]
        self._timer_seq = 0

        # Self-pipe per worker: other threads write a byte to interrupt select().
        self._wake_r = []
        self._wake_w = []
        for _ in range(self.nprocs):
            r, w = socket.socketpair()
            r.setblocking(False)
            w.setblocking(False)
            self._wake_r.append(r)
            self._wake_w.append(w)

        # Round-robin cursor + total pending count (for steal liveness).
        self._smeta = threading.Lock()
        self._rr = 0
        self._pt = 0

        self._running = False
        self._outstanding = 0
        self._done_cond = threading.Condition()

        self._blocking_pool = None
        self._failures = []
        self._fail_lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        for wid in range(self.nprocs):
            threading.Thread(
                target=self._worker_main,
                args=(wid,),
                name=f"pyroutine-M{wid}",
                daemon=True,
            ).start()

    def shutdown(self, wait=True):
        if wait:
            self.wait_idle()
        self._running = False
        for wid in range(self.nprocs):
            self._nudge(wid)
        if self._blocking_pool is not None:
            self._blocking_pool.shutdown(wait=False)

    def wait_idle(self, timeout=None):
        with self._done_cond:
            return self._done_cond.wait_for(lambda: self._outstanding == 0, timeout)

    # -- submission --------------------------------------------------------

    def spawn(self, g):
        g.sched = self
        with self._done_cond:
            self._outstanding += 1
        with self._smeta:
            wid = self._rr % self.nprocs
            self._rr += 1
            self._pt += 1
        with self._qlock[wid]:
            self._pending[wid].append(g)
        self._nudge(wid)
        return g

    def submit(self, task):
        from ._pyroutine import Task

        if not isinstance(task, Task):
            task = Task(task, (), {})
        return self.spawn(task)

    # -- the per-core event loop ------------------------------------------

    def _worker_main(self, wid):
        sel = selectors.DefaultSelector()
        wake_r = self._wake_r[wid]
        sel.register(wake_r, READ)
        _local.hub = greenlet.getcurrent()
        _local.wid = wid
        _local.sched = self
        _local.selector = sel
        _local.io_waiters = {}  # fd -> {event: greenlet}, touched only by this worker
        wake_fd = wake_r.fileno()
        try:
            # A standard event-loop tick: timers and I/O are serviced EVERY pass, so
            # neither starves under a steady stream of runnable work.
            while self._running:
                self._expire_timers(wid)
                ran = self._run_ready_batch(wid)
                started = self._start_pending_batch(wid)
                self._poll(wid, sel, wake_fd, block=not (ran or started))
        finally:
            sel.close()

    def _run_ready_batch(self, wid):
        # Run only the greenlets ready *now*; ones woken during the batch wait for
        # the next tick, so I/O polling isn't starved by a self-refilling queue.
        with self._qlock[wid]:
            if not self._ready[wid]:
                return False
            batch = list(self._ready[wid])
            self._ready[wid].clear()
        for gr in batch:
            gr.switch()
        return True

    def _start_pending_batch(self, wid, limit=64):
        started = False
        for _ in range(limit):
            g = self._take_pending(wid)
            if g is None:
                break
            self._start(g, wid)
            started = True
        if not started:
            g = self._steal_pending(wid)
            if g is not None:
                self._start(g, wid)
                started = True
        return started

    def _poll(self, wid, sel, wake_fd, block):
        if block:
            timeout = self._next_timeout(wid)
            with self._smeta:
                others_pending = self._pt > 0
            if others_pending:
                timeout = _STEAL_POLL if timeout is None else min(timeout, _STEAL_POLL)
        else:
            timeout = 0  # we did work this tick — just sweep ready I/O, don't sleep
        try:
            events = sel.select(timeout)
        except OSError:
            events = []
        for key, mask in events:
            if key.fd == wake_fd:
                self._drain(wid)
            else:
                self._io_ready(wid, key.fd, mask)

    def _start(self, g, wid):
        g.wid = wid
        gr = greenlet.greenlet(g._run)  # parent defaults to the hub
        g.gr = gr
        gr.switch()

    # -- run queues --------------------------------------------------------

    def _ready_append(self, wid, gr):
        """Same-worker enqueue (I/O, timers): no nudge needed."""
        with self._qlock[wid]:
            self._ready[wid].append(gr)

    def _enqueue_ready(self, wid, gr):
        """Cross-worker enqueue: append and wake the target worker's loop."""
        with self._qlock[wid]:
            self._ready[wid].append(gr)
        self._nudge(wid)

    def _take_pending(self, wid):
        with self._qlock[wid]:
            g = (
                self._pending[wid].pop() if self._pending[wid] else None
            )  # LIFO: locality
        if g is not None:
            with self._smeta:
                self._pt -= 1
        return g

    def _steal_pending(self, wid):
        for victim in range(self.nprocs):
            if victim == wid:
                continue
            with self._qlock[victim]:
                g = self._pending[victim].popleft() if self._pending[victim] else None
            if g is not None:
                with self._smeta:
                    self._pt -= 1
                return g
        return None

    # -- timers (owning worker only) --------------------------------------

    def _add_timer(self, wid, deadline, gr, fd=None, event=None):
        self._timer_seq += 1
        timer = Timer(deadline, gr, fd, event)
        heapq.heappush(self._timers[wid], (deadline, self._timer_seq, timer))
        return timer

    def _next_timeout(self, wid):
        heap = self._timers[wid]
        if not heap:
            return None
        return max(0.0, heap[0][0] - time.monotonic())

    def _expire_timers(self, wid):
        heap = self._timers[wid]
        now = time.monotonic()
        while heap and heap[0][0] <= now:
            _, _, timer = heapq.heappop(heap)
            if timer.active:
                timer.active = False
                if timer.fd is not None:
                    # Clean up selector
                    slots = _local.io_waiters.get(timer.fd)
                    if slots and timer.event in slots:
                        slots.pop(timer.event)
                        if not slots:
                            _local.io_waiters.pop(timer.fd, None)
                            try:
                                _local.selector.unregister(timer.fd)
                            except KeyError, ValueError:
                                pass
                        else:
                            mask = 0
                            for ev in slots:
                                mask |= ev
                            try:
                                _local.selector.modify(timer.fd, mask)
                            except KeyError, ValueError:
                                pass
                self._ready_append(wid, timer.gr)

    # -- per-worker netpoller (owning worker only) ------------------------

    def _io_ready(self, wid, fd, mask):
        sel = _local.selector
        slots = _local.io_waiters.get(fd)
        if not slots:
            try:
                sel.unregister(fd)
            except KeyError, ValueError:
                pass
            return
        for ev in (READ, WRITE):
            if mask & ev and ev in slots:
                self._ready_append(wid, slots.pop(ev))
        if slots:
            newmask = 0
            for ev in slots:
                newmask |= ev
            sel.modify(fd, newmask)
        else:
            _local.io_waiters.pop(fd, None)
            try:
                sel.unregister(fd)
            except KeyError, ValueError:
                pass

    # -- self-pipe ---------------------------------------------------------

    def _nudge(self, wid):
        try:
            self._wake_w[wid].send(b"\x01")
        except BlockingIOError, OSError:
            pass

    def _drain(self, wid):
        try:
            while self._wake_r[wid].recv(4096):
                pass
        except BlockingIOError:
            pass

    # -- completion / failure ---------------------------------------------

    def _on_done(self, g):
        if getattr(g, "exc", None) is not None and not getattr(g, "_retrieved", False):
            with self._fail_lock:
                self._failures.append(g)
        with self._done_cond:
            self._outstanding -= 1
            if self._outstanding == 0:
                self._done_cond.notify_all()

    def _report_unretrieved(self):
        with self._fail_lock:
            failures = list(self._failures)
        for g in failures:
            if not getattr(g, "_retrieved", False):
                tb = getattr(g, "tb", None)
                msg = "".join(tb) if tb else repr(getattr(g, "exc", None))
                print(
                    f"pyroutine: exception in pyroutine was never retrieved:\n{msg}",
                    file=sys.stderr,
                )

    # -- blocking offload --------------------------------------------------

    def _run_blocking(self, fn, args, kwargs):
        if self._blocking_pool is None:
            with self._smeta:
                if self._blocking_pool is None:
                    self._blocking_pool = ThreadPoolExecutor(
                        thread_name_prefix="pyroutine-io"
                    )
        waiter = Waiter()
        fut = self._blocking_pool.submit(fn, *args, **kwargs)
        fut.add_done_callback(lambda _f: waiter.wake(self))
        waiter.block()
        return fut.result()


# _Callable class removed


# -- cooperative scheduling API (called from inside a pyroutine) -------------


def yield_():
    """Yield to other ready pyroutines on this worker, then continue."""
    if not _on_worker():
        return
    _local.sched._ready_append(_local.wid, greenlet.getcurrent())
    _local.hub.switch()


def sleep(seconds):
    """Cooperative sleep: parks the pyroutine via a timer; frees the worker.

    Off-worker (e.g. the main thread) this falls back to ``time.sleep``.
    """
    if seconds <= 0:
        yield_()
        return
    if not _on_worker():
        time.sleep(seconds)
        return
    _local.sched._add_timer(
        _local.wid, time.monotonic() + seconds, greenlet.getcurrent()
    )
    _local.hub.switch()


def poll_wait(fd, event, timeout=None):
    """Park the current pyroutine until ``fd`` is ready for ``event`` (READ/WRITE), or timeout expires.

    Registers the fd on *this worker's* selector so the same thread resumes it --
    no cross-thread hand-off. Off-worker, blocks the calling thread on a transient
    selector instead.
    """
    if not _on_worker():
        sel = selectors.DefaultSelector()
        sel.register(fd, event)
        try:
            events = sel.select(timeout)
            if not events:
                raise TimeoutError("I/O timed out")
        finally:
            sel.close()
        return

    if timeout is not None and timeout <= 0:
        raise TimeoutError("I/O timed out")

    timer = None
    if timeout is not None:
        timer = _local.sched._add_timer(
            _local.wid, time.monotonic() + timeout, greenlet.getcurrent(), fd, event
        )

    sel = _local.selector
    slots = _local.io_waiters.setdefault(fd, {})
    slots[event] = greenlet.getcurrent()
    mask = 0
    for ev in slots:
        mask |= ev
    try:
        sel.modify(fd, mask)
    except KeyError:
        sel.register(fd, mask)

    _local.hub.switch()

    # Woken up! Check if we timed out or were ready
    if timer is not None:
        if timer.active:
            # Woken up by I/O, cancel timer
            timer.active = False
        else:
            # Woken up because timer expired
            raise TimeoutError("I/O timed out")


def run_blocking(fn, *args, **kwargs):
    """Run an *uninstrumented* blocking call on a helper thread, parking the
    pyroutine meanwhile so its worker keeps serving others::

        status = run_blocking(requests.get, url).status_code

    Off-worker, calls ``fn`` directly. Each concurrent call uses one pool thread.
    """
    if not _on_worker():
        return fn(*args, **kwargs)
    return _local.sched._run_blocking(fn, args, kwargs)


# -- global default scheduler ------------------------------------------------

_global_scheduler = None
_global_lock = threading.Lock()
_configured_nprocs = None


def _default_nprocs():
    if _configured_nprocs is not None:
        return _configured_nprocs
    env = os.environ.get("PYROUTINE_MAXPROCS")
    if env:
        try:
            return int(env)
        except ValueError:
            pass
    return os.cpu_count() or 1


def get_scheduler():
    """Return the lazily-started global scheduler that backs ``spawn()``."""
    global _global_scheduler
    with _global_lock:
        if _global_scheduler is None:
            _global_scheduler = Scheduler(_default_nprocs())
            _global_scheduler.start()
            atexit.register(_atexit_drain)
        return _global_scheduler


def current_scheduler():
    return getattr(_local, "sched", None) or _global_scheduler


def _atexit_drain():
    sched = _global_scheduler
    if sched is not None:
        if not _interrupted:
            sched.wait_idle()
        sched._report_unretrieved()
        sched.shutdown(wait=False)


def set_max_procs(n):
    """Set the number of worker hubs. Must be called before the first spawn."""
    global _configured_nprocs
    with _global_lock:
        if _global_scheduler is not None:
            raise RuntimeError(
                "set_max_procs() must be called before the first pyroutine is spawned"
            )
        _configured_nprocs = int(n)


def shutdown():
    """Wait for outstanding pyroutines, then stop the global scheduler."""
    sched = _global_scheduler
    if sched is not None:
        sched._report_unretrieved()
        sched.shutdown(wait=True)
