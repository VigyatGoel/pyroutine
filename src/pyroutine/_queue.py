"""Cooperative FIFO queue, mimicking Python's ``queue.Queue``.

Allows pyroutines (and standard threads) to pass messages cooperatively.
Under the hood, blocking parks the pyroutine on a Waiter token.
"""

import threading
from collections import deque

from ._runtime import Waiter, current_scheduler


class Queue:
    """A cooperative FIFO queue."""

    def __init__(self, maxsize=0):
        # maxsize <= 0 means infinite size
        self.maxsize = maxsize
        self._buf = deque()
        self._lock = threading.Lock()
        self._get_waiters = deque()  # waiting receivers (Waiter)
        self._put_waiters = deque()  # waiting senders (Waiter)

    def qsize(self):
        """Return the size of the queue."""
        with self._lock:
            return len(self._buf)

    def empty(self):
        """Return True if the queue is empty."""
        with self._lock:
            return len(self._buf) == 0

    def full(self):
        """Return True if the queue is full."""
        with self._lock:
            return self.maxsize > 0 and len(self._buf) >= self.maxsize

    def put(self, item, timeout=None):
        """Put an item into the queue. Blocks cooperatively if full."""
        with self._lock:
            # If there's a waiter waiting to get, hand off directly.
            if self._get_waiters:
                w = self._get_waiters.popleft()
                w.value = item
                w.wake(current_scheduler())
                return

            # Otherwise, check if we have space.
            if self.maxsize <= 0 or len(self._buf) < self.maxsize:
                self._buf.append(item)
                return

            # Bounded queue is full: block.
            w = Waiter()
            w.value = item
            self._put_waiters.append(w)

        # Wait block
        if not w.block(timeout):
            # Timed out
            with self._lock:
                try:
                    self._put_waiters.remove(w)
                except ValueError:
                    pass
            raise TimeoutError("Queue put timed out")

    def get(self, timeout=None):
        """Remove and return an item from the queue. Blocks cooperatively if empty."""
        with self._lock:
            # If there's an item in the buffer, return it.
            if self._buf:
                item = self._buf.popleft()
                # If there's a blocked putter, pull their item into the buffer.
                if self._put_waiters:
                    pw = self._put_waiters.popleft()
                    self._buf.append(pw.value)
                    pw.wake(current_scheduler())
                return item

            # If no items but a blocked putter is waiting, rendezvous (unbuffered case).
            if self._put_waiters:
                pw = self._put_waiters.popleft()
                item = pw.value
                pw.wake(current_scheduler())
                return item

            # Queue is empty: block.
            w = Waiter()
            self._get_waiters.append(w)

        # Wait block
        if not w.block(timeout):
            # Timed out
            with self._lock:
                try:
                    self._get_waiters.remove(w)
                except ValueError:
                    pass
            raise TimeoutError("Queue get timed out")
        return w.value
