"""Parallelism proof: CPU-bound pyroutines run faster than serial because the
GIL is genuinely off and multiple processors run at once."""

import sys
import time

from pyroutine import spawn, gather


def _cpu_burn(n):
    total = 0
    for i in range(n):
        total += i * i
    return total


def test_gil_off():
    assert sys._is_gil_enabled() is False


def test_cpu_bound_speedup():
    iters = 3_000_000
    tasks = 8

    # Serial baseline.
    t0 = time.monotonic()
    for _ in range(tasks):
        _cpu_burn(iters)
    serial = time.monotonic() - t0

    # Concurrent via pyroutines.
    t0 = time.monotonic()
    handles = [spawn(_cpu_burn, iters) for _ in range(tasks)]
    gather(*handles)
    concurrent = time.monotonic() - t0

    # On 8 free-threaded cores this should be a large win; require a modest 1.8x
    # to stay robust on busy CI machines.
    assert concurrent < serial / 1.8, (
        f"serial={serial:.3f}s concurrent={concurrent:.3f}s (no real parallelism?)"
    )
