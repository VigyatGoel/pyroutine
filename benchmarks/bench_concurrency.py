"""Concurrency model: CPU parallelism + I/O fan-out.
pyroutine/threading parallelize CPU (~4x); asyncio cannot (~1x)."""

import asyncio
import threading
import time
from concurrent.futures import ProcessPoolExecutor

import pyroutine as pr

from benchmarks._harness import repeat


def _cpu_work(iters):
    total = 0
    for i in range(iters):
        total += i * i
    return total


def _cpu_serial(tasks, iters):
    for _ in range(tasks):
        _cpu_work(iters)


def _cpu_threading(tasks, iters):
    ts = [threading.Thread(target=_cpu_work, args=(iters,)) for _ in range(tasks)]
    for t in ts: t.start()
    for t in ts: t.join()


def _cpu_asyncio(tasks, iters):
    async def main():
        for _ in range(tasks):
            _cpu_work(iters)  # one loop thread -> stays serial
    asyncio.run(main())


def _cpu_multiprocessing(tasks, iters):
    with ProcessPoolExecutor() as ex:
        list(ex.map(_cpu_work, [iters] * tasks))


def _cpu_pyroutine(tasks, iters):
    pr.gather(*[pr.spawn(_cpu_work, iters) for _ in range(tasks)])


def _io_pyroutine(fanout):
    pr.gather(*[pr.spawn(pr.sleep, 0.01) for _ in range(fanout)])


def _io_asyncio(fanout):
    async def main():
        await asyncio.gather(*(asyncio.sleep(0.01) for _ in range(fanout)))
    asyncio.run(main())


def _io_threading(fanout):
    ts = [threading.Thread(target=time.sleep, args=(0.01,)) for _ in range(fanout)]
    for t in ts: t.start()
    for t in ts: t.join()


def run(tasks=8, iters=10_000_000, io_fanout=200, runs=5):
    base = repeat(lambda: _cpu_serial(tasks, iters), runs=runs)
    cpu = [
        ("serial", base),
        ("threading", repeat(lambda: _cpu_threading(tasks, iters), runs=runs)),
        ("asyncio", repeat(lambda: _cpu_asyncio(tasks, iters), runs=runs)),
        ("multiprocessing", repeat(lambda: _cpu_multiprocessing(tasks, iters), runs=runs)),
        ("pyroutine", repeat(lambda: _cpu_pyroutine(tasks, iters), runs=runs)),
    ]
    rows = [{"name": n, "workload": "cpu", "seconds": s,
             "speedup": (base / s if s else 0.0)} for n, s in cpu]
    io = [
        ("asyncio", repeat(lambda: _io_asyncio(io_fanout), runs=runs)),
        ("threading", repeat(lambda: _io_threading(io_fanout), runs=runs)),
        ("pyroutine", repeat(lambda: _io_pyroutine(io_fanout), runs=runs)),
    ]
    rows += [{"name": n, "workload": "io_fanout", "seconds": s, "speedup": 0.0} for n, s in io]
    return {"columns": ["name", "workload", "seconds", "speedup"], "rows": rows}


if __name__ == "__main__":
    from benchmarks._harness import render_table
    s = run()
    print(render_table("Concurrency model", s["rows"], s["columns"]))
