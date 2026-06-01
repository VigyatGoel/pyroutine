# pyroutine

Goroutine-style concurrency for **free-threaded Python 3.14t** (GIL disabled).

Pyroutines are **greenlet-backed**: cheap to spawn (millions), **parked without holding an OS thread**, and run in **true parallel** across cores — the same trio of properties that make Go's goroutines special, on a custom M:N runtime.

```python
from pyroutine import spawn, Queue

def worker(q, n):
    q.put(n * n)          # ordinary blocking code — no async/await

q = Queue()               # cooperative FIFO queue (mimics queue.Queue)
task = spawn(worker, q, 10) # launch as a pyroutine task
spawn(worker, q, 5)
print(q.get(), q.get())   # 100, 25
task.join()               # wait for the task to finish
```

## Requirements

Runs on a **free-threaded** build (`python3.14t`) with **greenlet ≥ 3.5.1** (which,
verified, keeps the GIL off on free-threaded 3.14 — see below). With `uv`:

```bash
uv python pin 3.14t      # -> .python-version: 3.14+freethreaded
uv venv --python 3.14t
uv sync
uv run --no-sync python your_script.py
```

## Measured

```
1,000,000 pyroutines parked on a Queue      : 9 threads total, ~0.86 KB each
CPU-bound (8× sum(i*i)) vs serial          : 1.46s -> 0.43s   (3.4x, GIL off)
I/O throughput (200 conns × 200 echoes)    : 98k round-trips/s  (asyncio: 78k)
```

On that mixed in-process client+server I/O workload pyroutine **out-throughputs
asyncio ~1.24×**, because it parallelizes per-request Python work across cores
(GIL off) while asyncio runs on a single event-loop thread. (Caveat: a *connect
storm* of thousands of simultaneous new connections is bounded by the OS listen
backlog, `kern.ipc.somaxconn`, not by pyroutine — raise it or pool connections.)

## API (v1)

| Thing | Use |
|---|---|
| `spawn(fn, *a, **kw)` | Launch a callable as a concurrent pyroutine task -> returns a `Task` handle |
| `gather(*tasks)` | Wait for all tasks to complete and return a list of their results |
| `TaskGroup` | Context manager for structured concurrency. Spawns tasks and gathers them on exit, raising `ExceptionGroup` if any fail |
| `Task` | Represents a concurrent task. Methods: `.join(timeout=None)`, `.result(timeout=None)` (re-raises task exception), property `.done` |
| `Queue(maxsize=0)` | Cooperative FIFO queue. Methods: `.put(x, timeout=None)` (blocks if full), `.get(timeout=None)` (blocks if empty), `.qsize()`, `.empty()`, `.full()` |
| `sleep(s)` | Cooperative sleep — parks the pyroutine via a timer, freeing the worker thread |
| `yield_()` | Yield execution to other ready pyroutines on the current worker thread |
| `Socket(...)` | Cooperative non-blocking socket wrapping a raw socket. Parks on the netpoller |
| `poll_wait(fd, READ\|WRITE)` | Low-level: park the pyroutine until a raw fd is ready |
| `run_blocking(fn, *a)` | Run an uninstrumented blocking call (file I/O, DNS, sync requests) on a helper thread pool |
| `set_max_procs(n)` | Set worker/hub thread count (default: `os.cpu_count()`) |
| `shutdown()` | Drain outstanding pyroutines and stop the runtime |
| `http.App` | Decorator-based HTTP router: `@app.get("/path")`, `@app.post("/path")`, etc. Handles JSON serialization, status/headers tuples, chunked responses, and keep-alive |

See [`examples/`](examples/): [fanout.py](examples/fanout.py), [pipeline.py](examples/pipeline.py), [million.py](examples/million.py), [echo_server.py](examples/echo_server.py).

## How it works

Greenlets give **cheap stackful parking** — a pyroutine can suspend anywhere,
mid-stack, holding no OS thread (~1 KB each). One hard greenlet constraint shapes
everything: **a greenlet can't be switched from a different OS thread than the one
that created it** (greenlets don't migrate). So the runtime is *N independent
per-core cooperative schedulers running in parallel*:

- **Worker (M)** — one OS thread per processor (default `os.cpu_count()`). Its main
  greenlet is a **hub**: a loop that runs ready pyroutines and, when one parks,
  switches to the next.
- **Pyroutine (G)** — a greenlet, bound for life to the worker that started it.
- **Pending queues** — per-worker queues of not-yet-started pyroutines, assigned
  round-robin, with **work-stealing** (the only thing that can cross threads is a
  *not-yet-started* task).
- **Park / wake** — a parked pyroutine yields to its hub; waking it (possibly from
  another worker or the main thread) enqueues its greenlet on its *owner* worker's
  ready queue and signals that worker. Values and wakeups cross threads, greenlets
  don't.
- **Netpoller** — a single `selectors` (kqueue/epoll) thread. Socket I/O that would
  block parks the pyroutine and registers the fd; when it's ready the poller wakes
  the pyroutine. Thousands–millions of connections wait here on **one** thread,
  holding no workers — pyroutine's analogue of Go's integrated network poller.

### Does greenlet really keep the GIL off? (yes — verified)

Older greenlet (≤ 3.4.0) re-enabled the GIL on free-threaded builds. **greenlet
3.5.1 does not** — verified on CPython 3.14.5t: `sys._is_gil_enabled()` stays
`False` after import (no GIL-enable warning under `-W error::RuntimeWarning`),
switching works, and CPU work hits 3.4× parallelism. greenlet's free-threaded
support is still young/experimental (the docs note rare crashes; their CI doesn't
fully cover it), so treat heavy production use with care.

### Cooperative caveat (the one trade-off)

Only *cooperative* operations park cheaply: `Queue` operations, `sleep()`, `yield_()`,
and **socket I/O via `Socket`** (which parks on the netpoller). Those scale to
millions of parked pyroutines.

A blocking call the runtime *doesn't* know about — `time.sleep`, file I/O, DNS, or a
library using raw blocking sockets like `requests` — would freeze its worker and
every pyroutine on it. Wrap those in `run_blocking`, which offloads to a helper
thread pool:

```python
import requests
from pyroutine import spawn, run_blocking

def fetch(url):
    return run_blocking(requests.get, url).status_code   # one pool thread per call

task = spawn(fetch, "https://httpbin.org/get")
status_code = task.result()
```

So **native socket concurrency** (via `Socket`) is cheap and unbounded; *unwrapped*
third-party blocking I/O is bounded by OS threads (~thousands). A gevent-style
monkeypatch to make `requests` use the netpoller transparently is on the roadmap.

### Compared to Go

| | Go goroutine | pyroutine |
|---|---|---|
| Spawn | `go f()` | `spawn(f)` |
| Cheap stackful parking | ✅ ~2 KB | ✅ ~1 KB (greenlet) |
| Millions concurrently parked | ✅ | ✅ |
| True multicore parallelism | ✅ | ✅ (GIL off) |
| Netpoller for sockets (millions of conns) | ✅ | ✅ (via `Socket`) |
| Transparent I/O for *any* lib (e.g. `requests`) | ✅ (all I/O netpolled) | ⚠️ `Socket` is cooperative; others need `run_blocking` (monkeypatch on roadmap) |
| Preemption of CPU loops | ✅ | ❌ (cooperative; `yield_()` manually) |

## Roadmap

gevent-style **socket monkeypatching** so plain `requests`/socket code uses the
netpoller without `run_blocking`; cooperative **DNS** (`getaddrinfo`); `select` (with
non-blocking `default`); `Context` (cancellation/deadline); timers/`ticker`;
PEP 669–based preemption for CPU fairness.

## Development

```bash
uv run --no-sync pytest          # full suite (run on python3.14t)
```
