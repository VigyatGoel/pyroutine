# pyroutine

[![PyPI](https://img.shields.io/pypi/v/pyroutine-rt)](https://pypi.org/project/pyroutine-rt/)

Goroutine-style concurrency for **free-threaded Python 3.14t** (GIL disabled).

```bash
pip install pyroutine-rt        # import name stays `pyroutine`
```

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

Runs on a **free-threaded** build (`python3.14t`) with **greenlet ≥ 3.5.1**:

```bash
uv python pin 3.14t      # -> .python-version: 3.14+freethreaded
uv venv --python 3.14t
uv sync
uv run --no-sync python your_script.py
```

## Install

The PyPI **distribution** name is `pyroutine-rt`; the **import** name is `pyroutine`.
(`pip install pyroutine` / `pyroutines` grabs unrelated packages — use `pyroutine-rt`.)

```bash
pip install pyroutine-rt        # then:  from pyroutine import spawn, gather
# or with uv:
uv add pyroutine-rt
```

Install the latest unreleased code straight from git:

```bash
pip install "pyroutine-rt @ git+https://github.com/VigyatGoel/pyroutine.git"
```

Either way, do it inside a **free-threaded `python3.14t`** environment (see Requirements);
on a stock GIL build it imports but won't run pyroutines in parallel.

## Benchmarks

Cross-library benchmark suite in [`benchmarks/`](benchmarks/) comparing pyroutine
against asyncio/threading/multiprocessing and FastAPI/Flask/aiohttp/httpx/requests
across four surfaces: concurrency model, HTTP server, HTTP client, spawn/memory scaling.

    uv sync --group bench        # competitor libs
    brew install oha             # external HTTP load tool (server suite)
    uv run --no-sync python -m benchmarks.run_all

Results print and write to `benchmarks/RESULTS.md` + `results.json`. The server suite
skips with a hint if `oha` is absent. This dev box is 4 perf + 4 efficiency cores, so
CPU-parallel speedup caps ~4x.

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
| `enable_preemption(time_slice=0.010, check_interval=1000)` / `disable_preemption()` | Opt-in preemptive time-slicing so a CPU-bound pyroutine that never yields can't starve others on the same worker |
| `http.App` | Decorator-based HTTP router: `@app.get("/path")`, `@app.post("/path")`, etc. Handles JSON serialization, status/headers tuples, chunked responses, and keep-alive |

See [`examples/`](examples/): [fanout.py](examples/fanout.py), [pipeline.py](examples/pipeline.py), [echo_server.py](examples/echo_server.py), [http_server_example.py](examples/http_server_example.py).

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

| | Go goroutine | pyroutine                                                                      |
|---|---|--------------------------------------------------------------------------------|
| Spawn | `go f()` | `spawn(f)`                                                                     |
| Cheap stackful parking | ✅ ~2 KB | ✅ ~2 KB (greenlet)                                                             |
| Millions concurrently parked | ✅ | ✅                                                                              |
| True multicore parallelism | ✅ | ✅ (GIL off)                                                                    |
| Netpoller for sockets (millions of conns) | ✅ | ✅ (via `Socket`)                                                               |
| Transparent I/O for *any* lib (e.g. `requests`) | ✅ (all I/O netpolled) | ⚠️ `Socket` is cooperative; others need `run_blocking` |
| Preemption of CPU loops | ✅ | ✅ (opt-in via `enable_preemption()`; or `yield_()` manually)                   |

## Roadmap

gevent-style **socket monkeypatching** so plain `requests`/socket code uses the
netpoller without `run_blocking`; `select` (with non-blocking `default`);
`Context` (cancellation/deadline); timers/`ticker`.

## Development

```bash
uv run --no-sync pytest          # full suite (run on python3.14t)
```

## License

This project is licensed under the MIT License.
