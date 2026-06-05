"""Server launchers for the HTTP server benchmark.
    python -m benchmarks.servers --server NAME --port P --workers W"""

import sys

SERVERS = ("pyroutine", "fastapi_1", "fastapi_n", "flask", "aiohttp")
_CPU_ITERS = 2000


def _cpu_payload():
    return sum(i * i for i in range(_CPU_ITERS))


def _serve_pyroutine(port, workers):
    import pyroutine as pr
    if workers:
        pr.set_max_procs(workers)
    app = pr.http.App()

    @app.get("/json")
    def _json(r):
        return {"ok": True}

    @app.get("/cpu")
    def _cpu(r):
        return {"sum": _cpu_payload()}

    _, h = app.start(f"127.0.0.1:{port}")
    h.join()


def _build_fastapi():
    from fastapi import FastAPI
    app = FastAPI()

    @app.get("/json")
    async def _json():
        return {"ok": True}

    @app.get("/cpu")
    async def _cpu():
        return {"sum": _cpu_payload()}

    return app


def _serve_fastapi(port, workers):
    import uvicorn
    if workers and workers > 1:
        # Multi-worker: uvicorn forks workers that re-import this module, so it must
        # build the app itself via a factory (a module global set in the parent would
        # be None in the workers). factory=True calls _build_fastapi() in each worker.
        uvicorn.run("benchmarks.servers:_build_fastapi", factory=True, host="127.0.0.1",
                    port=port, workers=workers, log_level="warning")
    else:
        uvicorn.run(_build_fastapi(), host="127.0.0.1", port=port, log_level="warning")


def _serve_flask(port, workers):
    from flask import Flask, jsonify
    app = Flask(__name__)

    @app.get("/json")
    def _json():
        return jsonify(ok=True)

    @app.get("/cpu")
    def _cpu():
        return jsonify(sum=_cpu_payload())

    app.run(host="127.0.0.1", port=port, threaded=True)  # thread per request


def _serve_aiohttp(port, workers):
    from aiohttp import web

    async def _json(req):
        return web.json_response({"ok": True})

    async def _cpu(req):
        return web.json_response({"sum": _cpu_payload()})

    app = web.Application()
    app.add_routes([web.get("/json", _json), web.get("/cpu", _cpu)])
    web.run_app(app, host="127.0.0.1", port=port, print=None)


_SERVERS = {"pyroutine": _serve_pyroutine, "fastapi_1": _serve_fastapi,
            "fastapi_n": _serve_fastapi, "flask": _serve_flask, "aiohttp": _serve_aiohttp}


def server_cmd(name, port, workers):
    return [sys.executable, "-m", "benchmarks.servers", "--server", name,
            "--port", str(port), "--workers", str(workers)]


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--server", required=True, choices=SERVERS)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--workers", type=int, default=1)
    a = p.parse_args()
    _SERVERS[a.server](a.port, a.workers)
