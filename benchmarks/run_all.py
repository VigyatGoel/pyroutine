"""Run every surface and emit one combined report.
    uv run --no-sync python -m benchmarks.run_all [--quick]"""

import os

from benchmarks import _harness as h
from benchmarks import bench_concurrency, bench_http_client, bench_http_server, bench_scaling

DEFAULT_MD = os.path.join(os.path.dirname(__file__), "RESULTS.md")
DEFAULT_JSON = os.path.join(os.path.dirname(__file__), "results.json")


def build_report(md_path=DEFAULT_MD, json_path=DEFAULT_JSON, quick=False):
    if quick:
        conc = bench_concurrency.run(tasks=4, iters=50_000, io_fanout=8, runs=1)
        scaling = bench_scaling.run(n=2_000)
        client = bench_http_client.run(total=200, concurrency=10, runs=1, warmup=0)
        server = bench_http_server.run(duration_s=2, connections=10)
    else:
        conc, scaling = bench_concurrency.run(), bench_scaling.run()
        client, server = bench_http_client.run(), bench_http_server.run()
    report = {"env": h.capture_env(), "surfaces": {
        "Concurrency model": conc, "Spawn/memory scaling": scaling,
        "HTTP client": client, "HTTP server": server}}
    h.write_report(report, md_path=md_path, json_path=json_path)
    return report


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--quick", action="store_true")
    report = build_report(quick=p.parse_args().quick)
    for name, surface in report["surfaces"].items():
        if surface.get("skipped"):
            print(f"== {name} == (skipped: oha missing)\n"); continue
        print(h.render_table(name, surface["rows"], surface["columns"]), "\n")
    print(f"Report -> {DEFAULT_MD} and {DEFAULT_JSON}")
