# pyroutine Benchmark Results

## Environment

- **python**: 3.14.5
- **gil_enabled**: False
- **cpu_count**: 8
- **platform**: darwin
- **note**: 4 performance + 4 efficiency cores: CPU-parallel speedup caps ~4x, not 8x.

## Concurrency model

| name | workload | seconds | speedup |
| --- | --- | --- | --- |
| serial | cpu | 2.90 | 1.00 |
| threading | cpu | 0.83 | 3.49 |
| asyncio | cpu | 2.89 | 1.00 |
| multiprocessing | cpu | 0.80 | 3.61 |
| pyroutine | cpu | 0.84 | 3.47 |
| asyncio | io_fanout | 0.01 | 0.00 |
| threading | io_fanout | 0.02 | 0.00 |
| pyroutine | io_fanout | 0.01 | 0.00 |

## Spawn/memory scaling

| name | reached | spawn_s | kb_per_task |
| --- | --- | --- | --- |
| pyroutine | 1000000 | 7.34 | 2.63 |
| asyncio | 1000000 | 1.66 | 0.92 |
| threads | 2047 | 0.19 | 405.85 |

## HTTP client

| name | requests | seconds | req_per_sec |
| --- | --- | --- | --- |
| pyroutine | 10000 | 1.73 | 5,770.22 |
| httpx_sync | 10000 | 2.64 | 3,791.83 |
| httpx_async | 10000 | 6.39 | 1,564.30 |
| requests | 10000 | 3.44 | 2,905.91 |
| aiohttp | 10000 | 1.92 | 5,204.85 |

## HTTP server

| name | req_per_sec | p50_ms | p90_ms | p99_ms | success_rate |
| --- | --- | --- | --- | --- | --- |
| pyroutine | 74,011.12 | 0.35 | 1.67 | 3.35 | 1.00 |
| fastapi (1 worker) | 9,527.11 | 5.16 | 5.57 | 6.46 | 1.00 |
| fastapi (8 workers) | 19,609.08 | 1.51 | 6.76 | 9.82 | 1.00 |
| flask (threaded) | 3,406.32 | 8.99 | 10.49 | 14.85 | 0.96 |
| aiohttp | 35,397.30 | 1.38 | 1.52 | 1.89 | 1.00 |
