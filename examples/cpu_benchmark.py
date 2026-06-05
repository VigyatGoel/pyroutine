import time
import sys
import os
import threading
import pyroutine as pr

# CPU-bound work benchmark
# Compares serial execution, standard threading, and pyroutines.

ITERS = 10_000_000
TASKS = 8

def cpu_work(iters):
    total = 0
    for i in range(iters):
        total += i * i
    return total

def run_serial():
    start = time.monotonic()
    results = []
    for _ in range(TASKS):
        results.append(cpu_work(ITERS))
    elapsed = time.monotonic() - start
    return elapsed

def run_threading():
    start = time.monotonic()
    results = [None] * TASKS
    
    def worker(idx):
        results[idx] = cpu_work(ITERS)
        
    threads = []
    for i in range(TASKS):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
        
    elapsed = time.monotonic() - start
    return elapsed

def run_pyroutine():
    start = time.monotonic()
    # Spawn and gather using pyroutine
    handles = [pr.spawn(cpu_work, ITERS) for _ in range(TASKS)]
    pr.gather(*handles)
    elapsed = time.monotonic() - start
    return elapsed

if __name__ == "__main__":
    print("=" * 60)
    print(" pyroutine CPU-Bound Performance Benchmark")
    print("=" * 60)
    print(f"Platform:              {sys.platform}")
    print(f"Python Version:        {sys.version.split()[0]}")
    print(f"GIL Enabled:           {getattr(sys, '_is_gil_enabled', lambda: True)()}")
    print(f"Available CPU Cores:   {os.cpu_count()}")
    print(f"Benchmark Workload:    {TASKS} tasks of {ITERS:,} iterations each")
    print("-" * 60)
    
    # 1. Serial Run
    print("Running Serial baseline...")
    t_serial = run_serial()
    print(f"  Serial Time:         {t_serial:.3f}s")
    
    # 2. Threading Run
    print("Running Standard Threading (Parallel cores)...")
    t_threads = run_threading()
    print(f"  Threading Time:      {t_threads:.3f}s (Speedup: {t_serial/t_threads:.2f}x)")
    
    # 3. Pyroutine Run
    print("Running pyroutine (Parallel greenlet workers)...")
    t_pyroutine = run_pyroutine()
    print(f"  pyroutine Time:      {t_pyroutine:.3f}s (Speedup: {t_serial/t_pyroutine:.2f}x)")
    print("=" * 60)
