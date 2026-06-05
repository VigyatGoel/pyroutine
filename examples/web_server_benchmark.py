import time
import socket
import pyroutine as pr

# Force 1 processor worker so everything runs on the same thread
pr.set_max_procs(1)

# Helper to find a free local port
def get_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port

# Create the HTTP App
app = pr.http.App()

@app.get("/fast-io")
def fast_io(r):
    # Simulate a quick database or cache lookup
    pr.sleep(0.01)
    return "ok"

@app.get("/heavy-cpu")
def heavy_cpu(r):
    # Simulate heavy CPU workload (takes approx 0.1s of raw CPU execution)
    start = time.monotonic()
    while time.monotonic() - start < 0.100:
        _ = sum(i * i for i in range(200))
    return "done"

def run_scenario(address, config):
    preemption_enabled, time_slice, check_interval = config
    
    if preemption_enabled:
        mode_name = f"PREEMPTIVE (slice={time_slice}s, interval={check_interval})"
        pr.enable_preemption(time_slice=time_slice, check_interval=check_interval)
    else:
        mode_name = "COOPERATIVE"
        
    fast_latencies = []
    heavy_duration = [0.0]
    
    def heavy_client():
        t0 = time.monotonic()
        pr.http.get(f"http://{address}/heavy-cpu")
        heavy_duration[0] = time.monotonic() - t0
        
    def fast_client(idx):
        t0 = time.monotonic()
        # Stagger slightly to make sure the heavy request hits first
        pr.sleep(0.005)
        pr.http.get(f"http://{address}/fast-io")
        lat = time.monotonic() - t0
        fast_latencies.append(lat)
        
    with pr.TaskGroup() as tg:
        tg.spawn(heavy_client)
        for i in range(5):
            tg.spawn(fast_client, i)
            
    if preemption_enabled:
        pr.disable_preemption()
        
    avg_lat = sum(fast_latencies) / len(fast_latencies)
    max_lat = max(fast_latencies)
    
    return {
        "mode": mode_name,
        "heavy_time": heavy_duration[0],
        "avg_fast_lat": avg_lat,
        "max_fast_lat": max_lat,
    }

if __name__ == "__main__":
    port = get_free_port()
    address = f"127.0.0.1:{port}"
    
    print("=" * 75)
    print(" Web Server Preemption Tuning Sweep")
    print(" Running on a single worker thread (max_procs = 1)")
    print("=" * 75)
    
    # Start server
    server, h = app.start(address)
    time.sleep(0.05)  # Wait for server to bind
    
    # Safe configurations to test: (enabled, time_slice, check_interval)
    configs = [
        (False, 0.0, 0),                      # Cooperative (no preemption)
        (True, 0.050, 1000),                  # Lazy / Low overhead preemption
        (True, 0.010, 200),                   # Balanced / Default-like preemption
    ]
    
    results = []
    try:
        for cfg in configs:
            res = run_scenario(address, cfg)
            results.append(res)
            # Short cooldown between runs
            time.sleep(0.02)
    finally:
        server.close()
        h.join()
        
    # Print results table
    print(f"\n{'Configuration':<45} | {'Heavy CPU':<10} | {'Avg Fast I/O':<12} | {'Max Fast I/O':<12}")
    print("-" * 87)
    for r in results:
        print(f"{r['mode']:<45} | {r['heavy_time']:.3f}s     | {r['avg_fast_lat']:.3f}s       | {r['max_fast_lat']:.3f}s")
    print("=" * 75)
