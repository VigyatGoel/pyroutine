import time
import sys
import pyroutine as pr

# Force 1 processor worker so everything runs on the same thread
pr.set_max_procs(1)

# Keep track of when events occur
timeline = []

def cpu_hog(duration):
    """A task that burns CPU cycles continuously without yielding."""
    start = time.monotonic()
    timeline.append(("hog_start", start))
    count = 0
    while time.monotonic() - start < duration:
        # Simple computation to consume CPU
        _ = sum(i * i for i in range(100))
        count += 1
    timeline.append(("hog_end", time.monotonic()))
    return count

def reporter(duration):
    """A lightweight task that expects to run periodically."""
    start = time.monotonic()
    while time.monotonic() - start < duration:
        timeline.append(("reporter_tick", time.monotonic()))
        # Sleep for a tiny amount of time to let other tasks run
        pr.sleep(0.01)

def run_test(preemption_enabled):
    global timeline
    timeline = []
    
    mode_name = "PREEMPTIVE" if preemption_enabled else "COOPERATIVE"
    print(f"\n--- Running Demo in {mode_name} Mode ---")
    
    if preemption_enabled:
        # Enable preemption with 5ms time-slice
        pr.enable_preemption(time_slice=0.005, check_interval=50)
    
    # Spawn both tasks
    t_hog = pr.spawn(cpu_hog, 0.2)
    t_reporter = pr.spawn(reporter, 0.2)
    
    # Wait for both to finish
    t_hog.join()
    t_reporter.join()
    
    if preemption_enabled:
        pr.disable_preemption()
        
    # Analyze the timeline
    hog_start = [t for event, t in timeline if event == "hog_start"][0]
    hog_end = [t for event, t in timeline if event == "hog_end"][0]
    ticks = [t for event, t in timeline if event == "reporter_tick"]
    
    # Count ticks that occurred WHILE the hog was running
    ticks_during_hog = sum(1 for t in ticks if hog_start <= t <= hog_end)
    
    print(f"CPU Hog ran from {hog_start:.3f} to {hog_end:.3f} (Duration: {hog_end - hog_start:.3f}s)")
    print(f"Total reporter ticks: {len(ticks)}")
    print(f"Reporter ticks during CPU Hog execution: {ticks_during_hog}")
    
    if ticks_during_hog == 0:
        print("-> Result: Reporter was COMPLETELY STARVED while Hog was running.")
    else:
        print(f"-> Result: Reporter successfully ran concurrently during the Hog's execution ({ticks_during_hog} ticks)!")

if __name__ == "__main__":
    print("=" * 60)
    print(" Starvation and Preemption Demonstration")
    print(" Running on a single worker thread (max_procs = 1)")
    print("=" * 60)
    
    # 1. Run in Cooperative Mode (No Preemption)
    run_test(preemption_enabled=False)
    
    # 2. Run in Preemptive Mode
    run_test(preemption_enabled=True)
    
    # Shutdown scheduler
    import pyroutine._runtime as rt
    if rt._global_scheduler is not None:
        rt._global_scheduler.shutdown(wait=True)
    print("\n" + "=" * 60)
