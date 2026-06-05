import time
import pytest
import pyroutine as pr

def setup_module(module):
    # Reset any existing scheduler to allow starting a fresh one with max procs = 1
    import pyroutine._runtime as rt
    if rt._global_scheduler is not None:
        rt._global_scheduler.shutdown(wait=True)
        rt._global_scheduler = None
    rt._configured_nprocs = 1


def compute_step():
    return sum(i * i for i in range(20))

def cpu_hog(name, duration, list_timestamps):
    start = time.monotonic()
    end = start + duration
    count = 0
    while time.monotonic() < end:
        compute_step()
        count += 1
        if count % 10 == 0:
            list_timestamps.append((name, time.monotonic()))

def test_no_preemption_starves():
    timestamps = []
    
    # We spawn tasks without enabling preemption
    t1 = pr.spawn(cpu_hog, "A", 0.05, timestamps)
    t2 = pr.spawn(cpu_hog, "B", 0.05, timestamps)
    
    t1.join()
    t2.join()
    
    # Since there was no preemption and both tasks are purely CPU-bound,
    # one task should run to completion before the other starts.
    # Note: LIFO queue means B starts and completes first, then A starts and completes.
    a_times = [t for name, t in timestamps if name == "A"]
    b_times = [t for name, t in timestamps if name == "B"]
    
    # Verify both got run
    assert len(a_times) > 0
    assert len(b_times) > 0
    
    # Verify no interleaving (all B times should be before all A times, or vice-versa)
    max_b = max(b_times)
    min_a = min(a_times)
    assert max_b <= min_a

def test_preemption_interleaves():
    # Enable preemption with a very small time slice (e.g. 5ms) to force switches
    pr.enable_preemption(time_slice=0.005, check_interval=50)
    
    timestamps = []
    
    try:
        t1 = pr.spawn(cpu_hog, "A", 0.1, timestamps)
        t2 = pr.spawn(cpu_hog, "B", 0.1, timestamps)
        
        t1.join()
        t2.join()
    finally:
        pr.disable_preemption()
        
    # With preemption, tasks A and B should interleave.
    # Calculate the number of times execution switched between A and B in the timeline.
    switches = 0
    for i in range(1, len(timestamps)):
        if timestamps[i][0] != timestamps[i - 1][0]:
            switches += 1
            
    print(f"Preemptive execution timestamps: {timestamps}")
    print(f"Number of switches detected: {switches}")
    
    # We expect multiple context switches (at least 2, but usually many more depending on CPU speed)
    assert switches >= 2

def test_preemption_after_voluntary_yield():
    # Enable preemption with small time slice
    pr.enable_preemption(time_slice=0.005, check_interval=50)
    
    timestamps = []
    
    def mixed_task(name, duration):
        # 1. CPU burn to force at least one preemption
        start = time.monotonic()
        while time.monotonic() - start < duration:
            compute_step()
            timestamps.append((name, "cpu1"))
        
        # 2. Voluntary yield (sleep)
        pr.sleep(0.01)
        timestamps.append((name, "yield"))
        
        # 3. CPU burn again to check if preemption still works
        start = time.monotonic()
        while time.monotonic() - start < duration:
            compute_step()
            timestamps.append((name, "cpu2"))

    try:
        t1 = pr.spawn(mixed_task, "A", 0.05)
        t2 = pr.spawn(mixed_task, "B", 0.05)
        t1.join()
        t2.join()
    finally:
        pr.disable_preemption()

    # If preemption works in phase 3 (cpu2), we expect interleaving in the "cpu2" phase.
    # Let's filter timestamps for cpu2
    cpu2_sequence = [name for name, step in timestamps if step == "cpu2"]
    
    # Check if there's any context switch in the cpu2 phase
    switches = 0
    for i in range(1, len(cpu2_sequence)):
        if cpu2_sequence[i] != cpu2_sequence[i - 1]:
            switches += 1
            
    print(f"cpu2 sequence: {cpu2_sequence}")
    print(f"cpu2 switches: {switches}")
    assert switches >= 2


def teardown_module(module):

    # Shutdown and reset global scheduler to allow other tests to start a fresh scheduler with default max procs
    import pyroutine._runtime as rt
    if rt._global_scheduler is not None:
        rt._global_scheduler.shutdown(wait=True)
        rt._global_scheduler = None
        rt._configured_nprocs = None
