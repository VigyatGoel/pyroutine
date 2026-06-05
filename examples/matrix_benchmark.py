import time
import random
from array import array
import pyroutine as pr

# Matrix dimension and number of tasks
N = 120
TASKS = 8

def make_matrix(n):
    # Return a flat double-precision array of size n*n
    return array('d', [random.random() for _ in range(n * n)])

# Multiply a subset of rows from matrix A with matrix B, storing in C
def multiply_rows(A, B, start_row, end_row, C, n):
    for i in range(start_row, end_row):
        for j in range(n):
            val = 0.0
            for k in range(n):
                val += A[i * n + k] * B[k * n + j]
            C[i * n + j] = val

def run_pyroutine_matrix(A, B):
    C = array('d', [0.0] * (N * N))
    rows_per_task = N // TASKS
    
    handles = []
    for i in range(TASKS):
        start = i * rows_per_task
        end = N if i == TASKS - 1 else (i + 1) * rows_per_task
        # Spawn a task to compute rows from start to end
        h = pr.spawn(multiply_rows, A, B, start, end, C, N)
        handles.append(h)
        
    pr.gather(*handles)
    return C

def run_serial_matrix(A, B):
    C = array('d', [0.0] * (N * N))
    multiply_rows(A, B, 0, N, C, N)
    return C

if __name__ == "__main__":
    A = make_matrix(N)
    B = make_matrix(N)
    
    print(f"Multiplying {N}x{N} flat arrays using {TASKS} parallel tasks...")
    
    # Serial baseline
    t0 = time.monotonic()
    C_serial = run_serial_matrix(A, B)
    t_serial = time.monotonic() - t0
    print(f"Serial:      {t_serial:.3f}s")
    
    # Parallel pyroutine
    t0 = time.monotonic()
    C_parallel = run_pyroutine_matrix(A, B)
    t_parallel = time.monotonic() - t0
    print(f"pyroutine:   {t_parallel:.3f}s (Speedup: {t_serial/t_parallel:.2f}x)")
    
    # Sanity check
    assert C_serial == C_parallel, "Results do not match!"
    print("Verification passed (results match).")
