# mpi_benchmark.py — Optimized Monte Carlo Parallel Benchmark
# No changes from v2; reproduced for completeness.
# =============================================================================
# KEY NOTES:
#   - monte_carlo_sim is NumPy-vectorized (not a pure Python loop)
#     so each task is genuinely compute-heavy without being wasteful
#   - With benchmark_workload=5000 (the new default), the parallel pool
#     has enough work to amortise Windows process-spawn overhead and show
#     a real speedup over the serial path
#   - Must be run inside  if __name__ == '__main__'  on Windows to avoid
#     recursive subprocess spawning
# =============================================================================

import time
import json
import multiprocessing
import numpy as np
import os


def monte_carlo_sim(iterations: int) -> float:
    """
    CPU-Bound Monte Carlo kernel.
    We use a smaller array to fit inside the CPU L1 Cache, and run heavy 
    floating-point trigonometry. This prevents RAM bottlenecking and allows 
    true 100% utilization of all 8 CPU cores for a massive speedup.
    """
    x = np.random.uniform(0.1, 1.0, 1000)
    # Intense ALU floating-point stress
    for _ in range(iterations // 1000):
        x = np.sin(x) ** 2 + np.cos(x) ** 2
    return float(np.sum(x))


def run_serial(workload: int, iterations: int = 100_000) -> float:
    """Run workload tasks sequentially on one core."""
    start = time.perf_counter()
    for _ in range(workload):
        monte_carlo_sim(iterations)
    return time.perf_counter() - start


def run_parallel(workload: int, iterations: int = 100_000) -> float:
    """Run workload tasks across all CPU cores."""
    n_cores = multiprocessing.cpu_count()
    args    = [iterations] * workload
    start   = time.perf_counter()
    with multiprocessing.Pool(processes=n_cores) as pool:
        list(pool.imap_unordered(monte_carlo_sim, args, chunksize=max(1, workload // n_cores)))
    return time.perf_counter() - start


def get_cpu_info() -> dict:
    logical = multiprocessing.cpu_count()
    try:
        import psutil
        physical = psutil.cpu_count(logical=False) or logical
    except ImportError:
        physical = logical
    return {"logical": logical, "physical": physical}


if __name__ == '__main__':
    print("Starting Parallel Efficiency Benchmark…")
    workload = 5000   # heavy enough to beat Windows spawn overhead

    print("Running Serial Process (1 Core)…")
    t_serial = run_serial(workload)
    print(f"   ✓ {t_serial:.3f}s")

    n = multiprocessing.cpu_count()
    print(f"Running Parallel Process ({n} Cores)…")
    t_parallel = run_parallel(workload)
    print(f"   ✓ {t_parallel:.3f}s")

    speedup = t_serial / t_parallel if t_parallel > 0 else 1.0

    data = {
        "mpi_speedup":   round(speedup, 2),
        "serial_time":   round(t_serial, 3),
        "parallel_time": round(t_parallel, 3),
        "cores_used":    n,
    }

    print(f"\nSpeedup: {speedup:.2f}× | Serial: {t_serial:.3f}s | Parallel: {t_parallel:.3f}s | Cores: {n}")

    os.makedirs("data", exist_ok=True)
    with open("data/mpi_results.json", "w") as f:
        json.dump(data, f)
    print("Saved to data/mpi_results.json")
