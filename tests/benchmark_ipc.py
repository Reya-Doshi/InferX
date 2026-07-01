# tests/benchmark_ipc.py
"""
InferX IPC Performance Benchmark.

Compares standard multiprocessing.Queue payload serialization transfer times
versus our SharedMemoryPool zero-copy transfer times across process boundaries.
"""

import time
import multiprocessing

from inferx.worker.ipc import SharedMemoryPool
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


def queue_worker_target(
    request_queue: multiprocessing.Queue,
    response_queue: multiprocessing.Queue,
    count: int,
) -> None:
    """Consumes raw payload strings directly from the queue."""
    for _ in range(count):
        try:
            payload = request_queue.get(timeout=2.0)
            response_queue.put(len(payload))
        except Exception:
            break


def shm_worker_target(
    request_queue: multiprocessing.Queue,
    response_queue: multiprocessing.Queue,
    shm_name: str,
    shm_size: int,
    count: int,
) -> None:
    """Consumes metadata offsets and reads directly from shared memory."""
    shm = SharedMemoryPool(name=shm_name, size=shm_size, create=False)
    for _ in range(count):
        try:
            # Get metadata: (offset, size)
            offset, size = request_queue.get(timeout=2.0)
            payload = shm.read(offset, size)
            response_queue.put(len(payload))
        except Exception:
            break
    shm.close()


def run_queue_benchmark(payload_size: int, count: int) -> float:
    """Measures transfer latency using standard multiprocessing.Queue."""
    ctx = multiprocessing.get_context("spawn")
    req_queue = ctx.Queue()
    res_queue = ctx.Queue()

    payload = b"x" * payload_size

    proc = ctx.Process(target=queue_worker_target, args=(req_queue, res_queue, count))
    proc.start()

    # Pre-warm queues
    time.sleep(0.1)

    start_time = time.perf_counter()

    for _ in range(count):
        req_queue.put(payload)
        res_queue.get()

    duration = time.perf_counter() - start_time

    proc.join()
    return (duration / count) * 1e6


def run_shm_benchmark(payload_size: int, count: int) -> float:
    """Measures transfer latency using zero-copy SharedMemoryPool."""
    shm_name = "inferx_benchmark_shm_pool"
    shm_size = max(1024 * 1024, payload_size * 2)

    # Allocate pool
    shm = SharedMemoryPool(name=shm_name, size=shm_size, create=True)
    ctx = multiprocessing.get_context("spawn")
    req_queue = ctx.Queue()
    res_queue = ctx.Queue()

    payload = b"x" * payload_size
    offset = 0

    proc = ctx.Process(
        target=shm_worker_target, args=(req_queue, res_queue, shm_name, shm_size, count)
    )
    proc.start()

    time.sleep(0.1)

    start_time = time.perf_counter()

    for _ in range(count):
        # Zero-copy write to offset
        shm.write(offset, payload)

        # Enqueue metadata offset/size package
        req_queue.put((offset, payload_size))
        res_queue.get()

    duration = time.perf_counter() - start_time

    proc.join()
    shm.close()
    shm.unlink()
    return (duration / count) * 1e6


def main() -> None:
    count = 1000
    print("\n" + "=" * 70)
    print(f"INFERX IPC PERFORMANCE BENCHMARK (Messages: {count})")
    print("=" * 70)
    print(
        f"{'Payload Size':<15} | {'Queue Latency (us)':<22} | {'Zero-Copy SHM (us)':<22} | {'Speedup':<10}"
    )
    print("-" * 70)

    # Benchmark sizes: 1KB, 10KB, 100KB, 1MB
    sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024]

    for size in sizes:
        # Standard Queue
        q_lat = run_queue_benchmark(size, count)
        # Shared Memory Pool
        shm_lat = run_shm_benchmark(size, count)

        speedup = q_lat / shm_lat

        size_str = (
            f"{size / 1024:.0f} KB"
            if size < 1024 * 1024
            else f"{size / (1024 * 1024):.0f} MB"
        )
        print(
            f"{size_str:<15} | {q_lat:20.2f} us | {shm_lat:20.2f} us | {speedup:.2f}x"
        )

    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
