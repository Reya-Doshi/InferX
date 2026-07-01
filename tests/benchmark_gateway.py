# tests/benchmark_gateway.py
"""
InferX Gateway Throughput Benchmark.

Measures REST predict request throughput (requests/sec) and average latencies.
"""

import asyncio
import time
import json
from typing import List

from inferx.admission.limiter import TokenBucketLimiter
from inferx.admission.manager import AdmissionManager
from inferx.admission.shedder import BackpressureController, LoadShedder, CircuitBreaker
from inferx.core.context import RuntimeContext
from inferx.gateway.router import GatewayRouter
from inferx.gateway.middleware import MiddlewarePipeline
from inferx.gateway.protocols import RestAdapter
from inferx.gateway.manager import GatewayManager
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def mock_predict(model_name: str, version: str, prompt: str) -> str:
    # Fast mock prediction
    return "ok"


async def client_worker(host: str, port: int, count: int, payload: str) -> List[float]:
    """Client task sending requests sequentially over TCP connections."""
    latencies = []

    for _ in range(count):
        t_start = time.perf_counter()
        try:
            reader, writer = await asyncio.open_connection(host, port)

            headers = (
                "POST /predict HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "Content-Type: application/json\r\n"
                "Authorization: Bearer sk-valid-key\r\n"
                f"Content-Length: {len(payload)}\r\n\r\n"
            )
            writer.write(headers.encode("utf-8") + payload.encode("utf-8"))
            await writer.drain()

            await reader.read(1024)
            writer.close()
            await writer.wait_closed()

            latencies.append(time.perf_counter() - t_start)
        except Exception:
            pass

    return latencies


async def run_gateway_benchmark(
    client_count: int = 10, reqs_per_client: int = 200
) -> None:
    """Spawns concurrent client workers and evaluates request throughput."""
    context = RuntimeContext()
    context.update_telemetry(vram_util=0.5, cpu_util=0.4, avg_queue_lat=0.0)

    # High limits to prevent rate limits
    limiter = TokenBucketLimiter(100000.0, 100000.0)
    backpressure = BackpressureController()
    shedder = LoadShedder(backpressure)
    circuit_breaker = CircuitBreaker()
    admission = AdmissionManager(context, limiter, shedder, circuit_breaker)

    pipeline = MiddlewarePipeline(
        admission_manager=admission, allowed_api_keys=["sk-valid-key"]
    )
    router = GatewayRouter()

    rest_adapter = RestAdapter(pipeline, router, mock_predict)
    manager = GatewayManager(host="127.0.0.1", port=0, rest_adapter=rest_adapter)
    await manager.start()

    payload = json.dumps({"prompt": "benchmark_prompt", "model": "llama"})
    total_reqs = client_count * reqs_per_client

    print("\n" + "=" * 70)
    print(f"INFERX GATEWAY THROUGHPUT BENCHMARK (Requests: {total_reqs})")
    print("=" * 70)

    start_time = time.perf_counter()

    # Spawn concurrent client workers
    tasks = [
        asyncio.create_task(
            client_worker(manager.host, manager.port, reqs_per_client, payload)
        )
        for _ in range(client_count)
    ]

    results = await asyncio.gather(*tasks)

    duration = time.perf_counter() - start_time
    throughput = total_reqs / duration

    # Compile latencies
    all_latencies = []
    for worker_latencies in results:
        all_latencies.extend(worker_latencies)

    all_latencies.sort()
    avg_us = (sum(all_latencies) / len(all_latencies)) * 1e6 if all_latencies else 0.0
    p95_us = (
        all_latencies[int(len(all_latencies) * 0.95)] * 1e6 if all_latencies else 0.0
    )

    print(f"Total Requests Completed  : {len(all_latencies)}")
    print(f"Total Time Taken          : {duration:.4f} s")
    print(f"Throughput (requests/sec) : {throughput:.2f} req/sec")
    print(f"Average Latency           : {avg_us:.2f} us")
    print(f"p95 Latency               : {p95_us:.2f} us")
    print("=" * 70 + "\n")

    await manager.stop()


if __name__ == "__main__":
    # Import List dynamically to avoid namespace clashes
    from typing import List

    asyncio.run(run_gateway_benchmark(15, 200))
