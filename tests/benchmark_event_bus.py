# tests/benchmark_event_bus.py
"""
InferX Event Bus Performance Benchmark.

Measures event throughput (events/sec) and publishing latencies
under sustained high-frequency loads.
"""
import asyncio
import time
from typing import List

from inferx.event_bus.bus import EventBus
from inferx.event_bus.envelope import EventEnvelope
from inferx.event_bus.events import RequestReceived
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def run_benchmark(event_count: int = 50000) -> None:
    """
    Executes a high-frequency throughput benchmark on the EventBus.
    
    Spawns concurrent publisher and subscriber loops and calculates processing metrics.
    """
    bus = EventBus(queue_capacity=event_count + 10)
    sub_id = bus.subscribe("RequestReceived")
    queue = bus.get_queue(sub_id)

    # 1. Pre-generate payloads to isolate benchmark to EventBus routing time
    payload = RequestReceived(
        request_id="req-benchmark",
        model_name="llama",
        tenant_id="benchmark",
        payload_size_bytes=512
    )
    envelopes = [EventEnvelope.create_from_payload(payload) for _ in range(event_count)]

    logger.info(f"Starting EventBus benchmark with {event_count} events...", component="benchmark")
    
    start_time = time.perf_counter()

    # 2. Spawn concurrent consumer task
    async def consumer() -> int:
        received_count = 0
        while received_count < event_count:
            await queue.get()
            received_count += 1
            queue.task_done()
        return received_count

    consumer_task = asyncio.create_task(consumer())

    # 3. Publish events as fast as possible
    for env in envelopes:
        await bus.publish(env)

    # Wait for consumer to ingest all messages
    await consumer_task
    
    end_time = time.perf_counter()
    duration = end_time - start_time
    throughput = event_count / duration

    # 4. Print benchmark statistics
    print("\n" + "="*50)
    print("INFERX EVENT BUS BENCHMARK RESULTS")
    print("="*50)
    print(f"Total Events Processed : {event_count}")
    print(f"Time Taken (seconds)   : {duration:.4f} s")
    print(f"Throughput (events/sec): {throughput:.2f} msg/sec")
    print(f"Average Latency (ms)   : {(duration / event_count) * 1000:.6f} ms")
    print("="*50 + "\n")


if __name__ == "__main__":
    asyncio.run(run_benchmark(50000))
