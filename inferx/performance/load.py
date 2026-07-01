# inferx/performance/load.py
import asyncio
import time
import random
from typing import Any, Callable, Coroutine, List, Optional
from inferx.performance.interfaces import ILoadGenerator


class LoadGenerator(ILoadGenerator):
    """Simulates concurrent client traffic, bursts, and steady request generation against target handlers."""

    def __init__(
        self, request_fn: Optional[Callable[[], Coroutine[Any, Any, Any]]] = None
    ) -> None:
        # Default mock request function if none provided
        self.request_fn = request_fn or self._default_mock_request

    async def _default_mock_request(self) -> None:
        # Simulates 5ms to 15ms request processing time
        await asyncio.sleep(random.uniform(0.005, 0.015))

    async def generate_steady_load(
        self, rps: float, duration_sec: float
    ) -> List[float]:
        """Generates steady RPS stream of requests using asynchronous tasks."""
        latencies: List[float] = []
        1.0 / rps
        start_time = time.perf_counter()

        async def run_single_request() -> None:
            t0 = time.perf_counter()
            try:
                await self.request_fn()
            except Exception:
                pass
            latencies.append((time.perf_counter() - t0) * 1000.0)

        tasks = []
        sent_count = 0
        while True:
            elapsed = time.perf_counter() - start_time
            if elapsed >= duration_sec:
                break

            # Catch up with target counts based on elapsed duration
            expected_sent = int(elapsed * rps)
            if sent_count < expected_sent:
                batch = expected_sent - sent_count
                for _ in range(batch):
                    tasks.append(asyncio.create_task(run_single_request()))
                sent_count += batch

            await asyncio.sleep(0.002)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return latencies

    async def generate_burst_load(
        self, concurrent_users: int, burst_size: int
    ) -> List[float]:
        """Dispatches large batches of concurrent users in parallel to simulate traffic spikes."""
        latencies: List[float] = []

        async def run_single_request() -> None:
            t0 = time.perf_counter()
            try:
                await self.request_fn()
            except Exception:
                pass
            latencies.append((time.perf_counter() - t0) * 1000.0)

        # Build list of calls to execute in parallel
        tasks = []
        # Total requests is concurrent_users * burst_size
        total_requests = concurrent_users * burst_size
        for _ in range(total_requests):
            tasks.append(asyncio.create_task(run_single_request()))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return latencies
