# tests/test_scheduler.py
"""
InferX Scheduler Test Suite.

Verifies chronological FIFO, priority heap sort, deadline EDF sorting,
Weighted Fair Queue/DRR balance, priority aging starvation checks,
and concurrent async producer-consumer loops.
"""
import asyncio
import unittest
from typing import List

from inferx.scheduler.interfaces import ScheduledRequest
from inferx.scheduler.manager import Scheduler
from inferx.scheduler.policies import (
    FIFOPolicy,
    PriorityQueuePolicy,
    DeadlinePolicy,
    WeightedFairQueuePolicy,
    PriorityAgingPolicy,
    AdaptivePolicy
)


class TestSchedulerPolicies(unittest.IsolatedAsyncioTestCase):
    """Unit tests verifying all sorting policies in the Scheduling Engine."""

    def build_request(
        self,
        request_id: str,
        tenant_id: str = "t1",
        priority: int = 0,
        max_latency_ms: float = 30000.0
    ) -> ScheduledRequest:
        """Helper to construct ScheduledRequest models."""
        return ScheduledRequest(
            request_id=request_id,
            tenant_id=tenant_id,
            priority=priority,
            max_latency_ms=max_latency_ms,
            payload=b"test_bytes"
        )

    async def test_fifo_policy(self) -> None:
        scheduler = Scheduler(FIFOPolicy())
        await scheduler.start()

        r1 = self.build_request("r1")
        r2 = self.build_request("r2")
        r3 = self.build_request("r3")

        await scheduler.enqueue(r1)
        await scheduler.enqueue(r2)
        await scheduler.enqueue(r3)

        self.assertEqual(scheduler.size(), 3)

        p1 = await scheduler.dequeue()
        p2 = await scheduler.dequeue()
        p3 = await scheduler.dequeue()

        # FIFO ordering: r1 -> r2 -> r3
        self.assertEqual(p1.request_id, "r1")
        self.assertEqual(p2.request_id, "r2")
        self.assertEqual(p3.request_id, "r3")
        
        await scheduler.stop()

    async def test_priority_queue_policy(self) -> None:
        scheduler = Scheduler(PriorityQueuePolicy())
        await scheduler.start()

        # High priority value represents high urgency
        r_low = self.build_request("r_low", priority=1)
        r_high = self.build_request("r_high", priority=5)
        r_med = self.build_request("r_med", priority=3)

        await scheduler.enqueue(r_low)
        await scheduler.enqueue(r_high)
        await scheduler.enqueue(r_med)

        p1 = await scheduler.dequeue()
        p2 = await scheduler.dequeue()
        p3 = await scheduler.dequeue()

        # Priority ordering: high (5) -> med (3) -> low (1)
        self.assertEqual(p1.request_id, "r_high")
        self.assertEqual(p2.request_id, "r_med")
        self.assertEqual(p3.request_id, "r_low")

        await scheduler.stop()

    async def test_deadline_edf_policy(self) -> None:
        scheduler = Scheduler(DeadlinePolicy())
        await scheduler.start()

        # Closest deadline (smallest max_latency_ms) should pop first
        r_late = self.build_request("r_late", max_latency_ms=50000.0)
        r_early = self.build_request("r_early", max_latency_ms=5000.0)
        r_mid = self.build_request("r_mid", max_latency_ms=25000.0)

        await scheduler.enqueue(r_late)
        await scheduler.enqueue(r_early)
        await scheduler.enqueue(r_mid)

        p1 = await scheduler.dequeue()
        p2 = await scheduler.dequeue()
        p3 = await scheduler.dequeue()

        # EDF ordering: early -> mid -> late
        self.assertEqual(p1.request_id, "r_early")
        self.assertEqual(p2.request_id, "r_mid")
        self.assertEqual(p3.request_id, "r_late")

        await scheduler.stop()

    async def test_deficit_round_robin_fairness(self) -> None:
        # Tenant weights: A has 3, B has 1. A gets 3x more execution slots.
        weights = {"tenant-A": 3, "tenant-B": 1}
        scheduler = Scheduler(WeightedFairQueuePolicy(tenant_weights=weights))
        await scheduler.start()

        # Enqueue 4 requests for each tenant
        for i in range(4):
            await scheduler.enqueue(self.build_request(f"A-{i}", tenant_id="tenant-A"))
            await scheduler.enqueue(self.build_request(f"B-{i}", tenant_id="tenant-B"))

        # Dequeue 4 items and verify weights (should return A, A, A, B)
        results = [await scheduler.dequeue() for _ in range(4)]
        
        a_count = sum(1 for r in results if r.tenant_id == "tenant-A")
        b_count = sum(1 for r in results if r.tenant_id == "tenant-B")

        self.assertEqual(a_count, 3)
        self.assertEqual(b_count, 1)
        self.assertEqual(results[3].tenant_id, "tenant-B")

        await scheduler.stop()

    async def test_priority_aging_starvation_prevention(self) -> None:
        # High aging rate to see fast state updates
        policy = PriorityAgingPolicy(aging_rate_per_sec=500.0)
        scheduler = Scheduler(policy, aging_interval_ms=10)
        await scheduler.start()

        # 1. Enqueue low-priority request
        r_low = self.build_request("low_priority", priority=1)
        await scheduler.enqueue(r_low)

        # Let the background aging loop increment aged priority of r_low
        await asyncio.sleep(0.05)

        # 2. Enqueue high-priority request
        r_high = self.build_request("high_priority", priority=10)
        await scheduler.enqueue(r_high)

        # If priority aging works, r_low's priority is boosted above r_high, popping first
        p1 = await scheduler.dequeue()
        p2 = await scheduler.dequeue()

        self.assertEqual(p1.request_id, "low_priority")
        self.assertEqual(p2.request_id, "high_priority")

        await scheduler.stop()

    async def test_adaptive_policy_congestion(self) -> None:
        aging_policy = PriorityAgingPolicy(aging_rate_per_sec=1.0)
        adaptive_policy = AdaptivePolicy(
            base_policy=aging_policy,
            congestion_threshold=3,
            boosted_aging_rate=10.0
        )
        scheduler = Scheduler(adaptive_policy)
        await scheduler.start()

        # Queue depth below threshold (size = 1) -> normal aging rate
        await scheduler.enqueue(self.build_request("r1"))
        self.assertEqual(aging_policy.aging_rate, 1.0)

        # Queue depth reaches threshold (size = 3) -> boosted aging rate
        await scheduler.enqueue(self.build_request("r2"))
        await scheduler.enqueue(self.build_request("r3"))
        
        # Adaptive check is triggered on push/pop
        self.assertEqual(aging_policy.aging_rate, 10.0)

        # Consume all requests
        await scheduler.dequeue()
        await scheduler.dequeue()
        await scheduler.dequeue()

        # Queue depth below threshold (size = 0) -> normal aging rate
        self.assertEqual(aging_policy.aging_rate, 1.0)
        await scheduler.stop()

    async def test_concurrency_conditions(self) -> None:
        scheduler = Scheduler(PriorityQueuePolicy())
        await scheduler.start()
        
        consumed: List[str] = []
        
        # Spawn multiple concurrent producer tasks
        async def producer(tid: int) -> None:
            for i in range(10):
                req = self.build_request(f"p-{tid}-{i}", priority=i)
                await scheduler.enqueue(req)
                await asyncio.sleep(0.001)

        # Spawn multiple concurrent consumer tasks
        async def consumer() -> None:
            for _ in range(20):
                req = await scheduler.dequeue()
                consumed.append(req.request_id)
                await asyncio.sleep(0.001)

        # Run 2 producers and 1 consumer concurrently (total 20 requests)
        await asyncio.gather(
            producer(1),
            producer(2),
            consumer()
        )

        self.assertEqual(len(consumed), 20)
        await scheduler.stop()


if __name__ == "__main__":
    unittest.main()
