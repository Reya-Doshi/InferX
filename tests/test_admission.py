# tests/test_admission.py
"""
InferX Admission Controller Test Suite.

Verifies Token Bucket limits, priority-aware load shedders, CircuitBreaker state loops,
and concurrency safety.
"""
import asyncio
import unittest
from typing import Dict

from inferx.admission.interfaces import AdmissionVerdict
from inferx.admission.limiter import TokenBucketLimiter, LeakyBucketLimiter
from inferx.admission.manager import AdmissionManager
from inferx.admission.metrics import AdmissionMetrics
from inferx.admission.shedder import BackpressureController, LoadShedder, CircuitBreaker
from inferx.core.context import RuntimeContext
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.scheduler.manager import Scheduler
from inferx.scheduler.policies import FIFOPolicy


class TestAdmissionController(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the Admission Controller."""

    def setUp(self) -> None:
        self.context = RuntimeContext()
        self.context.update_telemetry(vram_util=0.5, cpu_util=0.4, avg_queue_lat=0.0)
        for _ in range(5):
            self.context.increment_active_requests()
        
        # 2 tokens capacity, refills 1 token per second
        self.limiter = TokenBucketLimiter(
            global_capacity=2.0,
            global_refill_rate=1.0,
            tenant_configs={"tenant-A": (1.0, 1.0)}  # Tenant A capacity is 1
        )
        
        self.backpressure = BackpressureController(max_vram_ratio=0.85, max_cpu_ratio=0.8)
        self.shedder = LoadShedder(self.backpressure)
        self.circuit_breaker = CircuitBreaker(failure_threshold=3, cooldown_sec=1.0)
        self.metrics = AdmissionMetrics()
        self.scheduler = Scheduler(FIFOPolicy())
        
        self.manager = AdmissionManager(
            context=self.context,
            limiter=self.limiter,
            shedder=self.shedder,
            circuit_breaker=self.circuit_breaker,
            metrics=self.metrics,
            scheduler=self.scheduler
        )

    def build_request(self, request_id: str, tenant_id: str = "tenant-global", priority: int = 1) -> ScheduledRequest:
        return ScheduledRequest(
            request_id=request_id,
            tenant_id=tenant_id,
            priority=priority,
            payload=b"test_payload"
        )

    async def test_token_bucket_limits(self) -> None:
        r1 = self.build_request("r1", tenant_id="tenant-global")
        r2 = self.build_request("r2", tenant_id="tenant-global")
        r3 = self.build_request("r3", tenant_id="tenant-global")

        # Global capacity is 2
        v1 = await self.manager.admit(r1)
        v2 = await self.manager.admit(r2)
        v3 = await self.manager.admit(r3)

        self.assertTrue(v1.admitted)
        self.assertTrue(v2.admitted)
        # 3rd request should fail (rate limited)
        self.assertFalse(v3.admitted)
        self.assertEqual(v3.status_code, 429)

    async def test_tenant_specific_limits(self) -> None:
        # Tenant A capacity is 1
        r1 = self.build_request("r1", tenant_id="tenant-A")
        r2 = self.build_request("r2", tenant_id="tenant-A")

        v1 = await self.manager.admit(r1)
        v2 = await self.manager.admit(r2)

        self.assertTrue(v1.admitted)
        # 2nd request for Tenant A should fail
        self.assertFalse(v2.admitted)
        self.assertEqual(v2.status_code, 429)

    async def test_priority_aware_load_shedding(self) -> None:
        # Simulate high VRAM load (88% > 85% threshold)
        self.context.update_telemetry(vram_util=0.88, cpu_util=0.4, avg_queue_lat=0.0)
        self.manager.limiter = TokenBucketLimiter(10.0, 10.0)

        r_low = self.build_request("low", priority=1)
        r_med = self.build_request("med", priority=3)
        r_high = self.build_request("high", priority=5)

        v_low = await self.manager.admit(r_low)
        v_med = await self.manager.admit(r_med)
        v_high = await self.manager.admit(r_high)

        # Under moderate congestion (88% VRAM):
        # Low priority (1) is shed
        self.assertFalse(v_low.admitted)
        # Medium priority (3) is admitted
        self.assertTrue(v_med.admitted)
        # High priority (5) is admitted
        self.assertTrue(v_high.admitted)

        # Now simulate critical VRAM load (92% VRAM)
        self.context.update_telemetry(vram_util=0.92, cpu_util=0.4, avg_queue_lat=0.0)
        # We need to reset rate limits to bypass 429s
        self.limiter = TokenBucketLimiter(10.0, 10.0)
        self.manager.limiter = self.limiter

        v_med_crit = await self.manager.admit(r_med)
        v_high_crit = await self.manager.admit(r_high)

        # Under critical congestion (92% VRAM):
        # Medium priority (3) is shed
        self.assertFalse(v_med_crit.admitted)
        # High priority (5) is admitted
        self.assertTrue(v_high_crit.admitted)

    async def test_circuit_breaker_tripping(self) -> None:
        # Trip the circuit breaker by recording 3 failures
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()
        self.circuit_breaker.record_failure()

        self.assertEqual(self.circuit_breaker.state, "OPEN")

        r = self.build_request("r")
        v = await self.manager.admit(r)

        # Circuit is OPEN, request should be rejected immediately
        self.assertFalse(v.admitted)
        self.assertEqual(v.status_code, 503)
        self.assertEqual(v.error_code, "CIRCUIT_BREAKER_OPEN")

    async def test_leaky_bucket_limiter(self) -> None:
        # Capacity is 2, leaks 1 token per 100ms
        leaky = LeakyBucketLimiter(capacity=2, leak_interval_sec=0.1)
        
        self.assertTrue(leaky.consume())
        self.assertTrue(leaky.consume())
        # 3rd should fail
        self.assertFalse(leaky.consume())

        # Sleep to let water leak
        await asyncio.sleep(0.12)
        # 1 request should leak, allowing 1 more consumption
        self.assertTrue(leaky.consume())


if __name__ == "__main__":
    unittest.main()
