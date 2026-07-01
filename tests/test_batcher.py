# tests/test_batcher.py
"""
InferX Batcher Test Suite.

Verifies static size/timeout batching, Pydantic padding layouts, ShapeBucketeer bins,
continuous iteration steps, and batch splits and merges.
"""
import asyncio
import unittest
from typing import List

from inferx.batcher.engine import StaticBatcher, ContinuousBatcher, AdaptiveBatcher, split_batch, merge_batches
from inferx.batcher.interfaces import Batch, IBatchHandler
from inferx.batcher.padding import pad_tensors, ShapeBucketeer
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.scheduler.manager import Scheduler
from inferx.scheduler.policies import FIFOPolicy


class MockBatchHandler(IBatchHandler):
    """Mocks the execution target, storing received batches for inspection."""
    def __init__(self) -> None:
        self.batches: List[Batch] = []

    async def handle_batch(self, batch: Batch) -> None:
        self.batches.append(batch)


class TestBatcher(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the Dynamic Batcher."""

    def build_request(self, request_id: str, tokens: List[int], max_tokens: int = 10) -> ScheduledRequest:
        return ScheduledRequest(
            request_id=request_id,
            tenant_id="t1",
            priority=1,
            payload=tokens if max_tokens == 10 else {"max_tokens": max_tokens},
            max_latency_ms=30000.0
        )

    async def test_static_batching_by_size(self) -> None:
        scheduler = Scheduler(FIFOPolicy())
        await scheduler.start()
        
        handler = MockBatchHandler()
        batcher = StaticBatcher(scheduler, handler, max_batch_size=3, max_queue_delay_ms=5000)
        await batcher.start()

        # Enqueue 3 requests
        await scheduler.enqueue(self.build_request("r1", [1, 2]))
        await scheduler.enqueue(self.build_request("r2", [1, 2, 3]))
        await scheduler.enqueue(self.build_request("r3", [1]))

        # Yield control to let async batch loop run
        await asyncio.sleep(0.05)

        # Batch should be flushed immediately on size match (size=3)
        self.assertEqual(len(handler.batches), 1)
        batch = handler.batches[0]
        self.assertEqual(len(batch.requests), 3)
        
        await batcher.stop()
        await scheduler.stop()

    async def test_static_batching_by_timeout(self) -> None:
        scheduler = Scheduler(FIFOPolicy())
        await scheduler.start()
        
        handler = MockBatchHandler()
        # Fast timeout of 50ms
        batcher = StaticBatcher(scheduler, handler, max_batch_size=10, max_queue_delay_ms=50)
        await batcher.start()

        # Enqueue only 2 requests (below max_batch_size=10)
        await scheduler.enqueue(self.build_request("r1", [1]))
        await scheduler.enqueue(self.build_request("r2", [1, 2]))

        # Wait for timeout to expire (50ms + safety margin)
        await asyncio.sleep(0.15)

        # Batch should be flushed on timeout
        self.assertEqual(len(handler.batches), 1)
        batch = handler.batches[0]
        self.assertEqual(len(batch.requests), 2)

        await batcher.stop()
        await scheduler.stop()

    def test_tensor_padding_alignment(self) -> None:
        r1 = self.build_request("r1", [1, 2])
        r2 = self.build_request("r2", [1, 2, 3, 4])
        r3 = self.build_request("r3", [1])

        padded, shape = pad_tensors([r1, r2, r3], pad_token=0)

        # Shape should be [3 requests, max_seq_len=4]
        self.assertEqual(shape, [3, 4])
        self.assertEqual(padded[0], [1, 2, 0, 0])
        self.assertEqual(padded[1], [1, 2, 3, 4])
        self.assertEqual(padded[2], [1, 0, 0, 0])

    def test_shape_bucketing(self) -> None:
        bucketeer = ShapeBucketeer(thresholds=[2, 4])
        
        r_short = self.build_request("r_short", [1])       # len=1 <= 2
        r_med = self.build_request("r_med", [1, 2, 3])     # len=3 <= 4
        r_long = self.build_request("r_long", [1, 2, 3, 4, 5]) # len=5 > 4 (overflow)

        bucketeer.add_request(r_short)
        bucketeer.add_request(r_med)
        bucketeer.add_request(r_long)

        self.assertEqual(bucketeer.bucket_size(2), 1)
        self.assertEqual(bucketeer.bucket_size(4), 1)
        self.assertEqual(bucketeer.bucket_size(-1), 1)

        b2 = bucketeer.get_bucket(2)
        self.assertEqual(b2[0].request_id, "r_short")
        self.assertEqual(bucketeer.bucket_size(2), 0)  # Check cleared

    async def test_continuous_batching_steps(self) -> None:
        scheduler = Scheduler(FIFOPolicy())
        await scheduler.start()

        # Target max_tokens: 3 iterations
        r1 = self.build_request("r1", [], max_tokens=3)
        # Target max_tokens: 2 iterations
        r2 = self.build_request("r2", [], max_tokens=2)

        await scheduler.enqueue(r1)
        await scheduler.enqueue(r2)

        continuous_batcher = ContinuousBatcher(scheduler, max_batch_size=2)
        
        # Step 1: Ingests both requests, runs first generation step
        completed = await continuous_batcher.step()
        self.assertEqual(len(completed), 0)
        self.assertEqual(len(continuous_batcher.active_requests), 2)

        # Step 2: Second generation step. r2 finishes (generated 2 tokens)
        completed = await continuous_batcher.step()
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0][0].request_id, "r2")
        self.assertEqual(completed[0][1], 2)
        # r2 ejected
        self.assertEqual(len(continuous_batcher.active_requests), 1)

        # Step 3: Third generation step. r1 finishes (generated 3 tokens)
        completed = await continuous_batcher.step()
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0][0].request_id, "r1")
        # All tasks finished
        self.assertEqual(len(continuous_batcher.active_requests), 0)

        await scheduler.stop()

    def test_batch_split_and_merge_ops(self) -> None:
        r1 = self.build_request("r1", [1, 2])
        r2 = self.build_request("r2", [1, 2, 3])
        r3 = self.build_request("r3", [1])

        # Form initial batch
        padded, shape = pad_tensors([r1, r2, r3])
        batch = Batch(
            batch_id="b-init",
            requests=[r1, r2, r3],
            padded_tensors=padded,
            padded_shape=shape,
            max_tokens=30000
        )

        # Split batch into chunks of size 2
        splits = split_batch(batch, chunk_size=2)
        self.assertEqual(len(splits), 2)
        self.assertEqual(len(splits[0].requests), 2)
        self.assertEqual(len(splits[1].requests), 1)

        # Merge them back
        merged = merge_batches(splits[0], splits[1])
        self.assertEqual(len(merged.requests), 3)
        self.assertEqual(merged.padded_shape, [3, 3])  # Max length in merged is 3 ([1,2,3])
