# inferx/batcher/engine.py
"""
InferX Dynamic Batching Engine.

Implements Static dynamic batching (timeout/size bounds), Continuous iteration-level
scheduling (vLLM style), Adaptive batch sizing, and batch split/merge operations.
"""
import asyncio
from datetime import datetime, timezone
import uuid
from typing import Any, Dict, List, Optional, Tuple

from inferx.batcher.interfaces import Batch, IBatcher, IBatchHandler
from inferx.batcher.metrics import BatcherMetrics
from inferx.batcher.padding import pad_tensors
from inferx.scheduler.interfaces import IScheduler, ScheduledRequest
from inferx.utils.logging import get_logger

logger = get_logger("batcher")


def split_batch(batch: Batch, chunk_size: int) -> List[Batch]:
    """
    Splits a large Batch into smaller Batch segments.
    
    Pads and calculates new shapes for each segment.
    """
    if chunk_size <= 0:
        raise ValueError("Split chunk_size must be positive.")
    
    if len(batch.requests) <= chunk_size:
        return [batch]

    batches = []
    for i in range(0, len(batch.requests), chunk_size):
        chunk_requests = batch.requests[i : i + chunk_size]
        padded_tensors, padded_shape = pad_tensors(chunk_requests)
        
        # Recalculate max tokens for this subset
        max_tokens = max(req.max_latency_ms for req in chunk_requests) if chunk_requests else 0

        sub_batch = Batch(
            batch_id=str(uuid.uuid4()),
            requests=chunk_requests,
            padded_tensors=padded_tensors,
            padded_shape=padded_shape,
            max_tokens=int(max_tokens)
        )
        batches.append(sub_batch)
    return batches


def merge_batches(b1: Batch, b2: Batch) -> Batch:
    """
    Merges two Batch objects into a single larger Batch.
    
    Performs tensor padding alignment across all merged inputs.
    """
    merged_requests = b1.requests + b2.requests
    padded_tensors, padded_shape = pad_tensors(merged_requests)
    max_tokens = max(b1.max_tokens, b2.max_tokens)

    return Batch(
        batch_id=str(uuid.uuid4()),
        requests=merged_requests,
        padded_tensors=padded_tensors,
        padded_shape=padded_shape,
        max_tokens=max_tokens
    )


class StaticBatcher(IBatcher):
    """
    Aggregates requests into batches based on max size and timeout constraints.
    
    Coordinates enqueuing tasks asynchronously from the IScheduler.
    """
    def __init__(
        self,
        scheduler: IScheduler,
        handler: IBatchHandler,
        max_batch_size: int,
        max_queue_delay_ms: int,
        metrics: Optional[BatcherMetrics] = None
    ) -> None:
        self.scheduler = scheduler
        self.handler = handler
        self.max_batch_size = max_batch_size
        self.max_queue_delay_sec = max_queue_delay_ms / 1000.0
        self.metrics = metrics or BatcherMetrics()
        
        self._batch_task: Optional[asyncio.Task[None]] = None
        self._is_active = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Launches the background batch polling task loop."""
        async with self._lock:
            if self._is_active:
                return
            self._is_active = True
            self._batch_task = asyncio.create_task(self._batch_loop())
            logger.info(
                f"Static batcher active (max_size: {self.max_batch_size}, timeout: {self.max_queue_delay_sec}s).",
                component="batcher"
            )

    async def stop(self) -> None:
        """Gracefully cancels the loop and flushes remaining requests."""
        async with self._lock:
            self._is_active = False
            if self._batch_task:
                self._batch_task.cancel()
                try:
                    await self._batch_task
                except asyncio.CancelledError:
                    pass
                self._batch_task = None
            
            # Flush any remaining items in the scheduler queues
            await self._flush_remaining()

    async def _batch_loop(self) -> None:
        """Core assembly loop pulling items and checking timeouts."""
        while self._is_active:
            try:
                # 1. Block until the first request is available in the scheduler
                first_req = await self.scheduler.dequeue()
                
                accumulated = [first_req]
                start_time = asyncio.get_event_loop().time()
                
                # 2. Accumulate up to max_batch_size or until timeout expires
                while len(accumulated) < self.max_batch_size:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_time = self.max_queue_delay_sec - elapsed
                    
                    if remaining_time <= 0:
                        break
                    
                    # Check if scheduler has items (non-blocking size check)
                    if self.scheduler.size() > 0:
                        next_req = await self.scheduler.dequeue()
                        accumulated.append(next_req)
                    else:
                        # Sleep briefly to avoid busy-wait cycles
                        await asyncio.sleep(0.001)

                # 3. Compile and flush the batch
                await self._dispatch_batch(accumulated)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batcher execution loop: {e}", exc_info=True, component="batcher")

    async def _dispatch_batch(self, requests: List[ScheduledRequest]) -> None:
        """Pads tensors, records metrics, and dispatches to handler."""
        if not requests:
            return
        
        padded_tensors, padded_shape = pad_tensors(requests)
        max_tokens = max(req.max_latency_ms for req in requests)
        
        batch = Batch(
            batch_id=str(uuid.uuid4()),
            requests=requests,
            padded_tensors=padded_tensors,
            padded_shape=padded_shape,
            max_tokens=int(max_tokens)
        )

        # Calculate metrics
        actual_tokens = sum(len(req.payload) if isinstance(req.payload, list) else 1 for req in requests)
        padded_tokens = padded_shape[0] * padded_shape[1]
        self.metrics.record_batch(len(requests), actual_tokens, padded_tokens)

        # Dispatch
        await self.handler.handle_batch(batch)

    async def _flush_remaining(self) -> None:
        """Flushes remaining elements immediately."""
        accumulated = []
        while self.scheduler.size() > 0:
            try:
                # Dequeue non-blocking wrapper
                req = await self.scheduler.dequeue()
                accumulated.append(req)
            except Exception:
                break
        
        if accumulated:
            logger.info(f"Flushing {len(accumulated)} remaining requests during teardown.", component="batcher")
            await self._dispatch_batch(accumulated)


class ContinuousBatcher:
    """
    Simulates iteration-level continuous batching (vLLM style).
    
    Evaluates token completion boundaries and manages slot injection dynamically.
    """
    def __init__(self, scheduler: IScheduler, max_batch_size: int) -> None:
        self.scheduler = scheduler
        self.max_batch_size = max_batch_size
        self.active_requests: List[ScheduledRequest] = []
        # Maps request_id to generated tokens count
        self._generated_counts: Dict[str, int] = {}

    async def step(self) -> List[Tuple[ScheduledRequest, int]]:
        """
        Executes one token generation step.
        
        Returns:
            A list of tuples: (CompletedRequest, total_tokens_generated).
        """
        completed = []

        # 1. Fill empty slots by pulling from the scheduler queue first
        while len(self.active_requests) < self.max_batch_size and self.scheduler.size() > 0:
            try:
                new_req = await self.scheduler.dequeue()
                self.active_requests.append(new_req)
                self._generated_counts[new_req.request_id] = 0
            except Exception:
                break

        # 2. Check active requests and increment token generation counters
        remaining_requests = []
        for req in self.active_requests:
            req_id = req.request_id
            self._generated_counts[req_id] = self._generated_counts.get(req_id, 0) + 1
            
            # Determine generation target limit
            limit = 10
            if isinstance(req.payload, dict):
                limit = req.payload.get("max_tokens", 10)
            elif isinstance(req.payload, list):
                # If list represent token IDs, use length or static limit
                limit = max(5, len(req.payload))

            if self._generated_counts[req_id] >= limit:
                completed.append((req, self._generated_counts[req_id]))
                self._generated_counts.pop(req_id, None)
            else:
                remaining_requests.append(req)

        self.active_requests = remaining_requests
        return completed


class AdaptiveBatcher(StaticBatcher):
    """
    StaticBatcher subclass that adjusts max_batch_size based on scheduler queue depth.
    
    Prioritizes low-latency (small batch sizes) under low load,
    and scales up to prioritize throughput (large batch sizes) under heavy load.
    """
    def __init__(
        self,
        scheduler: IScheduler,
        handler: IBatchHandler,
        min_batch_size: int = 4,
        max_batch_size: int = 32,
        congestion_threshold: int = 20,
        max_queue_delay_ms: int = 10,
        metrics: Optional[BatcherMetrics] = None
    ) -> None:
        super().__init__(scheduler, handler, max_batch_size, max_queue_delay_ms, metrics)
        self.min_batch_size = min_batch_size
        self.peak_batch_size = max_batch_size
        self.congestion_threshold = congestion_threshold

    async def _batch_loop(self) -> None:
        """Overridden loop adapting batch sizes dynamically before aggregation runs."""
        while self._is_active:
            try:
                # Dynamic adaptation check
                qsize = self.scheduler.size()
                if qsize >= self.congestion_threshold:
                    self.max_batch_size = self.peak_batch_size
                else:
                    self.max_batch_size = self.min_batch_size

                first_req = await self.scheduler.dequeue()
                accumulated = [first_req]
                start_time = asyncio.get_event_loop().time()
                
                while len(accumulated) < self.max_batch_size:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    remaining_time = self.max_queue_delay_sec - elapsed
                    
                    if remaining_time <= 0:
                        break
                    
                    if self.scheduler.size() > 0:
                        next_req = await self.scheduler.dequeue()
                        accumulated.append(next_req)
                    else:
                        await asyncio.sleep(0.001)

                await self._dispatch_batch(accumulated)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in adaptive batcher: {e}", exc_info=True, component="batcher")
