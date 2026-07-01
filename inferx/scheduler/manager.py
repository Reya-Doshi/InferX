# inferx/scheduler/manager.py
"""
InferX Scheduler Manager.

Implements the thread-safe and async-safe Scheduler orchestrator.
Handles request enqueuing, priority popping, and background request aging worker tasks.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from inferx.scheduler.interfaces import IScheduler, ISchedulingPolicy, ScheduledRequest
from inferx.scheduler.metrics import SchedulerMetrics
from inferx.utils.logging import get_logger

logger = get_logger("scheduler")


class Scheduler(IScheduler):
    """
    State manager coordinating task enqueue and dequeue events.

    Interfaces with dynamic scheduling policies and executes a background
    aging worker loop if the policy requires it.
    """

    def __init__(
        self,
        policy: ISchedulingPolicy,
        metrics: Optional[SchedulerMetrics] = None,
        aging_interval_ms: int = 100,
    ) -> None:
        self.policy = policy
        self.metrics = metrics or SchedulerMetrics()
        self.aging_interval_sec = aging_interval_ms / 1000.0

        self._cond = asyncio.Condition()
        self._aging_task: Optional[asyncio.Task[None]] = None
        self._is_active = False

    async def start(self) -> None:
        """Starts background aging worker tasks if supported by the scheduling policy."""
        async with self._cond:
            if self._is_active:
                return
            self._is_active = True

            # Check if the policy supports dynamic request aging
            if hasattr(self.policy, "age_requests"):
                self._aging_task = asyncio.create_task(self._aging_worker_loop())
                logger.info(
                    f"Starvation prevention active. Spawning aging worker (interval: {self.aging_interval_sec}s).",
                    component="scheduler",
                )

    async def stop(self) -> None:
        """Cancels background tasks and flushes active queues."""
        async with self._cond:
            self._is_active = False
            if self._aging_task:
                self._aging_task.cancel()
                try:
                    await self._aging_task
                except asyncio.CancelledError:
                    pass
                self._aging_task = None

            # Wake up any blocked consumers waiting on dequeue
            self._cond.notify_all()
            logger.info("Scheduler stopped.", component="scheduler")

    async def enqueue(self, request: ScheduledRequest) -> None:
        """Enqueues a task and notifies waiting consumers."""
        async with self._cond:
            self.policy.push(request)
            self.metrics.record_enqueue()

            # Notify a single waiting dequeue consumer task
            self._cond.notify(1)

    async def dequeue(self) -> ScheduledRequest:
        """Pops the next available task, blocking asynchronously if the queue is empty."""
        async with self._cond:
            while self.policy.size() == 0:
                if not self._is_active:
                    raise asyncio.CancelledError(
                        "Scheduler stopped while waiting for requests."
                    )
                await self._cond.wait()

            request = self.policy.pop()
            if request is None:
                raise asyncio.CancelledError(
                    "Queue state changed or pop operation yielded None."
                )

            # Log queue wait-time metrics
            now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
            wait_ns = now_ns - request.enqueue_timestamp_ns
            self.metrics.record_dequeue(wait_ns)

            return request

    def size(self) -> int:
        """Returns the total number of enqueued requests across all queues."""
        return self.policy.size()

    async def _aging_worker_loop(self) -> None:
        """Background loop executing aged priority calculations periodically."""
        while self._is_active:
            try:
                await asyncio.sleep(self.aging_interval_sec)
                async with self._cond:
                    # Execute policy-specific aging recalculation
                    if hasattr(self.policy, "age_requests"):
                        self.policy.age_requests()
                        self.metrics.record_starvation_warning()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error in scheduler aging loop: {e}",
                    exc_info=True,
                    component="scheduler",
                )
