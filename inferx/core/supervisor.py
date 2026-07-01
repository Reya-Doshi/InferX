# inferx/core/supervisor.py
"""
InferX Worker Supervisor.

Orchestrates the lifecycle of GPU worker subprocesses using async-safe wrappers
and non-blocking process joins.
"""

import asyncio
import multiprocessing
from multiprocessing.context import BaseContext
import time
from typing import Any, Optional

from inferx.interfaces.core import IRuntimeSupervisor
from inferx.utils.logging import get_logger, telemetry_context

logger = get_logger("supervisor")


def worker_hot_loop(
    worker_id: str, gpu_id: int, heartbeat_counter: Any, stop_event: Any
) -> None:
    """Target hot-loop executed inside spawned worker processes."""
    import sys

    try:
        while not stop_event.is_set():
            with heartbeat_counter.get_lock():
                heartbeat_counter.value += 1
            time.sleep(0.1)
    except Exception:
        sys.exit(1)


class WorkerHandle:
    """Wraps multiprocessing handle state for a single GPU worker."""

    def __init__(
        self,
        worker_id: str,
        gpu_id: int,
        process: multiprocessing.Process,
        heartbeat_counter: Any,
        stop_event: Any,
    ) -> None:
        self.worker_id = worker_id
        self.gpu_id = gpu_id
        self.process = process
        self.heartbeat_counter = heartbeat_counter
        self.stop_event = stop_event
        self.last_heartbeat_value = 0
        self.last_heartbeat_time = time.time()


class RuntimeSupervisor(IRuntimeSupervisor):
    """
    Coordinates worker subprocesses using non-blocking primitives.

    Verifies heartbeats and executes process recovery routines off the async loop.
    """

    def __init__(self, gpus: list[int], heartbeat_timeout_ms: int) -> None:
        self.gpus = gpus
        self.heartbeat_timeout = heartbeat_timeout_ms / 1000.0
        self._workers: dict[str, WorkerHandle] = {}
        self._monitor_task: Optional[asyncio.Task[None]] = None
        self._is_active = False
        self._lock = asyncio.Lock()
        self._mp_ctx: BaseContext = multiprocessing.get_context("spawn")

    async def start(self) -> None:
        """Launches all worker subprocesses and starts the watchdog monitor."""
        async with self._lock:
            if self._is_active:
                return

            self._is_active = True
            logger.info(
                f"Starting runtime supervisor for GPUs: {self.gpus}",
                component="supervisor",
            )

            for idx, gpu_id in enumerate(self.gpus):
                worker_id = f"worker-gpu-{gpu_id}-{idx}"
                # Offload process launching to prevent loop stalling during spawn cycles
                await asyncio.to_thread(self._spawn_worker, worker_id, gpu_id)

            self._monitor_task = asyncio.create_task(self._watchdog_loop())

    async def stop(self) -> None:
        """Gracefully shuts down all worker processes and stops the monitor loop."""
        async with self._lock:
            self._is_active = False

            if self._monitor_task:
                self._monitor_task.cancel()
                try:
                    await self._monitor_task
                except asyncio.CancelledError:
                    pass
                self._monitor_task = None

            logger.info("Stopping all worker subprocesses...", component="supervisor")

            for handle in self._workers.values():
                handle.stop_event.set()

            for worker_id, handle in list(self._workers.items()):
                proc = handle.process
                if proc.is_alive():
                    # Offload process join to thread pool to prevent blocking event loop
                    await asyncio.to_thread(proc.join, timeout=2.0)
                    if proc.is_alive():
                        logger.warning(
                            f"Worker {worker_id} did not exit cleanly. Force killing.",
                            component="supervisor",
                        )
                        proc.terminate()
                        await asyncio.to_thread(proc.join)

            self._workers.clear()
            logger.info("All worker processes terminated.", component="supervisor")

    async def recover_worker(self, worker_id: str) -> None:
        """Kills, recycles, and restarts an unhealthy worker process."""
        async with self._lock:
            handle = self._workers.get(worker_id)
            if not handle:
                return

            logger.error(
                f"Worker {worker_id} failure detected. Triggering recovery...",
                component="supervisor",
            )

            # Setup logging context metadata for tracing this recovery
            ctx = telemetry_context.get().copy()
            ctx["worker_id"] = worker_id
            token = telemetry_context.set(ctx)

            try:
                proc = handle.process
                if proc.is_alive():
                    proc.terminate()
                    await asyncio.to_thread(proc.join, timeout=1.0)
                    if proc.is_alive():
                        proc.kill()
                        await asyncio.to_thread(proc.join)

                gpu_id = handle.gpu_id
                await asyncio.to_thread(self._spawn_worker, worker_id, gpu_id)
                logger.info(
                    f"Worker {worker_id} recovery complete. Restarted successfully.",
                    component="supervisor",
                )
            finally:
                telemetry_context.reset(token)

    def _spawn_worker(self, worker_id: str, gpu_id: int) -> None:
        """Helper to create and launch a multiprocessing process wrapper."""
        heartbeat_counter = self._mp_ctx.Value("i", 0)
        stop_event = self._mp_ctx.Event()

        process = self._mp_ctx.Process(
            target=worker_hot_loop,
            args=(worker_id, gpu_id, heartbeat_counter, stop_event),
            name=f"inferx-{worker_id}",
        )
        process.daemon = True
        process.start()

        self._workers[worker_id] = WorkerHandle(
            worker_id=worker_id,
            gpu_id=gpu_id,
            process=process,
            heartbeat_counter=heartbeat_counter,
            stop_event=stop_event,
        )
        logger.info(
            f"Worker {worker_id} spawned (PID: {process.pid})", component="supervisor"
        )

    async def _watchdog_loop(self) -> None:
        """Watchdog loop running in the background, checking heartbeats."""
        while self._is_active:
            try:
                await asyncio.sleep(0.5)
                workers_snapshot = list(self._workers.values())

                for handle in workers_snapshot:
                    worker_id = handle.worker_id
                    proc = handle.process

                    if not proc.is_alive():
                        logger.error(
                            f"Worker {worker_id} process died unexpectedly (Exit Code: {proc.exitcode}).",
                            component="supervisor",
                        )
                        await self.recover_worker(worker_id)
                        continue

                    current_val = handle.heartbeat_counter.value
                    now = time.time()

                    if current_val != handle.last_heartbeat_value:
                        handle.last_heartbeat_value = current_val
                        handle.last_heartbeat_time = now
                    else:
                        elapsed = now - handle.last_heartbeat_time
                        if elapsed > self.heartbeat_timeout:
                            logger.error(
                                f"Worker {worker_id} heartbeat stall detected. "
                                f"No update for {elapsed:.2f} seconds (limit: {self.heartbeat_timeout}s).",
                                component="supervisor",
                            )
                            await self.recover_worker(worker_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error in watchdog monitor loop: {e}",
                    exc_info=True,
                    component="supervisor",
                )
