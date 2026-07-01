# tests/test_worker.py
"""
InferX Worker Runtime Test Suite.

Verifies SharedMemory zero-copy transfer, process crash recovery watchdogs,
heartbeat timeouts, and concurrent CUDA streams executions.
"""

import asyncio
import unittest
import time

from inferx.scheduler.interfaces import ScheduledRequest
from inferx.worker.interfaces import WorkerStatus
from inferx.worker.ipc import SharedMemoryPool, SharedMemoryAllocator
from inferx.worker.executor import CudaStream, BatchExecutor
from inferx.worker.manager import WorkerManager


class TestWorkerRuntime(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the Worker Runtime components."""

    async def asyncSetUp(self) -> None:
        # Fast heartbeat timeouts for tests
        self.manager = WorkerManager(num_workers=1, heartbeat_timeout_sec=0.5)

    async def asyncTearDown(self) -> None:
        await self.manager.stop()

    def build_request(
        self, request_id: str, payload_data: str, latency_ms: float = 30000.0
    ) -> ScheduledRequest:
        return ScheduledRequest(
            request_id=request_id,
            tenant_id="t1",
            priority=1,
            payload=payload_data,
            max_latency_ms=latency_ms,
        )

    def test_shared_memory_allocator_and_zero_copy(self) -> None:
        shm_name = "inferx_test_shm_zero_copy"
        shm_size = 1024

        # Create pool
        pool = SharedMemoryPool(name=shm_name, size=shm_size, create=True)
        allocator = SharedMemoryAllocator(pool_size=shm_size, slot_size=256)

        try:
            self.assertEqual(allocator.free_slots_count(), 4)  # 1024 // 256 = 4 slots

            offset1 = allocator.allocate()
            self.assertEqual(offset1, 0)
            self.assertEqual(allocator.free_slots_count(), 3)

            # Write bytes to offset
            test_data = b"hello_shared_memory"
            pool.write(offset1, test_data)

            # Read back from offset
            read_bytes = pool.read(offset1, len(test_data))
            self.assertEqual(read_bytes, test_data)

            # Free slot
            allocator.free(offset1)
            self.assertEqual(allocator.free_slots_count(), 4)

        finally:
            pool.close()
            pool.unlink()

    async def test_cuda_stream_concurrency_and_deadlines(self) -> None:
        stream = CudaStream(stream_id=1)
        req = self.build_request("r1", "payload", latency_ms=10.0)

        # 1. Successful execution
        res = await stream.execute_task(req, execution_time_ms=2.0)
        self.assertEqual(res, b"processed_payload")

        # 2. Timeout enforcement test
        executor = BatchExecutor(num_streams=1)
        batch = self.build_request("r2", "timeout_payload", latency_ms=5.0)

        # Schedule execution exceeding deadline
        from inferx.batcher.interfaces import Batch

        b = Batch(
            batch_id="b",
            requests=[batch],
            padded_tensors=[[0]],
            padded_shape=[1, 1],
            max_tokens=10,
        )

        results = await executor.execute_batch(b, task_execution_time_ms=20.0)
        output = results.get("r2")
        self.assertIn(b"error_Task execution exceeded", output)

    async def test_worker_process_spawn_and_execution(self) -> None:
        # Start manager
        await self.manager.start()

        # Wait for worker process to initialize and become ready
        await asyncio.sleep(0.5)

        worker_id = "worker-0"
        self.assertEqual(self.manager.get_worker_status(worker_id), WorkerStatus.READY)

        # Enqueue a request
        req = self.build_request("req-shm-1", "shm_payload")
        fut = await self.manager.enqueue_request(req)

        # Await execution output (resolves via response reader thread)
        result = await fut
        self.assertEqual(result, b"processed_shm_payload")

    async def test_worker_process_crash_recovery(self) -> None:
        await self.manager.start()
        await asyncio.sleep(0.5)

        worker_id = "worker-0"
        proc = self.manager._processes[worker_id]

        # Simulates worker crash by terminating process
        proc.terminate()
        await asyncio.to_thread(proc.join, timeout=1.0)

        # Give watchdog monitor loop time to run and spawn recovery worker
        await asyncio.sleep(1.5)

        # Status should recover to READY
        self.assertEqual(self.manager.get_worker_status(worker_id), WorkerStatus.READY)

        # The new process should be alive
        new_proc = self.manager._processes[worker_id]
        self.assertTrue(new_proc.is_alive())

    async def test_worker_heartbeat_timeout_recovery(self) -> None:
        await self.manager.start()
        await asyncio.sleep(0.5)

        worker_id = "worker-0"
        heartbeat_val = self.manager._heartbeats[worker_id]

        # Simulate heartbeat freeze: set heartbeat timestamp in past (10 seconds ago)
        heartbeat_val.value = time.time() - 10.0

        # Give watchdog monitor time to detect timeout and trigger recovery
        await asyncio.sleep(1.5)

        # Status should recover to READY
        self.assertEqual(self.manager.get_worker_status(worker_id), WorkerStatus.READY)


if __name__ == "__main__":
    unittest.main()
