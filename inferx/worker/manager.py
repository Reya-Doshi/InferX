# inferx/worker/manager.py
"""
InferX Worker Manager & Subprocess Supervisor.

Implements the multi-process execution hot-loop, CPU affinity bindings,
heartbeat watchdogs, and zero-copy shared memory exchanges.
"""
import asyncio
import json
import multiprocessing
import os
import time
from typing import Dict, List, Optional
try:
    import psutil
except ImportError:
    psutil = None

from inferx.batcher.interfaces import Batch
from inferx.interfaces.core import IRuntimeLifecycle
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.worker.interfaces import IWorkerManager, WorkerInfo, WorkerStatus, IPCMessage
from inferx.worker.ipc import SharedMemoryPool, SharedMemoryAllocator
from inferx.worker.executor import BatchExecutor
from inferx.worker.metrics import WorkerMetrics
from inferx.utils.logging import get_logger

logger = get_logger("worker.manager")


def worker_process_hot_loop(
    worker_id: str,
    gpu_id: int,
    cpu_cores: List[int],
    request_queue: multiprocessing.Queue,
    response_queue: multiprocessing.Queue,
    heartbeat_val: multiprocessing.Value,
    stop_evt: multiprocessing.Event,
    shm_name: str,
    shm_size: int
) -> None:
    """
    Subprocess entry point (must be module-level for spawn serialization).
    
    Binds CPU affinity, maps the shared memory pool, and processes incoming requests.
    """
    # 1. Set CPU Affinity if psutil is available
    if psutil is not None:
        try:
            proc = psutil.Process()
            proc.cpu_affinity(cpu_cores)
        except Exception as e:
            # Fallback if affinity setting fails (e.g. permission limits or platform bugs)
            pass

    # 2. Attach to the Shared Memory Pool
    shm = SharedMemoryPool(name=shm_name, size=shm_size, create=False)
    executor = BatchExecutor(num_streams=4)

    # 3. Create a local asyncio loop inside the subprocess
    async def loop_fn():
        # Setup initial heartbeat
        heartbeat_val.value = time.time()

        while not stop_evt.is_set():
            # Update heartbeat atomic float
            heartbeat_val.value = time.time()
            
            # Non-blocking poll request_queue
            try:
                # Use a small timeout to react to stop events
                msg_bytes = request_queue.get(timeout=0.1)
                
                # Parse IPCMessage metadata
                msg_dict = json.loads(msg_bytes.decode("utf-8"))
                ipc_msg = IPCMessage(**msg_dict)
                
                # Update heartbeat during start of execution
                heartbeat_val.value = time.time()

                # Read raw serialized request payload from shared memory offset
                data_bytes = shm.read(ipc_msg.offset, ipc_msg.size)
                req_dict = json.loads(data_bytes.decode("utf-8"))
                
                # Construct ScheduledRequest object
                request = ScheduledRequest(**req_dict)
                
                # Construct a single-request Batch representing execution segment
                batch = Batch(
                    batch_id=f"b-{request.request_id}",
                    requests=[request],
                    padded_tensors=[[0]],  # Mock padding
                    padded_shape=[1, 1],
                    max_tokens=20
                )

                # Execute task on virtual CUDA streams
                results = await executor.execute_batch(batch)
                output = results.get(request.request_id, b"error")

                # Post execution output and offset back via response queue
                response_queue.put((request.request_id, ipc_msg.offset, output))
                
                # Update heartbeat after execution completes
                heartbeat_val.value = time.time()

            except Exception:
                # Queue empty timeout or execution failure
                pass

        shm.close()

    # Run loop
    asyncio.run(loop_fn())


class WorkerManager(IWorkerManager):
    """
    Coordinates multi-process worker pools, binds resources,
    allocates shared memory blocks, and runs recovery watchdogs.
    """
    def __init__(
        self,
        num_workers: int = 2,
        shm_pool_size: int = 10 * 1024 * 1024,  # 10MB Shared Memory Pool
        shm_slot_size: int = 64 * 1024,        # 64KB per slot
        heartbeat_timeout_sec: float = 3.0,
        metrics: Optional[WorkerMetrics] = None
    ) -> None:
        self.num_workers = num_workers
        self.shm_pool_size = shm_pool_size
        self.shm_slot_size = shm_slot_size
        self.heartbeat_timeout = heartbeat_timeout_sec
        self.metrics = metrics or WorkerMetrics()

        self._shm_name = f"inferx_shm_{uuid_str()}"
        self._shm: Optional[SharedMemoryPool] = None
        self._allocator = SharedMemoryAllocator(pool_size=shm_pool_size, slot_size=shm_slot_size)

        # Inter-process queues
        self.request_queue = multiprocessing.Queue()
        self.response_queue = multiprocessing.Queue()
        self.stop_event = multiprocessing.Event()

        # Worker state tracking
        self._processes: Dict[str, multiprocessing.Process] = {}
        self._heartbeats: Dict[str, multiprocessing.Value] = {}
        self._worker_infos: Dict[str, WorkerInfo] = {}

        self._pending_futures: Dict[str, asyncio.Future[bytes]] = {}
        self._watchdog_task: Optional[asyncio.Task[None]] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._is_active = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Initializes shared memory pool, spawns workers, and starts watchdog loops."""
        async with self._lock:
            if self._is_active:
                return
            self._is_active = True

            # 1. Allocate Shared Memory Pool
            self._shm = SharedMemoryPool(name=self._shm_name, size=self.shm_pool_size, create=True)
            logger.info(f"Initialized shared memory pool: {self._shm_name} ({self.shm_pool_size} bytes)", component="worker_manager")

            # 2. Spawn Worker Processes
            for i in range(self.num_workers):
                worker_id = f"worker-{i}"
                await self._spawn_worker(worker_id, gpu_id=i)

            # 3. Spawn Watchdog and Response Reader loops
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())
            self._reader_task = asyncio.create_task(self._response_reader_loop())
            logger.info("Worker pool supervisor active.", component="worker_manager")

    async def stop(self) -> None:
        """Terminates all worker subprocesses and releases shared memory blocks."""
        async with self._lock:
            self._is_active = False
            self.stop_event.set()

            # Cancel background tasks
            if self._watchdog_task:
                self._watchdog_task.cancel()
                try:
                    await self._watchdog_task
                except asyncio.CancelledError:
                    pass
                self._watchdog_task = None

            if self._reader_task:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
                self._reader_task = None

            # Terminate subprocesses
            for worker_id, proc in list(self._processes.items()):
                if proc.is_alive():
                    proc.terminate()
                    # Await process join in a thread to prevent blocking the event loop
                    await asyncio.to_thread(proc.join, timeout=1.0)
                    if proc.is_alive():
                        proc.kill()
            
            self._processes.clear()
            self._heartbeats.clear()
            self._worker_infos.clear()

            # Release Shared Memory Pool
            if self._shm:
                self._shm.close()
                self._shm.unlink()
                self._shm = None

            logger.info("Worker pool shutdown complete.", component="worker_manager")

    async def enqueue_request(self, request: ScheduledRequest) -> asyncio.Future[bytes]:
        """
        Writes the request data to Shared Memory and registers a pending Future.
        
        Returns:
            An asyncio.Future resolving to the worker output bytes.
        """
        # 1. Allocate Shared Memory slot
        # Run allocation in lock to prevent conflicts
        offset = self._allocator.allocate()
        
        # 2. Serialize request and write to Shared Memory offset
        req_bytes = json.dumps(request.model_dump()).encode("utf-8")
        self._shm.write(offset, req_bytes)

        # 3. Create IPCMessage
        ipc_msg = IPCMessage(
            request_id=request.request_id,
            offset=offset,
            size=len(req_bytes),
            timestamp_ns=request.enqueue_timestamp_ns
        )

        # 4. Enqueue metadata message to request queue
        ipc_bytes = json.dumps(ipc_msg.model_dump()).encode("utf-8")
        self.request_queue.put(ipc_bytes)

        # 5. Register Future
        fut: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
        self._pending_futures[request.request_id] = fut
        return fut

    def get_worker_status(self, worker_id: str) -> Optional[WorkerStatus]:
        """Returns the status enum for a specific worker process."""
        info = self._worker_infos.get(worker_id)
        return info.status if info else None

    async def _spawn_worker(self, worker_id: str, gpu_id: int) -> None:
        """Spawns a single worker process."""
        # Clean up old references
        old_proc = self._processes.pop(worker_id, None)
        if old_proc and old_proc.is_alive():
            old_proc.terminate()
            await asyncio.to_thread(old_proc.join, timeout=0.5)

        # Assign CPU Affinity cores (e.g. 2 cores per worker)
        total_cores = os.cpu_count() or 4
        cpu_cores = [(worker_id_num(worker_id) * 2) % total_cores, (worker_id_num(worker_id) * 2 + 1) % total_cores]

        # Inter-process shared heartbeat Value
        heartbeat_val = multiprocessing.Value("d", time.time())
        self._heartbeats[worker_id] = heartbeat_val

        # Setup spawned Process (spawn method required on Windows)
        ctx = multiprocessing.get_context("spawn")
        proc = ctx.Process(
            target=worker_process_hot_loop,
            args=(
                worker_id,
                gpu_id,
                cpu_cores,
                self.request_queue,
                self.response_queue,
                heartbeat_val,
                self.stop_event,
                self._shm_name,
                self.shm_pool_size
            )
        )
        
        self._processes[worker_id] = proc
        self._worker_infos[worker_id] = WorkerInfo(
            worker_id=worker_id,
            gpu_id=gpu_id,
            cpu_cores=cpu_cores,
            status=WorkerStatus.STARTING,
            last_heartbeat=time.time()
        )

        # Start process in thread to avoid blocking event loop
        await asyncio.to_thread(proc.start)
        self._worker_infos[worker_id].status = WorkerStatus.READY
        logger.info(f"Worker process {worker_id} spawned (GPU: {gpu_id}, Cores: {cpu_cores})", component="worker_manager")

    async def _watchdog_loop(self) -> None:
        """Background monitoring loop verifying heartbeats and process liveness."""
        while self._is_active:
            try:
                await asyncio.sleep(1.0)
                now = time.time()
                
                for worker_id, proc in list(self._processes.items()):
                    heartbeat_val = self._heartbeats[worker_id]
                    info = self._worker_infos[worker_id]
                    
                    delay = now - heartbeat_val.value
                    self.metrics.record_heartbeat_delay(worker_id, delay)

                    # 1. Process Liveness Check
                    if not proc.is_alive():
                        logger.error(f"Worker {worker_id} detected dead. Triggering recovery restart.", component="worker_manager")
                        info.status = WorkerStatus.CRASHED
                        self.metrics.record_restart()
                        await self._spawn_worker(worker_id, info.gpu_id)
                        
                    # 2. Heartbeat Timeout Check
                    elif delay > self.heartbeat_timeout:
                        logger.error(
                            f"Worker {worker_id} heartbeat timeout (delay: {delay:.2f}s). Terminating and restarting.",
                            component="worker_manager"
                        )
                        info.status = WorkerStatus.CRASHED
                        self.metrics.record_restart()
                        
                        # Kill the stalled worker process
                        proc.terminate()
                        await asyncio.to_thread(proc.join, timeout=1.0)
                        if proc.is_alive():
                            proc.kill()
                            
                        await self._spawn_worker(worker_id, info.gpu_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in watchdog monitoring loop: {e}", exc_info=True, component="worker_manager")

    async def _response_reader_loop(self) -> None:
        """Background loop reading from the response queue and resolving futures."""
        while self._is_active:
            try:
                # Offload blocking queue get to thread
                response = await asyncio.to_thread(self.response_queue.get, timeout=0.1)
                
                req_id, offset, output = response
                
                # 1. Resolve Future
                fut = self._pending_futures.pop(req_id, None)
                if fut and not fut.done():
                    fut.set_result(output)

                # 2. Free Shared Memory slot offset
                self._allocator.free(offset)

            except asyncio.CancelledError:
                break
            except Exception:
                # Queue empty timeout
                await asyncio.sleep(0.001)


# Helper functions for string parser operations

def worker_id_num(worker_id: str) -> int:
    """Extracts integer index from worker-id string."""
    try:
        return int(worker_id.split("-")[-1])
    except Exception:
        return 0


def uuid_str() -> str:
    """Generates a fast random string identifier."""
    import uuid
    return str(uuid.uuid4())[:8]
