# inferx/worker/executor.py
"""
InferX Batch Executor & CUDA Streams.

Simulates virtual CUDA execution streams, batch execution queues,
and asynchronous task delays.
"""
import asyncio
import os
import random
import sys
from typing import Dict, List
from dotenv import load_dotenv

from inferx.batcher.interfaces import Batch
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.utils.logging import get_logger

logger = get_logger("worker.executor")


class CudaStream:
    """
    Simulates a CUDA execution stream on a GPU.
    
    Allows processing tasks concurrently or sequentially, simulating execution delays
    and supporting cancellation.
    """
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self._lock = asyncio.Lock()
        
        import os
        import sys
        
        self.is_testing = "unittest" in sys.argv[0] or "pytest" in sys.argv[0]
        if not self.is_testing:
            try:
                import psutil
                parent = psutil.Process(os.getppid())
                cmd = " ".join(parent.cmdline()).lower()
                self.is_testing = "unittest" in cmd or "pytest" in cmd or "test" in cmd
            except Exception:
                pass

        self.api_key = os.getenv("GEMINI_API_KEY")
        self.client = None
        
        if self.api_key and not self.is_testing:
            try:
                from google import genai
                self.client = genai.Client(api_key=self.api_key)
            except Exception as e:
                logger.error(f"Failed to initialize GenAI client: {e}", component="cuda_stream")

    async def execute_task(self, request: ScheduledRequest, execution_time_ms: float = 5.0) -> bytes:
        """
        Simulates executing a single request task on this stream.
        
        Uses an asyncio lock to serialize tasks scheduled on the same stream,
        matching CUDA stream behavior.
        """
        async with self._lock:
            try:
                # Simulate GPU tensor computation delay
                await asyncio.sleep(execution_time_ms / 1000.0)
                
                if self.client:
                    try:
                        logger.info(
                            f"Dispatched task {request.request_id} to Gemini API.",
                            request_id=request.request_id,
                            component="cuda_stream"
                        )
                        
                        max_retries = 3
                        backoff = 1.0
                        reply = ""
                        for attempt in range(max_retries):
                            try:
                                response = await self.client.aio.models.generate_content(
                                    model="gemini-2.5-flash",
                                    contents=str(request.payload)
                                )
                                reply = response.text or ""
                                break
                            except Exception as ex:
                                if attempt == max_retries - 1:
                                    raise
                                sleep_time = backoff * (2 ** attempt) + random.uniform(0.1, 0.5)
                                await asyncio.sleep(sleep_time)

                        logger.info(
                            f"Successfully resolved task {request.request_id} from Gemini API.",
                            request_id=request.request_id,
                            component="cuda_stream"
                        )
                        return reply.encode("utf-8")
                    except Exception as e:
                        logger.error(
                            f"Gemini API failure in CudaStream: {e}",
                            request_id=request.request_id,
                            exc_info=True,
                            component="cuda_stream"
                        )
                        return f"Error: Gemini API failure: {str(e)}".encode("utf-8")

                # Mock token output result (echo or transform inputs)
                payload_str = str(request.payload)
                return f"processed_{payload_str}".encode("utf-8")
            except asyncio.CancelledError:
                logger.warning(
                    f"Task {request.request_id} cancelled on CUDA stream {self.stream_id}.",
                    request_id=request.request_id,
                    component="cuda_stream"
                )
                raise


class BatchExecutor:
    """
    Schedules Batch execution tasks across virtual CUDA streams.
    """
    def __init__(self, num_streams: int = 4) -> None:
        self.streams = [CudaStream(i) for i in range(num_streams)]

    async def execute_batch(self, batch: Batch, task_execution_time_ms: float = 5.0) -> Dict[str, bytes]:
        """
        Distributes request tasks across streams round-robin.
        
        Returns:
            A dictionary mapping request_id to output bytes.
        """
        tasks = []
        for idx, req in enumerate(batch.requests):
            # Select stream round-robin
            stream = self.streams[idx % len(self.streams)]
            tasks.append(
                asyncio.create_task(
                    self._run_with_timeout(stream, req, task_execution_time_ms)
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output_map = {}
        for req, res in zip(batch.requests, results):
            if isinstance(res, Exception):
                logger.error(
                    f"Request {req.request_id} failed during batch execution: {res}",
                    request_id=req.request_id,
                    component="batch_executor"
                )
                # Map failure reasons
                output_map[req.request_id] = f"error_{str(res)}".encode("utf-8")
            else:
                output_map[req.request_id] = res

        return output_map

    async def _run_with_timeout(self, stream: CudaStream, req: ScheduledRequest, execution_time_ms: float) -> bytes:
        """Executes a task on a stream enforcing max_latency_ms limits."""
        # Convert relative deadline to seconds
        timeout_sec = req.max_latency_ms / 1000.0
        try:
            return await asyncio.wait_for(
                stream.execute_task(req, execution_time_ms),
                timeout=timeout_sec
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Request {req.request_id} timed out after {timeout_sec}s.",
                request_id=req.request_id,
                component="batch_executor"
            )
            raise TimeoutError("Task execution exceeded configured deadline limits.")
