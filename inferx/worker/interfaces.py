# inferx/worker/interfaces.py
"""
InferX Worker Runtime Interfaces.

Defines status enums, information structures, IPC message models,
and core worker interfaces.
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class WorkerStatus(Enum):
    """Execution status states for worker processes."""
    STARTING = "STARTING"
    READY = "READY"
    BUSY = "BUSY"
    CRASHED = "CRASHED"
    STOPPED = "STOPPED"


class WorkerInfo(BaseModel):
    """Data model containing worker status and host assignment details."""
    worker_id: str
    gpu_id: int
    cpu_cores: List[int]
    status: WorkerStatus
    last_heartbeat: float

    model_config = {"arbitrary_types_allowed": True}


class IPCMessage(BaseModel):
    """
    Data model representing a metadata packet sent across the IPC queue.
    
    Contains offsets pointing to the payload data stored in the SharedMemory pool.
    """
    request_id: str
    offset: int
    size: int
    timestamp_ns: int

    model_config = {"frozen": True}


class IWorker(ABC):
    """Interface representing a single execution worker instance (run inside subprocesses)."""

    @abstractmethod
    def start(self) -> None:
        """Starts the worker hot-loop listening on the IPC channels."""
        pass

    @abstractmethod
    def stop(self) -> None:
        """Gracefully shuts down the worker process."""
        pass


class IWorkerManager(ABC):
    """Interface representing the process supervisor coordinating worker lifetimes."""

    @abstractmethod
    async def start(self) -> None:
        """Spawns the configured workers and starts the heartbeat watchdog loops."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Shuts down all worker processes and frees shared memory allocations."""
        pass
