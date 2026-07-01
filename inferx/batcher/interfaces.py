# inferx/batcher/interfaces.py
"""
InferX Batcher Interfaces.

Defines the Batch data model, the worker callback interface (IBatchHandler),
and the core IBatcher life-cycle interface.
"""

from abc import ABC, abstractmethod
from typing import List
from pydantic import BaseModel

from inferx.scheduler.interfaces import ScheduledRequest


class Batch(BaseModel):
    """
    Data model representing a merged batch of scheduled inference requests.

    Contains input tensor matrices and padding dimensions.
    """

    batch_id: str
    requests: List[ScheduledRequest]
    padded_tensors: List[List[int]]
    padded_shape: List[int]
    max_tokens: int

    model_config = {"arbitrary_types_allowed": True}


class IBatchHandler(ABC):
    """Callback interface representing the execution target (e.g. Worker Pool)."""

    @abstractmethod
    async def handle_batch(self, batch: Batch) -> None:
        """Invoked when a batch is successfully formed and ready for execution."""
        pass


class IBatcher(ABC):
    """Lifecycle interface for dynamic batching engines."""

    @abstractmethod
    async def start(self) -> None:
        """Starts background loops, polling queues and monitoring timeouts."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stops background tasks and flushes remaining partial batches."""
        pass
