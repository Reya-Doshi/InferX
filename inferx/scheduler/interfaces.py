# inferx/scheduler/interfaces.py
"""
InferX Scheduling Engine Interfaces.

Defines structural models for scheduled tasks, policy behaviors,
and core scheduling operations.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Optional
from pydantic import BaseModel, Field


class ScheduledRequest(BaseModel):
    """
    Data model representing a task placed in the scheduling queues.

    Contains time margins, priority status, and payload details.
    """

    request_id: str
    tenant_id: str
    priority: int = Field(default=0, ge=0)  # Higher values represent higher priority
    max_latency_ms: float = Field(
        default=30000.0, gt=0.0
    )  # Maximum allowed queue delay
    enqueue_timestamp_ns: int = Field(
        default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1e9)
    )
    payload: Any

    # Internal attribute tracked by dynamic priority/aging algorithms
    aged_priority: float = 0.0

    model_config = {"arbitrary_types_allowed": True}

    @property
    def deadline_ns(self) -> int:
        """Calculates absolute epoch deadline in nanoseconds."""
        return self.enqueue_timestamp_ns + int(self.max_latency_ms * 1_000_000)


class ISchedulingPolicy(ABC):
    """Abstract interface defining the operational hooks for scheduling policies."""

    @abstractmethod
    def push(self, request: ScheduledRequest) -> None:
        """Adds a request to the policy's inner queue structures (non-blocking)."""
        pass

    @abstractmethod
    def pop(self) -> Optional[ScheduledRequest]:
        """Extracts the next request matching the policy's sorting criteria (non-blocking)."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Returns the total number of enqueued requests within the policy queues."""
        pass


class IScheduler(ABC):
    """Abstract interface defining thread-safe scheduling manager operations."""

    @abstractmethod
    async def enqueue(self, request: ScheduledRequest) -> None:
        """Enqueues a task into the system (blocking if limits are hit)."""
        pass

    @abstractmethod
    async def dequeue(self) -> ScheduledRequest:
        """Pops and returns the next task, blocking asynchronously if the queues are empty."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Returns the total number of enqueued tasks across all queues."""
        pass
