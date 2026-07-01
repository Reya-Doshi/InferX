# inferx/event_bus/interfaces.py
"""
InferX Event Bus Interface Specifications.

Declares the structural interfaces for publishing, subscribing, replaying,
persisting, and handling dead letter event flows.
"""
from abc import ABC, abstractmethod
from typing import Any, Optional, TypeVar

T = TypeVar("T")


class IEventBus(ABC):
    """Core Event Bus coordinating asynchronous pub/sub channels and prioritized distribution."""

    @abstractmethod
    async def publish(self, envelope: Any) -> None:
        """Publishes an event envelope asynchronously, routing to all active subscribers."""
        pass

    @abstractmethod
    def subscribe(self, event_type: str, priority_queue: bool = False) -> str:
        """
        Creates a subscriber channel for an event type.
        
        Args:
            event_type: String representation of the target event class name.
            priority_queue: If True, uses a PriorityQueue instead of a FIFO Queue.
            
        Returns:
            A unique subscription ID.
        """
        pass

    @abstractmethod
    def get_queue(self, sub_id: str) -> Any:
        """Retrieves the message queue (Queue or PriorityQueue) bound to a subscription ID."""
        pass

    @abstractmethod
    def unsubscribe(self, sub_id: str) -> None:
        """Terminates a subscription and cleans up associated event queues."""
        pass

    @abstractmethod
    async def replay(self, start_ns: int, sub_id: str) -> None:
        """
        Replays historical events from the persistent logs to the target subscription queue.
        
        Args:
            start_ns: Starting epoch timestamp in nanoseconds.
            sub_id: Target subscription identifier.
        """
        pass


class IEventPersister(ABC):
    """Interface for historical event logging and replay stores."""

    @abstractmethod
    async def save(self, envelope: Any) -> None:
        """Saves an event envelope to the persistent log repository."""
        pass

    @abstractmethod
    async def fetch_range(self, start_ns: int, end_ns: int) -> list[Any]:
        """Fetches all logged event envelopes within the timestamp range."""
        pass


class IDeadLetterQueue(ABC):
    """Interface for routing failed dispatches or corrupted event envelopes."""

    @abstractmethod
    async def route_to_dlq(
        self,
        envelope: Any,
        reason: str,
        exception: Optional[Exception] = None
    ) -> None:
        """
        Routes a failed event envelope to the dead letter log, capturing trace contexts.
        
        Args:
            envelope: The failed EventEnvelope.
            reason: Explanatory error diagnosis.
            exception: Caught exception instance.
        """
        pass
