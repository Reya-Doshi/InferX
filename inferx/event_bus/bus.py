# inferx/event_bus/bus.py
"""
InferX Event Bus Engine.

Implements the async publish/subscribe orchestration, prioritized queues dispatching,
and historical event replays.
"""
import asyncio
from datetime import datetime, timezone
import uuid
from typing import Any, Dict, List, Optional, Union

from inferx.event_bus.envelope import EventEnvelope
from inferx.event_bus.interfaces import IEventBus, IEventPersister, IDeadLetterQueue
from inferx.event_bus.metrics import EventBusMetrics
from inferx.utils.logging import get_logger

logger = get_logger("event_bus")


class InMemoryEventPersister(IEventPersister):
    """
    In-memory implementation of IEventPersister.
    
    Acts as a ring-buffered repository for historical event queries.
    """
    def __init__(self, capacity: int = 10000) -> None:
        self.capacity = capacity
        self._store: List[EventEnvelope] = []
        self._lock = asyncio.Lock()

    async def save(self, envelope: EventEnvelope) -> None:
        async with self._lock:
            if len(self._store) >= self.capacity:
                self._store.pop(0)
            self._store.append(envelope)

    async def fetch_range(self, start_ns: int, end_ns: int) -> List[EventEnvelope]:
        async with self._lock:
            return [e for e in self._store if start_ns <= e.timestamp_ns <= end_ns]


class Subscription:
    """Represents a registered consumer queue and target filters."""
    def __init__(self, sub_id: str, event_type: str, queue: Any, is_priority: bool) -> None:
        self.sub_id = sub_id
        self.event_type = event_type
        self.queue = queue
        self.is_priority = is_priority


class EventBus(IEventBus):
    """
    Asynchronous event bus orchestrator.
    
    Enables priority queue dispatches, event log storage, and non-blocking publishers.
    """
    def __init__(
        self,
        persister: Optional[IEventPersister] = None,
        dlq: Optional[IDeadLetterQueue] = None,
        metrics: Optional[EventBusMetrics] = None,
        queue_capacity: int = 5000
    ) -> None:
        self.persister = persister or InMemoryEventPersister()
        self.dlq = dlq
        self.metrics = metrics or EventBusMetrics()
        self.queue_capacity = queue_capacity
        
        self._subscriptions: Dict[str, Subscription] = {}
        self._lock = asyncio.Lock()

    async def publish(self, envelope: EventEnvelope) -> None:
        """
        Routes the event envelope to matching subscriber queues in a non-blocking manner.
        
        Appends the event to the persistence layer.
        """
        # 1. Log event details to persistent storage
        await self.persister.save(envelope)
        self.metrics.record_publish(envelope.event_type)

        async with self._lock:
            # Clone subscriber list to prevent mutability collisions during broadcast
            subscribers = list(self._subscriptions.values())

        for sub in subscribers:
            # Match specific event type or wildcard
            if sub.event_type != "*" and sub.event_type != envelope.event_type:
                continue

            queue = sub.queue
            try:
                # Non-blocking enqueue to protect gateway throughput
                queue.put_nowait(envelope)
                self.metrics.record_delivery(envelope.event_type)
                self.metrics.update_queue_depth(sub.sub_id, queue.qsize())
            except asyncio.QueueFull:
                logger.error(
                    f"Subscriber queue {sub.sub_id} full. Dropping message.",
                    event_id=envelope.event_id,
                    component="event_bus"
                )
                self.metrics.record_failure(envelope.event_type)
                
                # Route overflow events to DLQ
                if self.dlq:
                    # Run in background to prevent publisher blocking
                    asyncio.create_task(
                        self.dlq.route_to_dlq(
                            envelope,
                            reason=f"Queue capacity overflow on subscription {sub.sub_id}"
                        )
                    )

    def subscribe(self, event_type: str, priority_queue: bool = False) -> str:
        """Registers a queue channel to retrieve events of a specific type."""
        sub_id = str(uuid.uuid4())
        
        # Instantiate correct queue class matching priority requirements
        if priority_queue:
            queue = asyncio.PriorityQueue(maxsize=self.queue_capacity)
        else:
            queue = asyncio.Queue(maxsize=self.queue_capacity)

        sub = Subscription(
            sub_id=sub_id,
            event_type=event_type,
            queue=queue,
            is_priority=priority_queue
        )
        
        # Lock-free register
        self._subscriptions[sub_id] = sub
        logger.info(f"Subscriber registered. ID: {sub_id} (Type: {event_type})", component="event_bus")
        return sub_id

    def get_queue(self, sub_id: str) -> Any:
        """Returns the queue associated with the subscription."""
        sub = self._subscriptions.get(sub_id)
        if sub is None:
            raise KeyError(f"Subscription {sub_id} not found.")
        return sub.queue

    def unsubscribe(self, sub_id: str) -> None:
        """Removes the subscription and cleans up active metric telemetry."""
        if sub_id in self._subscriptions:
            self._subscriptions.pop(sub_id)
            self.metrics.remove_queue_metrics(sub_id)
            logger.info(f"Subscriber unregistered. ID: {sub_id}", component="event_bus")

    async def replay(self, start_ns: int, sub_id: str) -> None:
        """Fetches historical records from storage and writes them to the subscriber queue."""
        sub = self._subscriptions.get(sub_id)
        if sub is None:
            raise KeyError(f"Subscription {sub_id} not found.")

        # Query range from start_ns to current time in nanoseconds
        now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
        historical_envelopes = await self.persister.fetch_range(start_ns, now_ns)

        logger.info(
            f"Replaying {len(historical_envelopes)} historical events for subscriber {sub_id}.",
            component="event_bus"
        )

        for envelope in historical_envelopes:
            if sub.event_type == "*" or sub.event_type == envelope.event_type:
                # Await space if queue fills up to ensure replay logs are not discarded
                await sub.queue.put(envelope)
                self.metrics.update_queue_depth(sub_id, sub.queue.qsize())
