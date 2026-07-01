# inferx/event_bus/dlq.py
"""
InferX Event Bus Dead Letter Queue.

Captures failed event dispatches, wrapping diagnostic exceptions alongside the original
EventEnvelope for observability indexing.
"""

from typing import List, Optional, Tuple
import asyncio

from inferx.event_bus.envelope import EventEnvelope
from inferx.event_bus.interfaces import IDeadLetterQueue
from inferx.utils.logging import get_logger, telemetry_context

logger = get_logger("event_bus.dlq")


class DeadLetterQueue(IDeadLetterQueue):
    """
    Implements a Dead Letter Queue (DLQ) memory repository.

    Logs dispatch errors structured with trace contexts and retains failed
    envelopes for administrator extraction.
    """

    def __init__(self, capacity: int = 1000) -> None:
        self.capacity = capacity
        self._dlq_buffer: List[Tuple[EventEnvelope, str, Optional[str]]] = []
        self._lock = asyncio.Lock()

    async def route_to_dlq(
        self,
        envelope: EventEnvelope,
        reason: str,
        exception: Optional[Exception] = None,
    ) -> None:
        """
        Appends failed events to the buffer and writes structured logs.

        Extracts tracing context fields from the envelope to ensure the log records
        bind to the correct original request trace ID.
        """
        async with self._lock:
            # Enforce circular buffer bounds if capacity is exceeded
            if len(self._dlq_buffer) >= self.capacity:
                self._dlq_buffer.pop(0)

            exc_msg = str(exception) if exception else None
            self._dlq_buffer.append((envelope, reason, exc_msg))

        # Propagate tracing details from the original envelope to the logging contextvars
        ctx = telemetry_context.get().copy()
        ctx.update(
            {
                "trace_id": envelope.trace_id,
                "span_id": envelope.span_id,
                "request_id": envelope.request_id,
                "correlation_id": envelope.correlation_id,
                "tenant_id": envelope.tenant_id,
            }
        )
        token = telemetry_context.set(ctx)

        try:
            logger.error(
                f"Event dispatch failed. Routed to DLQ. Reason: {reason}",
                event_id=envelope.event_id,
                event_type=envelope.event_type,
                priority=envelope.priority,
                exc_info=exception,
                component="dlq",
            )
        finally:
            telemetry_context.reset(token)

    async def get_failed_events(self) -> List[Tuple[EventEnvelope, str, Optional[str]]]:
        """Returns the failed event list."""
        async with self._lock:
            return list(self._dlq_buffer)

    async def clear(self) -> None:
        """Clears all entries in the DLQ."""
        async with self._lock:
            self._dlq_buffer.clear()
