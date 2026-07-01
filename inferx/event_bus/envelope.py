# inferx/event_bus/envelope.py
"""
InferX Event Envelope.

Defines the EventEnvelope metadata structure encapsulating tracing, priorities,
and correlation identifiers for asynchronous event routing.
"""
from datetime import datetime, timezone
import uuid
from typing import Any, Optional
from pydantic import BaseModel, Field

from inferx.utils.logging import telemetry_context


class EventEnvelope(BaseModel):
    """
    Standard event packaging envelope.
    
    Contains execution metadata (trace and correlation context) to ensure
    trace propagation across event-driven loops.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp_ns: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1e9))
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tenant_id: Optional[str] = None
    priority: int = Field(default=0, ge=0)
    payload: Any

    model_config = {"frozen": True}

    @classmethod
    def create_from_payload(
        cls,
        payload: Any,
        priority: int = 0,
        event_type: Optional[str] = None
    ) -> "EventEnvelope":
        """
        Constructs an EventEnvelope from a typed payload.
        
        Automatically populates tracing and correlation parameters from the
        async ContextVar telemetry context if present.
        """
        # Determine event type string mapping
        evt_type = event_type or type(payload).__name__

        # Harvest active ContextVar attributes
        ctx = telemetry_context.get()
        
        return cls(
            event_type=evt_type,
            trace_id=ctx.get("trace_id"),
            span_id=ctx.get("span_id"),
            request_id=ctx.get("request_id"),
            correlation_id=ctx.get("correlation_id"),
            tenant_id=ctx.get("tenant_id"),
            priority=priority,
            payload=payload
        )

    def __lt__(self, other: Any) -> bool:
        """
        Comparison helper for PriorityQueue implementations.
        
        Orders by priority in descending order (higher priority numbers are dequeued first).
        If priorities match, orders by timestamp in ascending order (older events first).
        """
        if not isinstance(other, EventEnvelope):
            return NotImplemented
        
        if self.priority != other.priority:
            # We want higher priority values to sort first (Max-Heap behavior)
            return self.priority > other.priority
            
        return self.timestamp_ns < other.timestamp_ns
