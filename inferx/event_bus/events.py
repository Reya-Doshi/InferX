# inferx/event_bus/events.py
"""
InferX Typed Event System.

Declares the Pydantic schemas representing concrete payload parameters
for core lifecycle and execution events.
"""

from typing import Optional
from pydantic import BaseModel


class BaseEvent(BaseModel):
    """Base event payload interface ensuring clean Pydantic validations."""

    model_config = {"frozen": True}


class RequestReceived(BaseEvent):
    request_id: str
    model_name: str
    tenant_id: str
    payload_size_bytes: int


class RequestQueued(BaseEvent):
    request_id: str
    priority: int
    queue_depth: int


class BatchCreated(BaseEvent):
    batch_id: str
    batch_size: int
    model_name: str


class WorkerAssigned(BaseEvent):
    worker_id: str
    gpu_id: int
    batch_id: str


class InferenceStarted(BaseEvent):
    batch_id: str
    worker_id: str
    timestamp_ms: float


class InferenceCompleted(BaseEvent):
    batch_id: str
    worker_id: str
    latency_ms: float
    tokens_generated: int


class WorkerFailed(BaseEvent):
    worker_id: str
    gpu_id: int
    exit_code: Optional[int] = None
    reason: str


class CircuitOpened(BaseEvent):
    component: str
    error_rate: float
    trip_reason: str


class CircuitClosed(BaseEvent):
    component: str


class HealthChanged(BaseEvent):
    old_status: str
    new_status: str
    cause: str


class ShutdownStarted(BaseEvent):
    signal_num: int
    active_requests: int


class ShutdownCompleted(BaseEvent):
    exit_code: int
