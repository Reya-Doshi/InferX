# inferx/observability/interfaces.py
"""
InferX Observability Interfaces.

Declares the telemetry span data representations, metrics registry hook points,
and health/alert interfaces.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SpanData(BaseModel):
    """
    Data model representing a single distributed trace span.
    """
    span_id: str
    trace_id: str
    parent_span_id: Optional[str] = None
    name: str
    start_time_ns: int
    end_time_ns: int
    attributes: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}


class ITracer(ABC):
    """Interface representing the distributed tracing context coordinator."""

    @abstractmethod
    def span(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> Any:
        """Creates and returns an async context manager representing a trace span."""
        pass


class IMetricsRegistry(ABC):
    """Interface representing the central metrics aggregator."""

    @abstractmethod
    def counter(self, name: str, description: str, labels: Optional[Dict[str, str]] = None) -> Any:
        """Retrieves or registers a Counter metric."""
        pass

    @abstractmethod
    def gauge(self, name: str, description: str, labels: Optional[Dict[str, str]] = None) -> Any:
        """Retrieves or registers a Gauge metric."""
        pass

    @abstractmethod
    def histogram(self, name: str, description: str, buckets: List[float], labels: Optional[Dict[str, str]] = None) -> Any:
        """Retrieves or registers a Histogram metric."""
        pass
