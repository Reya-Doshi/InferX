# inferx/observability/interfaces.py
"""
InferX Observability Interfaces.

Declares the telemetry span data representations, metrics registry hook points,
and health/alert interfaces.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class SpanData(BaseModel):
    """
    Data model representing a single distributed trace span.
    """

    span_id: str
    trace_id: str
    parent_span_id: str | None = None
    name: str
    start_time_ns: int
    end_time_ns: int
    attributes: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}


class ITracer(ABC):
    """Interface representing the distributed tracing context coordinator."""

    @abstractmethod
    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        """Creates and returns an async context manager representing a trace span."""
        pass


class IMetricsRegistry(ABC):
    """Interface representing the central metrics aggregator."""

    @abstractmethod
    def counter(
        self, name: str, description: str, labels: dict[str, str] | None = None
    ) -> Any:
        """Retrieves or registers a Counter metric."""
        pass

    @abstractmethod
    def gauge(
        self, name: str, description: str, labels: dict[str, str] | None = None
    ) -> Any:
        """Retrieves or registers a Gauge metric."""
        pass

    @abstractmethod
    def histogram(
        self,
        name: str,
        description: str,
        buckets: list[float],
        labels: dict[str, str] | None = None,
    ) -> Any:
        """Retrieves or registers a Histogram metric."""
        pass
