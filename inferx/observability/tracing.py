# inferx/observability/tracing.py
"""
InferX Distributed Tracing.

Provides async-safe nested Spans context management, parent trace propagation
via ContextVars, and background buffered trace exporters.
"""

import asyncio
from contextvars import ContextVar
import time
import uuid
from typing import Any, Dict, List, Optional

from inferx.observability.interfaces import ITracer, SpanData
from inferx.utils.logging import get_logger

logger = get_logger("observability.tracing")

# Task-local ContextVar tracking active parent span context
parent_span_var: ContextVar[Optional[SpanData]] = ContextVar(
    "parent_span", default=None
)


class Span:
    """
    Async context manager measuring and tracking execution spans.
    """

    def __init__(
        self, tracer: "Tracer", name: str, attributes: Optional[Dict[str, Any]] = None
    ) -> None:
        self.tracer = tracer
        self.name = name
        self.attributes = attributes or {}

        self.span_id = str(uuid.uuid4())[:16]
        self.trace_id = ""
        self.parent_span_id: Optional[str] = None
        self.start_time_ns = 0

        self._token: Any = None

    async def __aenter__(self) -> "Span":
        self.start_time_ns = time.perf_counter_ns()

        # 1. Context propagation check: resolve parent span
        parent = parent_span_var.get()
        if parent:
            self.parent_span_id = parent.span_id
            self.trace_id = parent.trace_id
        else:
            self.trace_id = str(uuid.uuid4())[:16]

        # 2. Convert to SpanData record
        span_record = SpanData(
            span_id=self.span_id,
            trace_id=self.trace_id,
            parent_span_id=self.parent_span_id,
            name=self.name,
            start_time_ns=self.start_time_ns,
            end_time_ns=0,
            attributes=self.attributes,
        )

        # 3. Bind active context
        self._token = parent_span_var.set(span_record)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        end_time_ns = time.perf_counter_ns()

        # Log exception telemetry if block failed
        if exc_type is not None:
            self.attributes["error"] = True
            self.attributes["error.type"] = exc_type.__name__
            self.attributes["error.message"] = str(exc_val)

        # Create finalized SpanData record
        span_record = SpanData(
            span_id=self.span_id,
            trace_id=self.trace_id,
            parent_span_id=self.parent_span_id,
            name=self.name,
            start_time_ns=self.start_time_ns,
            end_time_ns=end_time_ns,
            attributes=self.attributes,
        )

        # 4. Push to exporter buffer
        self.tracer.exporter.export(span_record)

        # 5. Restore parent context
        parent_span_var.reset(self._token)


class BufferedSpanExporter:
    """
    Buffered trace exporter batching completed spans in a background task
    to guarantee <1% performance overhead.
    """

    def __init__(self, flush_interval_sec: float = 1.0, batch_size: int = 100) -> None:
        self.flush_interval = flush_interval_sec
        self.batch_size = batch_size

        self._queue: List[SpanData] = []
        self._exported_spans: List[SpanData] = []  # In-memory storage for testing/logs
        self._lock = threading_lock()

        self._loop_task: Optional[asyncio.Task[None]] = None
        self._is_active = False

    def start(self) -> None:
        """Launches background export loop."""
        self._is_active = True
        try:
            self._loop_task = asyncio.create_task(self._export_loop())
        except RuntimeError:
            # Handle cases where start is called outside active loop (e.g. sync setups)
            self._is_active = False

    async def stop(self) -> None:
        """Gracefully stops export loop, flushing remaining buffers."""
        self._is_active = False
        if self._loop_task:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None

        # Final flush
        self._flush()

    def export(self, span: SpanData) -> None:
        """Pushes a completed span to the buffer."""
        if not self._is_active:
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    self.start()
            except RuntimeError:
                pass

        with self._lock:
            self._queue.append(span)
            if len(self._queue) >= self.batch_size:
                # Trigger immediate flush in background
                self._flush()

    def get_exported_spans(self) -> List[SpanData]:
        """Returns copies of all exported span records."""
        with self._lock:
            return list(self._exported_spans)

    def _flush(self) -> None:
        """Drains buffer queue to persistent memory."""
        with self._lock:
            if not self._queue:
                return

            # Simulated telemetry export payload write
            self._exported_spans.extend(self._queue)
            self._queue.clear()

    async def _export_loop(self) -> None:
        """Flush trigger sleep timer loop."""
        while self._is_active:
            try:
                await asyncio.sleep(self.flush_interval)
                self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error in trace exporter loop: {e}",
                    exc_info=True,
                    component="span_exporter",
                )


class Tracer(ITracer):
    """
    Tracing coordinator orchestrating trace span lifetimes.
    """

    def __init__(self, exporter: Optional[BufferedSpanExporter] = None) -> None:
        self.exporter = exporter or BufferedSpanExporter()
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                self.exporter.start()
        except RuntimeError:
            pass

    def span(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> Span:
        return Span(self, name, attributes)


# Helper function to obtain lock primitives dynamically
def threading_lock() -> Any:
    import threading

    return threading.RLock()
