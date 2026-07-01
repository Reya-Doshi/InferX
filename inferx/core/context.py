# inferx/core/context.py
"""
InferX Runtime Context Manager.

Maintains thread-safe application state tracking, state transition validation rules,
and telemetry statistics.
"""
import asyncio
from enum import Enum
import threading
from typing import Any, Callable, Coroutine, Optional

from inferx.errors.taxonomy import StateTransitionError
from inferx.utils.logging import get_logger, telemetry_context

logger = get_logger("context")


class RuntimeState(Enum):
    INITIALIZING = "INITIALIZING"
    READY = "READY"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    DRAINING = "DRAINING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


# State transition rules matrix
VALID_TRANSITIONS = {
    RuntimeState.INITIALIZING: {RuntimeState.READY, RuntimeState.FAILED},
    RuntimeState.READY: {RuntimeState.RUNNING, RuntimeState.DRAINING, RuntimeState.FAILED},
    RuntimeState.RUNNING: {RuntimeState.DEGRADED, RuntimeState.DRAINING, RuntimeState.FAILED},
    RuntimeState.DEGRADED: {RuntimeState.RUNNING, RuntimeState.DRAINING, RuntimeState.FAILED},
    RuntimeState.DRAINING: {RuntimeState.SHUTTING_DOWN, RuntimeState.FAILED},
    RuntimeState.SHUTTING_DOWN: {RuntimeState.STOPPED, RuntimeState.FAILED},
    RuntimeState.STOPPED: set(),
    RuntimeState.FAILED: set(),
}


class RuntimeContext:
    """
    Central operational context for the InferX engine.
    
    Validates state transitions and updates contextvars for structured logging.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state_lock = asyncio.Lock()
        self._state = RuntimeState.INITIALIZING
        
        # Telemetry metrics
        self._active_requests = 0
        self._total_requests_processed = 0
        self._current_vram_utilization = 0.0
        self._current_cpu_utilization = 0.0
        self._average_queue_latency_ms = 0.0
        
        # State change callbacks
        self._listeners: list[Callable[[RuntimeState, RuntimeState], Coroutine[Any, Any, None]]] = []
        
        # Seed initial state in telemetry logger context
        self._update_log_context(RuntimeState.INITIALIZING)

    @property
    def state(self) -> RuntimeState:
        with self._lock:
            return self._state

    async def transition_to(self, target_state: RuntimeState) -> None:
        """
        Asynchronously transition runtime state after asserting valid transitions.
        
        Notifies all registered callback listeners of the transition.
        """
        async with self._state_lock:
            with self._lock:
                current = self._state

            if target_state not in VALID_TRANSITIONS[current]:
                raise StateTransitionError(
                    message=f"Illegal transition: {current.name} -> {target_state.name}",
                    cause=f"State model does not permit transitions from {current.name} to {target_state.name}."
                )

            with self._lock:
                self._state = target_state

            # Synchronize telemetry context
            self._update_log_context(target_state)
            logger.info(f"System transitioned state: {current.name} -> {target_state.name}", component="context")

            # Await all registered state transition callbacks
            for callback in self._listeners:
                try:
                    await callback(current, target_state)
                except Exception as e:
                    logger.error(
                        f"State listener callback failed: {e}",
                        exc_info=True,
                        component="context"
                    )

    def register_state_listener(
        self,
        callback: Callable[[RuntimeState, RuntimeState], Coroutine[Any, Any, None]]
    ) -> None:
        """Registers a coroutine callback to execute when state changes occur."""
        with self._lock:
            self._listeners.append(callback)

    @property
    def active_requests(self) -> int:
        with self._lock:
            return self._active_requests

    def increment_active_requests(self) -> None:
        """Atomically increments active request count."""
        with self._lock:
            self._active_requests += 1

    def decrement_active_requests(self) -> None:
        """Atomically decrements active request count."""
        with self._lock:
            if self._active_requests > 0:
                self._active_requests -= 1
                self._total_requests_processed += 1

    def update_telemetry(
        self,
        vram_util: float,
        cpu_util: float,
        avg_queue_lat: float
    ) -> None:
        """Updates internal telemetry snapshot figures."""
        with self._lock:
            self._current_vram_utilization = vram_util
            self._current_cpu_utilization = cpu_util
            self._average_queue_latency_ms = avg_queue_lat

    def get_telemetry(self) -> dict[str, Any]:
        """Returns a snapshot of telemetry values."""
        with self._lock:
            return {
                "active_requests": self._active_requests,
                "total_processed": self._total_requests_processed,
                "vram_utilization": self._current_vram_utilization,
                "cpu_utilization": self._current_cpu_utilization,
                "average_queue_latency_ms": self._average_queue_latency_ms,
            }

    def _update_log_context(self, state: RuntimeState) -> None:
        """Updates ContextVar dictionaries so logs capture current state automatically."""
        ctx = telemetry_context.get().copy()
        ctx["runtime_state"] = state.name
        telemetry_context.set(ctx)
