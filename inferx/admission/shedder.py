# inferx/admission/shedder.py
"""
InferX Load Shedders & Circuit Breakers.

Implements telemetry-aware backpressure evaluations, priority-aware load shedding,
and standard Circuit Breakers.
"""

import time
import threading

from inferx.core.context import RuntimeContext
from inferx.utils.logging import get_logger

logger = get_logger("admission.shedder")


class BackpressureController:
    """
    Evaluates hardware metrics to detect system load bottlenecks.
    """

    def __init__(
        self,
        max_vram_ratio: float = 0.90,
        max_cpu_ratio: float = 0.85,
        max_active_requests: int = 1000,
    ) -> None:
        self.max_vram_ratio = max_vram_ratio
        self.max_cpu_ratio = max_cpu_ratio
        self.max_active_requests = max_active_requests

    def is_congested(self, context: RuntimeContext) -> bool:
        """
        Evaluates system telemetry against configured limits.

        Returns:
            True if any limit threshold is violated.
        """
        telemetry = context.get_telemetry()

        vram = telemetry.get("vram_utilization", 0.0)
        cpu = telemetry.get("cpu_utilization", 0.0)
        active = telemetry.get("active_requests", 0)

        if vram >= self.max_vram_ratio:
            return True
        if cpu >= self.max_cpu_ratio:
            return True
        if active >= self.max_active_requests:
            return True
        return False


class LoadShedder:
    """
    Implements priority-aware load shedding to drop low-value traffic under load.
    """

    def __init__(self, backpressure_controller: BackpressureController) -> None:
        self.controller = backpressure_controller

    def should_shed(self, priority: int, context: RuntimeContext) -> bool:
        """
        Evaluates if the request should be shed based on its priority and system load.

        Priority mapping:
            - Priority < 2 (Low): Shed if system is congested.
            - Priority < 4 (Medium): Shed if system is highly congested (VRAM >= watermark + 5%).
            - Priority >= 4 (High): Admitted unless system is in extreme state (VRAM >= 95%).
        """
        if not self.controller.is_congested(context):
            return False

        telemetry = context.get_telemetry()
        vram = telemetry.get("vram_utilization", 0.0)
        watermark = self.controller.max_vram_ratio

        # Priority 0-1: Low priority, drop under normal congestion
        if priority < 2:
            return True

        # Priority 2-3: Medium priority, drop if VRAM exceeds watermark by 5%
        if priority < 4 and vram >= min(0.95, watermark + 0.05):
            return True

        # Priority 4+: High priority, drop only under extreme load (VRAM >= 95%)
        if vram >= 0.95:
            return True

        return False


class CircuitBreaker:
    """
    Implements a Circuit Breaker pattern to fail fast when errors spike.

    States:
        - CLOSED: Traffic flows normally.
        - OPEN: Rejects all requests immediately.
        - HALF_OPEN: Admits a limited number of requests to probe downstream health.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown_sec: float = 5.0,
        probe_success_threshold: int = 3,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown_sec = cooldown_sec
        self.probe_success_threshold = probe_success_threshold

        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.failures = 0
        self.successes = 0
        self.last_state_change = time.time()
        self.lock = threading.Lock()

    def allow_request(self) -> bool:
        """Checks if the circuit allows the request to proceed."""
        with self.lock:
            now = time.time()
            if self.state == "OPEN":
                # Check if cooldown has expired to transition to HALF_OPEN
                if now - self.last_state_change >= self.cooldown_sec:
                    self.state = "HALF_OPEN"
                    self.successes = 0
                    self.last_state_change = now
                    logger.warning(
                        "Circuit Breaker transitioned to HALF_OPEN. Probing health.",
                        component="circuit_breaker",
                    )
                    return True
                return False
            return True

    def record_success(self) -> None:
        """Records a successful execution, closing the circuit if probe succeeds."""
        with self.lock:
            if self.state == "HALF_OPEN":
                self.successes += 1
                if self.successes >= self.probe_success_threshold:
                    self.state = "CLOSED"
                    self.failures = 0
                    self.last_state_change = time.time()
                    logger.info(
                        "Circuit Breaker transitioned to CLOSED. Downstream recovered.",
                        component="circuit_breaker",
                    )

    def record_failure(self) -> None:
        """Records a failure, tripping the circuit if threshold is hit."""
        with self.lock:
            if self.state in ["CLOSED", "HALF_OPEN"]:
                self.failures += 1
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
                    self.last_state_change = time.time()
                    logger.error(
                        "Circuit Breaker transitioned to OPEN. Dropping downstream calls.",
                        component="circuit_breaker",
                    )
