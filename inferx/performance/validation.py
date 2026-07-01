# inferx/performance/validation.py
import logging
from typing import Any, Dict
from inferx.performance.interfaces import IValidationEngine

logger = logging.getLogger("inferx.performance.validation")


class ValidationEngine(IValidationEngine):
    """Evaluates benchmark parameters against SLA limits, checking latency percentiles and correctness validations."""

    def validate_sla(self, metrics: Dict[str, Any], max_p95_ms: float) -> bool:
        """Verifies that P95 tail latency satisfies SLA limits."""
        p95 = metrics.get("p95", 0.0)
        if p95 > max_p95_ms:
            logger.error(
                f"SLA VIOLATION: P95 latency is {p95:.2f} ms (Target limit: {max_p95_ms} ms)"
            )
            return False
        logger.info(
            f"SLA PASS: P95 latency is {p95:.2f} ms (Target limit: {max_p95_ms} ms)"
        )
        return True

    def validate_failover_recovery(
        self, failover_start_time: float, recovery_time: float, max_recovery_ms: float
    ) -> bool:
        """Verifies that cluster failover recovery completes within targets (e.g. 100ms)."""
        recovery_duration_ms = (recovery_time - failover_start_time) * 1000.0
        if recovery_duration_ms > max_recovery_ms:
            logger.error(
                f"FAILOVER SLA VIOLATION: Recovery took {recovery_duration_ms:.2f} ms (Target limit: {max_recovery_ms} ms)"
            )
            return False
        logger.info(
            f"FAILOVER SLA PASS: Recovery completed in {recovery_duration_ms:.2f} ms (Target limit: {max_recovery_ms} ms)"
        )
        return True

    def validate_throughput(self, metrics: Dict[str, Any], min_rps: float) -> bool:
        throughput = metrics.get("throughput_rps", 0.0)
        if throughput < min_rps:
            logger.error(
                f"THROUGHPUT SLA VIOLATION: Current throughput is {throughput:.2f} RPS (Target limit: {min_rps} RPS)"
            )
            return False
        logger.info(
            f"THROUGHPUT SLA PASS: Current throughput is {throughput:.2f} RPS (Target limit: {min_rps} RPS)"
        )
        return True

    def validate_correctness(self, actual: Any, expected: Any) -> bool:
        """Asserts outputs match expected validation references."""
        if actual != expected:
            logger.error(
                f"CORRECTNESS VIOLATION: Actual output '{actual}' does not match expected reference '{expected}'"
            )
            return False
        logger.info("CORRECTNESS PASS: Execution matches expected references.")
        return True

    def validate_zero_copy(self, pin_refs_count: int) -> bool:
        """Asserts zero memory copy references balance cleanly."""
        if pin_refs_count != 0:
            logger.error(
                f"MEMORY FRAGMENTATION WARNING: Pin memory references count is {pin_refs_count} (Expected clean balance: 0)"
            )
            return False
        logger.info("ZERO-COPY PASS: Pinned memory references released successfully.")
        return True
