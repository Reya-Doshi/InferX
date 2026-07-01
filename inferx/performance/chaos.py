# inferx/performance/chaos.py
import logging
import random
from typing import Any, Dict, List
from inferx.performance.interfaces import IChaosController, IFaultInjector

logger = logging.getLogger("inferx.performance.chaos")


class ChaosController(IChaosController):
    """Simulates physical infrastructure failures, node terminations, clock drifts, and CPU/Memory resource constraints."""

    def __init__(self) -> None:
        self.terminated_nodes: List[str] = []
        self.network_delay_ms: float = 0.0
        self.cpu_stress_active: bool = False
        self.memory_stress_mb: int = 0

    async def inject_node_failure(self, node_id: str) -> None:
        self.terminated_nodes.append(node_id)
        logger.warning(f"CHAOS INJECTED: Terminated node {node_id}")

    async def inject_network_delay(self, latency_ms: float) -> None:
        self.network_delay_ms = latency_ms
        logger.warning(
            f"CHAOS INJECTED: Added network latency delay of {latency_ms} ms"
        )

    async def inject_resource_pressure(self, cpu_stress: bool, memory_mb: int) -> None:
        self.cpu_stress_active = cpu_stress
        self.memory_stress_mb = memory_mb
        logger.warning(
            f"CHAOS INJECTED: Applied resource pressure (CPU stress: {cpu_stress}, Memory: {memory_mb} MB)"
        )

    def get_status(self) -> Dict[str, Any]:
        return {
            "terminated_nodes": self.terminated_nodes.copy(),
            "network_delay_ms": self.network_delay_ms,
            "cpu_stress_active": self.cpu_stress_active,
            "memory_stress_mb": self.memory_stress_mb,
        }


class FaultInjector(IFaultInjector):
    """Dynamically triggers logical application errors, timeout failures, and OOM terminations."""

    def __init__(self) -> None:
        self._timeout_prob: float = 0.0
        self._force_oom: bool = False
        self._force_queue_overflow: bool = False

    def inject_timeout(self, probability: float) -> None:
        self._timeout_prob = probability
        logger.warning(
            f"FAULT INJECTED: Timeout probability set to {probability * 100}%"
        )

    def inject_oom_error(self) -> None:
        self._force_oom = True
        logger.warning("FAULT INJECTED: Configured OOM error on next operation call")

    def inject_queue_overflow(self) -> None:
        self._force_queue_overflow = True
        logger.warning("FAULT INJECTED: Configured queue overflow drop scenario")

    def reset(self) -> None:
        self._timeout_prob = 0.0
        self._force_oom = False
        self._force_queue_overflow = False
        logger.info("Fault injector state reset successfully.")

    # Verification helper methods called by execution proxies
    def check_oom(self) -> None:
        if self._force_oom:
            self._force_oom = False
            raise MemoryError("Out of memory: process resource limits exceeded.")

    def check_queue_overflow(self) -> None:
        if self._force_queue_overflow:
            self._force_queue_overflow = False
            raise RuntimeError(
                "Queue overflow: Admission queue capacity exceeded limits."
            )

    def check_timeout(self) -> bool:
        if self._timeout_prob > 0.0:
            return random.random() < self._timeout_prob
        return False
