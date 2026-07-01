# inferx/performance/interfaces.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class IBenchmarkRunner(ABC):
    """Interface for collecting latencies, throughput, and system resource metrics."""

    @abstractmethod
    def record_latency(self, latency_ms: float) -> None:
        """Records a request duration measurement in milliseconds."""
        pass

    @abstractmethod
    def record_resource_usage(self, cpu: float, memory_mb: float, gpu: float) -> None:
        """Records instant CPU percentage, memory MB, and GPU utilization metrics."""
        pass

    @abstractmethod
    def record_batch(self, batch_size: int, queue_delay_ms: float) -> None:
        """Records batch size and request scheduling delay durations."""
        pass

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Calculates tail percentiles (P50, P90, P95, P99, P999) and averages."""
        pass


class IChaosController(ABC):
    """Interface for injecting infrastructure crash and pressure anomalies."""

    @abstractmethod
    async def inject_node_failure(self, node_id: str) -> None:
        """Simulates terminating a cluster node completely."""
        pass

    @abstractmethod
    async def inject_network_delay(self, latency_ms: float) -> None:
        """Simulates transit package transmission delay latency."""
        pass

    @abstractmethod
    async def inject_resource_pressure(self, cpu_stress: bool, memory_mb: int) -> None:
        """Injects artificial system CPU and memory utilization pressure."""
        pass


class IFaultInjector(ABC):
    """Interface for dynamically injecting application-level logical faults."""

    @abstractmethod
    def inject_timeout(self, probability: float) -> None:
        """Configures probability of throwing timeout faults on incoming calls."""
        pass

    @abstractmethod
    def inject_oom_error(self) -> None:
        """Forces an Out-Of-Memory failure on the next executor call."""
        pass

    @abstractmethod
    def inject_queue_overflow(self) -> None:
        """Simulates full admission controller queues, causing drops."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Clears all configured fault triggers."""
        pass


class ILoadGenerator(ABC):
    """Interface for simulating streaming, burst, and steady concurrent request traffic."""

    @abstractmethod
    async def generate_steady_load(self, rps: float, duration_sec: float) -> List[float]:
        """Generates steady rate requests, returning a list of request latencies."""
        pass

    @abstractmethod
    async def generate_burst_load(self, concurrent_users: int, burst_size: int) -> List[float]:
        """Simulates rapid user arrivals with large request pools."""
        pass


class IValidationEngine(ABC):
    """Interface for validating SLA targets, failovers, and correctness regressions."""

    @abstractmethod
    def validate_sla(self, metrics: Dict[str, Any], max_p95_ms: float) -> bool:
        """Verifies P95 metrics satisfy target SLA parameters."""
        pass

    @abstractmethod
    def validate_failover_recovery(self, failover_start_time: float, recovery_time: float, max_recovery_ms: float) -> bool:
        """Verifies cluster leadership recovery is within failover limits."""
        pass
