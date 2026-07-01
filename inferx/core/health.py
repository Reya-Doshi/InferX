# inferx/core/health.py
"""
InferX Health Manager.

Monitors hardware telemetry (CPU, GPU, RAM) and generates detailed subsystem health reports.
"""
import asyncio
from typing import Any, Callable, Coroutine, Optional
from pydantic import BaseModel, Field

from inferx.interfaces.core import IHealthManager
from inferx.utils.logging import get_logger

# Optional imports for hardware tracking
try:
    import psutil
except ImportError:
    psutil = None

try:
    import pynvml
except ImportError:
    pynvml = None

logger = get_logger("health")


class HardwareMetrics(BaseModel):
    cpu_utilization: float
    ram_utilization: float
    vram_utilization: float
    vram_allocated_bytes: int
    vram_total_bytes: int


class HealthReport(BaseModel):
    status: str  # INITIALIZING, READY, RUNNING, DEGRADED, SHUTTING_DOWN, FAILED
    metrics: HardwareMetrics
    workers: dict[str, str] = Field(default_factory=dict)
    plugins: dict[str, str] = Field(default_factory=dict)
    scheduler: dict[str, str] = Field(default_factory=dict)
    batcher: dict[str, str] = Field(default_factory=dict)
    queue: dict[str, str] = Field(default_factory=dict)
    models: dict[str, str] = Field(default_factory=dict)


class HealthManager(IHealthManager):
    """
    Implements structured health verification checks.
    
    Exposes sub-status details for all components and hardware systems.
    """
    def __init__(self, vram_watermark: float) -> None:
        self.vram_watermark = vram_watermark
        
        # Categorized callback registries
        self._providers: dict[str, dict[str, Callable[[], Coroutine[Any, Any, bool]]]] = {
            "workers": {},
            "plugins": {},
            "scheduler": {},
            "batcher": {},
            "queue": {},
            "models": {}
        }
        
        self._last_report: Optional[HealthReport] = None
        self._nvml_initialized = False
        self._initialize_nvml()

    def register_provider(
        self,
        name: str,
        check_fn: Callable[[], Coroutine[Any, Any, bool]],
        domain: str = "workers"
    ) -> None:
        """Registers a named check callback under a specific system domain."""
        if domain not in self._providers:
            raise ValueError(f"Invalid health domain: {domain}. Choices: {list(self._providers.keys())}")
        
        self._providers[domain][name] = check_fn
        logger.info(f"Registered health provider '{name}' in domain '{domain}'", component="health")

    async def evaluate_health(self) -> HealthReport:
        """Polls CPU, GPU, RAM, and invokes registered subsystem checks asynchronously."""
        status = "READY"
        domains_results: dict[str, dict[str, str]] = {
            "workers": {},
            "plugins": {"wasm_engine": "READY"},  # Placeholders
            "scheduler": {"scheduler_loop": "READY"},
            "batcher": {"batcher_aggregator": "READY"},
            "queue": {"priority_queues": "READY"},
            "models": {"weights_context": "READY"}
        }

        # 1. Execute all registered callbacks
        for domain, check_dict in self._providers.items():
            for name, check_fn in check_dict.items():
                try:
                    is_healthy = await asyncio.wait_for(check_fn(), timeout=1.0)
                    domains_results[domain][name] = "HEALTHY" if is_healthy else "UNHEALTHY"
                    if not is_healthy:
                        status = "DEGRADED"
                except Exception as e:
                    logger.error(
                        f"Health check '{name}' in domain '{domain}' failed: {e}",
                        exc_info=True,
                        component="health"
                    )
                    domains_results[domain][name] = f"ERROR: {str(e)}"
                    status = "DEGRADED"

        # 2. Poll hardware telemetry metrics
        cpu_util = self._get_cpu_usage()
        ram_util = self._get_ram_usage()
        vram_usage, vram_max = self._get_vram_usage()
        vram_util = vram_usage / vram_max if vram_max > 0 else 0.0

        if vram_util > self.vram_watermark:
            logger.warning(
                f"VRAM utilization ({vram_util:.2f}) exceeds high watermark threshold ({self.vram_watermark:.2f})",
                component="health"
            )
            status = "DEGRADED"

        metrics = HardwareMetrics(
            cpu_utilization=cpu_util,
            ram_utilization=ram_util,
            vram_utilization=vram_util,
            vram_allocated_bytes=vram_usage,
            vram_total_bytes=vram_max
        )

        report = HealthReport(
            status=status,
            metrics=metrics,
            workers=domains_results["workers"],
            plugins=domains_results["plugins"],
            scheduler=domains_results["scheduler"],
            batcher=domains_results["batcher"],
            queue=domains_results["queue"],
            models=domains_results["models"]
        )

        self._last_report = report
        return report

    def get_last_status(self) -> Optional[HealthReport]:
        """Returns the most recent HealthReport snapshot."""
        return self._last_report

    def _initialize_nvml(self) -> None:
        """Attempts to initialize NVML hardware library binding."""
        if pynvml is None:
            logger.warning("pynvml not installed. GPU metrics will be simulated.", component="health")
            return
        try:
            pynvml.nvmlInit()
            self._nvml_initialized = True
            logger.info("NVML bindings loaded successfully.", component="health")
        except Exception as e:
            logger.error(f"Failed to initialize NVML: {e}", component="health")

    def _get_cpu_usage(self) -> float:
        if psutil is not None:
            return float(psutil.cpu_percent())
        return 0.10

    def _get_ram_usage(self) -> float:
        if psutil is not None:
            return float(psutil.virtual_memory().percent / 100.0)
        return 0.25

    def _get_vram_usage(self) -> tuple[int, int]:
        if self._nvml_initialized and pynvml is not None:
            try:
                handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                return int(info.used), int(info.total)
            except Exception as e:
                logger.error(f"Failed to query GPU memory specs: {e}", component="health")
        return 1073741824, 8589934592  # 1GB used of 8GB total (Simulated)
