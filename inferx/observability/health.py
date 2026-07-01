# inferx/observability/health.py
"""
InferX Health Aggregator.

Consolidates startup, liveness, and readiness probe checks across runtime subsystems,
evaluating dependency health in parallel.
"""
import asyncio
from typing import Any, Callable, Coroutine, Dict, Tuple
import threading

from inferx.utils.logging import get_logger

logger = get_logger("observability.health")


class HealthAggregator:
    """
    Registry aggregating component checks to verify global system health.
    """
    def __init__(self) -> None:
        # Maps component name -> callback returning (is_healthy, detail_string)
        self._probes: Dict[str, Callable[[], Coroutine[Any, Any, Tuple[bool, str]]]] = {}
        self._lock = threading.Lock()

    def register_probe(self, name: str, callback: Callable[[], Coroutine[Any, Any, Tuple[bool, str]]]) -> None:
        """Registers a component health evaluation callback."""
        with self._lock:
            self._probes[name] = callback
            logger.info(f"Registered health probe check: {name}", component="health_aggregator")

    def unregister_probe(self, name: str) -> None:
        """Removes a component health check."""
        with self._lock:
            self._probes.pop(name, None)

    async def check_health(self) -> Tuple[bool, Dict[str, str]]:
        """
        Executes all registered probes in parallel.
        
        Returns:
            Tuple: (overall_healthy: bool, details_map: dict).
        """
        with self._lock:
            names = list(self._probes.keys())
            callbacks = list(self._probes.values())

        if not callbacks:
            return True, {"status": "healthy", "info": "No registered health probes."}

        # Run all callbacks concurrently to prevent blocking bottleneck
        tasks = [asyncio.create_task(cb()) for cb in callbacks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        overall_healthy = True
        details = {}

        for name, res in zip(names, results):
            if isinstance(res, Exception):
                overall_healthy = False
                details[name] = f"CRITICAL: Probe raised exception: {str(res)}"
            else:
                is_ok, msg = res
                if not is_ok:
                    overall_healthy = False
                details[name] = "UP" if is_ok else f"DOWN: {msg}"

        return overall_healthy, details

    async def check_liveness(self) -> Tuple[bool, Dict[str, str]]:
        """Verifies if components are alive (fail-fast check)."""
        return await self.check_health()

    async def check_readiness(self) -> Tuple[bool, Dict[str, str]]:
        """Verifies if the runtime is ready to accept incoming user traffic."""
        # Readiness checks verify if components are loaded (context state == READY/RUNNING)
        return await self.check_health()
