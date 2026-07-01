# inferx/observability/manager.py
"""
InferX Telemetry Manager.

Coordinates tracing registries, Prometheus exporters, alert checks,
and dashboard schema exports.
"""
from typing import Any, Dict, List, Optional, Tuple
import threading

from inferx.observability.metrics import MetricsRegistry
from inferx.observability.tracing import Tracer
from inferx.observability.health import HealthAggregator
from inferx.observability.alert import AlertManager
from inferx.utils.logging import get_logger

logger = get_logger("observability.manager")


class TelemetryManager:
    """
    Unified manager for Metrics, Traces, Health check aggregations, and Alerting.
    """
    def __init__(
        self,
        metrics_registry: Optional[MetricsRegistry] = None,
        tracer: Optional[Tracer] = None,
        health_aggregator: Optional[HealthAggregator] = None,
        alert_manager: Optional[AlertManager] = None
    ) -> None:
        self.metrics = metrics_registry or MetricsRegistry()
        self.tracer = tracer or Tracer()
        self.health = health_aggregator or HealthAggregator()
        self.alerts = alert_manager or AlertManager()
        self._lock = threading.Lock()

    def get_dashboard_data(self) -> Dict[str, Any]:
        """
        Compiles current metrics, health statuses, and tracing delays
        into a unified JSON dashboard schema.
        
        Returns:
            A dictionary containing live operations statistics.
        """
        # Parse Prometheus metrics registry values to populate snapshot
        # For simplicity, we can fetch Counter/Gauge metric values directly
        # or aggregate standard metrics fields.
        snapshot = {}
        try:
            # Simulated dashboard query payload mapping
            snapshot = {
                "active_connections": self._get_gauge_val("gateway_connections_active"),
                "requests_throughput_sec": self._get_counter_val("gateway_requests_total"),
                "avg_inference_latency_ms": self._get_gauge_val("model_inference_latency_avg_ms"),
                "queue_depth": self._get_gauge_val("scheduler_queue_depth"),
                "worker_utilization": self._get_gauge_val("worker_utilization_ratio"),
                "alerts_active": len(self.alerts._triggered_alerts)
            }
        except Exception as e:
            logger.warning(f"Error compiling dashboard data: {e}", component="telemetry_manager")
            
        return snapshot

    def _get_gauge_val(self, name: str) -> float:
        """Retrieves raw gauge value if registered."""
        try:
            metric = self.metrics._metrics.get(name)
            if metric and hasattr(metric, "_values"):
                # Return sum of all label values
                return sum(metric._values.values())
        except Exception:
            pass
        return 0.0

    def _get_counter_val(self, name: str) -> float:
        """Retrieves raw counter value if registered."""
        try:
            metric = self.metrics._metrics.get(name)
            if metric and hasattr(metric, "_values"):
                return sum(metric._values.values())
        except Exception:
            pass
        return 0.0
