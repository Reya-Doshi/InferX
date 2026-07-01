# tests/test_observability.py
"""
InferX Observability Test Suite.

Verifies Prometheus metric formatting, nested tracing span propagation,
health aggregator probe collections, alerting rules, and timeline execution profiles.
"""

import unittest
from typing import Any, Dict, Tuple

from inferx.observability.metrics import MetricsRegistry
from inferx.observability.tracing import Tracer, parent_span_var
from inferx.observability.health import HealthAggregator
from inferx.observability.alert import AlertManager
from inferx.observability.profiler import ExecutionTimeline
from inferx.observability.manager import TelemetryManager


class TestObservability(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the Observability Platform."""

    def setUp(self) -> None:
        self.metrics = MetricsRegistry()
        self.tracer = Tracer()
        self.health = HealthAggregator()
        self.alerts = AlertManager()

        self.telemetry = TelemetryManager(
            metrics_registry=self.metrics,
            tracer=self.tracer,
            health_aggregator=self.health,
            alert_manager=self.alerts,
        )

    async def asyncTearDown(self) -> None:
        await self.tracer.exporter.stop()

    def test_prometheus_metrics_export(self) -> None:
        counter = self.metrics.counter("requests_total", "Total requests processed")
        counter.inc(5.0, labels={"model": "llama", "status": "200"})

        gauge = self.metrics.gauge("vram_usage_bytes", "VRAM usage in bytes")
        gauge.set(4 * 1024 * 1024 * 1024, labels={"gpu": "0"})

        histogram = self.metrics.histogram(
            "inference_latency_sec", "Inference latency", buckets=[0.01, 0.05, 0.1]
        )
        histogram.observe(0.008, labels={"model": "llama"})
        histogram.observe(0.045, labels={"model": "llama"})
        histogram.observe(0.120, labels={"model": "llama"})

        export_str = self.metrics.export_prometheus()

        # Verify Counter formatting
        self.assertIn("# HELP requests_total Total requests processed", export_str)
        self.assertIn("# TYPE requests_total counter", export_str)
        self.assertIn('requests_total{model="llama", status="200"} 5.0', export_str)

        # Verify Gauge formatting
        self.assertIn('vram_usage_bytes{gpu="0"} 4294967296.0', export_str)

        # Verify Histogram buckets formatting
        self.assertIn(
            'inference_latency_sec_bucket{model="llama", le="0.01"} 1', export_str
        )
        self.assertIn(
            'inference_latency_sec_bucket{model="llama", le="0.05"} 2', export_str
        )
        self.assertIn(
            'inference_latency_sec_bucket{model="llama", le="0.1"} 2', export_str
        )
        self.assertIn(
            'inference_latency_sec_bucket{model="llama", le="inf"} 3', export_str
        )
        self.assertIn('inference_latency_sec_sum{model="llama"} 0.173', export_str)
        self.assertIn('inference_latency_sec_count{model="llama"} 3', export_str)

    async def test_nested_tracing_spans(self) -> None:
        # Create trace spans
        async with self.tracer.span(
            "parent_stage", attributes={"tier": "gateway"}
        ) as parent:
            self.assertEqual(parent_span_var.get().span_id, parent.span_id)

            async with self.tracer.span(
                "child_stage", attributes={"tier": "scheduler"}
            ) as child:
                self.assertEqual(parent_span_var.get().span_id, child.span_id)
                # Verify trace ID links, and parent span ID references
                self.assertEqual(child.trace_id, parent.trace_id)
                self.assertEqual(child.parent_span_id, parent.span_id)

        # Flush spans manually for testing
        self.tracer.exporter._flush()
        exported = self.tracer.exporter.get_exported_spans()

        self.assertEqual(len(exported), 2)
        # Order in exported: child first (completed first), then parent
        self.assertEqual(exported[0].name, "child_stage")
        self.assertEqual(exported[1].name, "parent_stage")
        self.assertEqual(exported[0].trace_id, exported[1].trace_id)
        self.assertEqual(exported[0].parent_span_id, exported[1].span_id)

    async def test_health_aggregator_probes(self) -> None:
        # Register mock health probes
        async def check_gpu() -> Tuple[bool, str]:
            return True, "GPU normal"

        async def check_vram() -> Tuple[bool, str]:
            # Simulate failure check
            return False, "VRAM threshold exceeded"

        self.health.register_probe("gpu", check_gpu)
        self.health.register_probe("vram", check_vram)

        healthy, details = await self.health.check_health()

        # vram check failed, so overall healthy is False
        self.assertFalse(healthy)
        self.assertEqual(details["gpu"], "UP")
        self.assertIn("DOWN", details["vram"])

    def test_alert_rules_evaluation(self) -> None:
        # Add rule: trigger if error rate > 5%
        def check_errors(metrics: Dict[str, Any]) -> Tuple[bool, str]:
            err_rate = metrics.get("error_rate", 0.0)
            if err_rate > 5.0:
                return True, f"Error rate is {err_rate}%"
            return False, "Normal"

        self.alerts.add_rule("HighErrorRate", threshold=5.0, check_fn=check_errors)

        alerts_triggered = []

        def handler(name, msg):
            alerts_triggered.append(name)

        self.alerts.register_handler(handler)

        # 1. Under normal conditions
        triggered = self.alerts.evaluate_rules({"error_rate": 2.0})
        self.assertEqual(len(triggered), 0)
        self.assertEqual(len(alerts_triggered), 0)

        # 2. Under warning threshold breach
        triggered = self.alerts.evaluate_rules({"error_rate": 8.0})
        self.assertEqual(len(triggered), 1)
        self.assertEqual(triggered[0], "HighErrorRate")
        self.assertEqual(len(alerts_triggered), 1)

    def test_execution_profiler_timeline(self) -> None:
        from unittest.mock import patch

        # Staggered nanosecond mock timestamps: Start at 0, scheduled at 10ms, batched at 25ms, complete at 30ms
        timestamps = [0, 10_000_000, 25_000_000, 30_000_000]

        with patch("time.perf_counter_ns", side_effect=timestamps):
            timeline = ExecutionTimeline(
                request_id="req-123", slow_request_threshold_ms=50.0
            )
            timeline.record("scheduled")
            timeline.record("batched")
            timeline.record("complete")

        durations = timeline.get_breakdown()
        total_duration = timeline.get_total_duration_ms()

        # Check total time is exactly 30ms
        self.assertEqual(total_duration, 30.0)
        self.assertEqual(durations["start_to_scheduled"], 10.0)
        self.assertEqual(durations["scheduled_to_batched"], 15.0)
        self.assertEqual(durations["batched_to_complete"], 5.0)

        # Verify it does not trigger slow request warn (duration < 50ms)
        self.assertFalse(timeline.check_slow_request())


if __name__ == "__main__":
    unittest.main()
