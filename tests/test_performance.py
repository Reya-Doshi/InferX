# tests/test_performance.py
import unittest
import asyncio
from inferx.performance.benchmark import BenchmarkRunner
from inferx.performance.chaos import ChaosController, FaultInjector
from inferx.performance.load import LoadGenerator
from inferx.performance.profiler import RuntimeProfiler
from inferx.performance.validation import ValidationEngine
from inferx.performance.report import ReportGenerator


class TestPerformance(unittest.IsolatedAsyncioTestCase):
    """Unit and integration tests validating chaos injection, load gen, profiling and reporting."""

    def setUp(self) -> None:
        self.runner = BenchmarkRunner()
        self.chaos = ChaosController()
        self.injector = FaultInjector()
        self.validator = ValidationEngine()
        self.profiler = RuntimeProfiler()

    def test_benchmark_runner_percentiles(self) -> None:
        """Verifies correct calculation of median, P95, and P99 latency percentiles."""
        # Record latencies 1ms to 100ms
        for i in range(1, 101):
            self.runner.record_latency(float(i))
            
        self.runner.record_resource_usage(cpu=25.0, memory_mb=512.0, gpu=80.0)
        self.runner.record_batch(batch_size=8, queue_delay_ms=2.5)

        metrics = self.runner.get_metrics()
        
        self.assertEqual(metrics["count"], 100)
        self.assertEqual(metrics["p50"], 50.0)
        self.assertEqual(metrics["p90"], 90.0)
        self.assertEqual(metrics["p95"], 95.0)
        self.assertEqual(metrics["p99"], 99.0)
        self.assertEqual(metrics["p999"], 100.0)
        self.assertEqual(metrics["cpu_avg"], 25.0)
        self.assertEqual(metrics["memory_avg_mb"], 512.0)
        self.assertEqual(metrics["gpu_avg"], 80.0)
        self.assertEqual(metrics["batch_size_avg"], 8.0)
        self.assertEqual(metrics["queue_delay_avg_ms"], 2.5)

    async def test_chaos_controller_state(self) -> None:
        """Verifies terminating nodes and adding delays updates chaos controller states."""
        await self.chaos.inject_node_failure("node-2")
        await self.chaos.inject_network_delay(50.0)
        await self.chaos.inject_resource_pressure(cpu_stress=True, memory_mb=1024)

        status = self.chaos.get_status()
        self.assertIn("node-2", status["terminated_nodes"])
        self.assertEqual(status["network_delay_ms"], 50.0)
        self.assertTrue(status["cpu_stress_active"])
        self.assertEqual(status["memory_stress_mb"], 1024)

    def test_fault_injector_exceptions(self) -> None:
        """Verifies application-level fault injections raise expected exceptions."""
        # Inject OOM
        self.injector.inject_oom_error()
        with self.assertRaises(MemoryError):
            self.injector.check_oom()

        # Inject Queue Overflow
        self.injector.inject_queue_overflow()
        with self.assertRaises(RuntimeError):
            self.injector.check_queue_overflow()

        # Reset injector
        self.injector.reset()
        # Should not raise exceptions now
        self.injector.check_oom()
        self.injector.check_queue_overflow()

    async def test_load_generator_steady_and_burst(self) -> None:
        """Verifies steady and burst traffic generators populate target latencies lists."""
        load_gen = LoadGenerator()
        
        # Test steady load
        steady_latencies = await load_gen.generate_steady_load(rps=50.0, duration_sec=0.1)
        self.assertTrue(len(steady_latencies) > 0)
        
        # Test burst load
        burst_latencies = await load_gen.generate_burst_load(concurrent_users=5, burst_size=2)
        self.assertEqual(len(burst_latencies), 10)

    def test_validation_engine_slas(self) -> None:
        """Verifies SLA validations fail on breaches and pass on targets match."""
        # SLA Latency Pass
        pass_metrics = {"p95": 40.0}
        self.assertTrue(self.validator.validate_sla(pass_metrics, max_p95_ms=50.0))

        # SLA Latency Violation
        fail_metrics = {"p95": 65.0}
        self.assertFalse(self.validator.validate_sla(fail_metrics, max_p95_ms=50.0))

        # Recovery validation (e.g. 80ms recovery -> Pass, 150ms -> Fail)
        self.assertTrue(self.validator.validate_failover_recovery(0.0, 0.08, max_recovery_ms=100.0))
        self.assertFalse(self.validator.validate_failover_recovery(0.0, 0.15, max_recovery_ms=100.0))

        # Correctness validation
        self.assertTrue(self.validator.validate_correctness("result", "result"))
        self.assertFalse(self.validator.validate_correctness("result", "error"))

    def test_report_generator_formats(self) -> None:
        """Verifies report formatter output layouts contain metric tables."""
        metrics = {
            "count": 100, "throughput_rps": 500.0,
            "p50": 10.0, "p95": 25.0, "p99": 45.0,
            "cpu_avg": 30.0, "memory_avg_mb": 256.0, "gpu_avg": 75.0,
            "batch_size_avg": 4.0, "queue_delay_avg_ms": 1.5
        }

        markdown_report = ReportGenerator.generate_markdown_report(metrics)
        self.assertIn("P95 Latency", markdown_report)
        self.assertIn("Throughput (RPS)", markdown_report)

        html_report = ReportGenerator.generate_html_report(metrics)
        self.assertIn("<title>InferX Performance Dashboard</title>", html_report)
        self.assertIn("25.00 ms", html_report)


if __name__ == "__main__":
    unittest.main()
