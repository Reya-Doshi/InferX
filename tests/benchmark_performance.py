# tests/benchmark_performance.py
import asyncio
import random
from inferx.performance.benchmark import BenchmarkRunner
from inferx.performance.load import LoadGenerator
from inferx.performance.profiler import RuntimeProfiler
from inferx.performance.validation import ValidationEngine
from inferx.performance.report import ReportGenerator


async def run_performance_engineering_validation() -> None:
    print("=" * 70)
    print("INFERX PERFORMANCE & CHAOS ENGINEERING FRAMEWORK VALIDATION")
    print("=" * 70)

    # Initialize profiler and metrics recorders
    profiler = RuntimeProfiler()
    runner = BenchmarkRunner()
    validator = ValidationEngine()

    # Start profiling memory allocations and CPU
    profiler.start()

    # Simulate request call simulation (averaging 15ms with 2ms jitter)
    async def request_simulation() -> None:
        latency = random.normalvariate(15.0, 2.0)
        await asyncio.sleep(latency / 1000.0)
        runner.record_latency(latency)
        # Mock resource usage and queue/batch parameters
        runner.record_resource_usage(
            cpu=random.uniform(30.0, 45.0),
            memory_mb=random.uniform(512.0, 580.0),
            gpu=random.uniform(75.0, 85.0),
        )
        runner.record_batch(
            batch_size=random.choice([4, 8, 16]),
            queue_delay_ms=random.uniform(1.0, 3.5),
        )

    # Initialize load generator and dispatch 250 requests/sec load
    load_gen = LoadGenerator(request_fn=request_simulation)
    print("Simulating steady traffic load of 250 RPS...")
    await load_gen.generate_steady_load(rps=250.0, duration_sec=1.0)

    # Stop profiler
    profile_results = profiler.stop()
    metrics = runner.get_metrics()

    # Add profile statistics to metrics dictionary
    metrics["cpu_avg"] = profile_results["cpu_utilization_percent"]
    metrics["peak_memory_bytes"] = profile_results["peak_memory_bytes"]

    # Validate against target SLAs
    print("\nEvaluating SLA verification criteria:")
    sla_passed = validator.validate_sla(metrics, max_p95_ms=50.0)
    throughput_passed = validator.validate_throughput(metrics, min_rps=100.0)

    # Simulate cluster failover recovery timing (e.g. 75ms recovery)
    failover_passed = validator.validate_failover_recovery(
        failover_start_time=0.0, recovery_time=0.075, max_recovery_ms=100.0
    )

    # Compile and write reports
    md_report = ReportGenerator.generate_markdown_report(metrics)
    html_report = ReportGenerator.generate_html_report(metrics)
    json_report = ReportGenerator.generate_json_report(metrics)

    # Output reports to files
    with open("performance_report.md", "w") as f:
        f.write(md_report)
    with open("performance_report.html", "w") as f:
        f.write(html_report)
    with open("performance_report.json", "w") as f:
        f.write(json_report)

    print("\nBenchmark reports successfully compiled and written:")
    print(
        "  - [Markdown Report](file:///c:/Users/lenovo/OneDrive/Desktop/ReyaWeb/InferX/performance_report.md)"
    )
    print(
        "  - [HTML Dashboard](file:///c:/Users/lenovo/OneDrive/Desktop/ReyaWeb/InferX/performance_report.html)"
    )
    print(
        "  - [JSON Report](file:///c:/Users/lenovo/OneDrive/Desktop/ReyaWeb/InferX/performance_report.json)"
    )
    print("-" * 70)
    print(f"Total Client Requests Run : {metrics['count']}")
    print(f"Steady State Throughput   : {metrics['throughput_rps']:.2f} req/sec")
    print(f"P50 Latency (Median)      : {metrics['p50']:.2f} ms")
    print(f"P95 Latency               : {metrics['p95']:.2f} ms")
    print(f"P99 Latency               : {metrics['p99']:.2f} ms")
    print(
        f"Peak Memory Allocated     : {metrics['peak_memory_bytes'] / (1024 * 1024):.2f} MB"
    )
    print(
        f"SLA Validation Status     : {'SUCCESS' if (sla_passed and throughput_passed and failover_passed) else 'FAILED'}"
    )
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_performance_engineering_validation())
