# tests/benchmark_deployment.py
import asyncio
import time
import os
import json
from inferx.deployment.config import RuntimeConfigurationManager
from inferx.deployment.controller import DeploymentController


async def run_benchmarks() -> None:
    print("=" * 70)
    print("INFERX DEPLOYMENT & ROLLOUT PERFORMANCE BENCHMARK")
    print("=" * 70)

    # 1. Config Hot Reload Latency
    config_file = "benchmark_config_temp.json"
    with open(config_file, "w") as f:
        json.dump({"param_x": 100}, f)

    mgr = RuntimeConfigurationManager()
    mgr.load_from_file(config_file)

    callback_latencies = []
    def on_update(val: int) -> None:
        pass
    mgr.register_callback("param_x", on_update)

    t_start = time.perf_counter_ns()
    # Trigger 1000 config updates
    for i in range(1000):
        mgr._update_value("param_x", i)
    t_end = time.perf_counter_ns()
    
    avg_reload_latency_us = ((t_end - t_start) / 1000.0) / 1000.0
    print(f"Avg Config Reload Latency  : {avg_reload_latency_us:.4f} us (microseconds)")
    
    if os.path.exists(config_file):
        os.remove(config_file)

    # 2. Rolling Update Rollout Duration
    # Initialize with 50 replicas to stress test replacement logic
    controller = DeploymentController(initial_replicas=50, initial_version="v1.0")
    
    t_start = time.perf_counter()
    await controller.start_rolling_update("v1.1", max_surge=2)
    t_end = time.perf_counter()
    
    rolling_update_duration_ms = (t_end - t_start) * 1000.0
    print(f"Rolling Update Duration    : {rolling_update_duration_ms:.2f} ms (50 Pods, MaxSurge=2)")

    # 3. HPA Autoscaling Response Time
    t_start = time.perf_counter()
    # Trigger scaling of 100 replicas
    await controller.scale_replicas(150)
    t_end = time.perf_counter()
    
    hpa_scaling_duration_ms = (t_end - t_start) * 1000.0
    print(f"HPA Scaling Response Time  : {hpa_scaling_duration_ms:.4f} ms (Scale from 50 to 150 Pods)")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_benchmarks())
