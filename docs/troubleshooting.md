# Troubleshooting Guide - InferX

This guide compiles answers to common errors, latency issues, and recovery procedures.

---

## 1. Cluster & Election Failures

### Issue: Follower nodes trigger campaigns repeatedly, failing to elect a single leader.
*   **Cause:** OS thread scheduling latencies (particularly on Windows) or network delays cause leader heartbeats to arrive late, exceeding follower election timeouts.
*   **Resolution:** Increase candidate random timeouts inside `values.yaml` (e.g. `election_min_ms: 150`, `election_max_ms: 300`) to absorb OS schedule variations.

---

## 2. Queue & Latency Violations

### Issue: SLA latency threshold is exceeded (P95 > 50ms).
*   **Cause:** Ingress traffic volume exceeds processing throughput, causing queues congestion or VRAM model swapping thrashings.
*   **Resolution:** 
    1. Check HPA settings to verify workers scale out successfully.
    2. Adjust `max_batch_size` in the dynamic batcher config to optimize hardware parallelism.

---

## 3. Worker Crash Recovery

### Issue: Worker node terminates or crashes during tensor execution.
*   **Observation:** The supervisor intercepts heartbeat failures, marks node status as `DOWN`, and initiates recovery restart protocols.
*   **Resolution:** Verify error logs to confirm OOM thresholds weren't breached, and verify CUDA stream availability on the target host.
