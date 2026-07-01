# Changelog - InferX

All notable changes to the **InferX** project will be documented in this file.

---

## [1.0.0] - 2026-06-30

### Added
*   **Distributed Control Plane:** Implemented Gossip-based node discovery loops, Raft consensus leader elections, and configuration metadata replication.
*   **Observability & Telemetry:** OpenTelemetry context tracing integration, Prometheus exporter metrics registry, and real-time profiler timelines.
*   **Gateway Ingress:** Protocol agnostic HTTP/gRPC/SSE endpoints dispatcher with auth token verification pipelines.
*   **Dynamic Batching & Scheduling:** Priority queuing and CUDA streams isolation managers.
*   **Deployment platform:** Multi-stage Dockerfiles, Helm charts, and queue-depth HPA templates.
*   **Verification Suites:** Unit tests, load generators, fault injectors, and performance benchmarks.
