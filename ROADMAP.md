# InferX Product Roadmap

This document outlines the development path and planned milestones for the **InferX** distributed inference engine.

---

## Phase 1: Core Architecture & Data Plane (v0.5.0) - Completed
*   [x] Core Runtime & Event Bus structure.
*   [x] Dynamic batching and priority schedulers.
*   [x] GPU pinned-memory allocations and CUDA streams.
*   [x] Liveness and worker recovery restart managers.

## Phase 2: Gateway & Observability (v0.8.0) - Completed
*   [x] HTTP/gRPC/SSE protocol agnostic gateway.
*   [x] OpenTelemetry context trace spans.
*   [x] Prometheus export registries.
*   [x] Diagnostic profilers.

## Phase 3: Control Plane & Scaling (v1.0.0) - Current Release
*   [x] Raft-inspired consensus leader elections.
*   [x] Gossip membership registries.
*   [x] Kubernetes Helm chart deployment manifests.
*   [x] Queue-depth and GPU custom metrics HPAs.
*   [x] Chaos engineering injection and recovery test suites.

## Phase 4: Enterprise Features & Optimization (v1.2.0) - Planned
*   [ ] Multi-Instance GPU (MIG) slice dynamic auto-splitting.
*   [ ] TensorRT LLM and vLLM native execution adapter bindings.
*   [ ] Zero-Copy shared memory IPC performance enhancements.
*   [ ] Secure token-based payload cryptography.
