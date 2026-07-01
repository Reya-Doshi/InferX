# Frequently Asked Questions - InferX

This document compiles answers to the most common questions regarding InferX's architecture, scaling rules, and configuration settings.

---

### 1. General Questions

#### Q: What makes InferX different from simple API gateways?
**A:** InferX is a full-featured distributed execution engine. It combines protocol-agnostic gateways (HTTP/gRPC/SSE) with scheduler admission control, dynamic batch queue sorting, CUDA execution streams isolation, and Raft consensus cluster replication.

#### Q: Does InferX require a GPU to run local tests?
**A:** No. InferX includes software fallback handlers that simulate CUDA execution streams and VRAM metrics. You can run all test suites and benchmarks on any standard CPU.

---

### 2. Scaling & Resource Allocation

#### Q: How does HPA scale workers based on queue depth?
**A:** The Helm chart configures custom metrics mappings via the Prometheus Adapter. When the admission controller's queue depth exceeds the target limit (e.g. 50 requests), Kubernetes automatically spins up new worker pods.

#### Q: How is MIG compatibility achieved?
**A:** InferX limits resource limits mapping under Kubernetes StatefulSets. By specifying exact GPU limits (e.g., `nvidia.com/gpu: 1`), workers bind safely to distinct physical MIG slices.

---

### 3. Troubleshooting

#### Q: What causes the Gossip failure detector to mark nodes as DOWN?
**A:** If a node's event loop blocks or fails to reply to ping RPC requests within `failure_timeout` (2.0s), the Gossip manager changes its status in the node registry to `DOWN`.
