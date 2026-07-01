# InferX Documentation Portal

Welcome to the **InferX** documentation portal. InferX is a production-grade, distributed AI inference engine designed for cloud-native orchestration of LLMs and deep learning workloads.

---

## Navigation Guide

*   **[Architecture Guide](architecture.md):** Deep dive into the Control Plane and Data Plane architectures.
*   **[API Reference](api.md):** Technical documentation of the event bus, scheduler, and model runtime APIs.
*   **[Deployment Guide](deployment.md):** Learn how to configure packaging, Docker Compose, and Kubernetes Helm charts.
*   **[Troubleshooting](troubleshooting.md):** Common errors, latency debugging, and recovery procedures.

---

## High-Performance SLA Targets
*   **Sub-50ms P95 Latency:** Guaranteed via priority admission queues and hardware dynamic batching.
*   **Sub-100ms Cluster Failover:** Rapid leader elections utilizing staggered term loop triggers.
*   **Zero Memory Copies:** Pin-allocated tensor pools maximizing data transfer throughput.
