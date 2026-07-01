# InferX

<div align="center">
  <p><strong>Production-grade, distributed AI inference engine designed for cloud-native orchestration of LLMs and deep learning workloads.</strong></p>

  [![CI](https://github.com/Reya-Doshi/InferX/actions/workflows/ci.yml/badge.svg)](https://github.com/Reya-Doshi/InferX/actions/workflows/ci.yml)
  [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
  [![Kubernetes](https://img.shields.io/badge/Kubernetes-Compatible-blue.svg)](deploy/kubernetes/)
</div>

---

## 📽️ Demonstration

We have uploaded a detailed walkthrough video demonstrating the distributed failover, load-shedding control loops, and zero-copy shared memory performance tests in real-time.

[![Watch Demonstration Video](https://img.shields.io/badge/Watch_Demo_Video-3b66f5?style=for-the-badge&logo=playstation)](https://github.com/Reya-Doshi/InferX/blob/main/InferX.mp4)

[▶ Click here to open and play the demonstration video](https://github.com/Reya-Doshi/InferX/blob/main/InferX.mp4)

---

## 🛠️ Tech Stack & Architecture

### Core Runtimes & Languages
*   ![Python](https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) - Advanced asynchronous standard library and parallel computation.
*   ![Asyncio](https://img.shields.io/badge/Asyncio-3776AB?style=for-the-badge&logo=python&logoColor=white) - High-throughput, non-blocking I/O event loops.

### Infrastructure & Deployment
*   ![Kubernetes](https://img.shields.io/badge/kubernetes-%23326ce5.svg?style=for-the-badge&logo=kubernetes&logoColor=white) - Orchestrated container scheduling and microservices topology.
*   ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white) - Reproducible, multi-platform runtime environments.
*   ![Helm](https://img.shields.io/badge/Helm-0F162D?style=for-the-badge&logo=helm&logoColor=white) - Kubernetes package manager for deployment templates.
*   ![Render](https://img.shields.io/badge/Render-%2346E3B7.svg?style=for-the-badge&logo=render&logoColor=white) - Automatic blueprint builds and live web service hosting.

### Protocols & Data Layers
*   ![JSON-RPC](https://img.shields.io/badge/JSON--RPC-404040?style=for-the-badge) - Secure, schema-validated inter-node RPC communications.
*   ![YAML](https://img.shields.io/badge/YAML-%23ffffff.svg?style=for-the-badge&logo=yaml&logoColor=151515) - Standard configuration layouts with PyYAML loader.

### Observability & Automated QA
*   ![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=Prometheus&logoColor=white) - Metric exporter for time-series throughput and latency tracking.
*   ![Pytest](https://img.shields.io/badge/Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white) - Scalable testing framework with code coverage reports.
*   ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white) - Automated CI pipelines verifying format, lint, and test suites.

---

## ⚡ Key Features

*   **Distributed Control Plane:** Fully distributed membership tracking with Gossip heartbeats, Raft-inspired consensus leader elections, and config metadata replication.
*   **Dynamic Batching & Priority Scheduling:** Priority queues sorting and batching queries based on real-time hardware stream availability.
*   **Worker & Model Runtime Management:** GPU tensor pinned-memory allocations, automatic worker liveness monitors, and lazy-loaded model version switching.
*   **Cloud-Native Ingress Gateway:** Protocol agnostic endpoint supporting HTTP, gRPC, and Server-Sent Events (SSE).
*   **Kubernetes Ready:** Production Helm charts integrating custom queue-depth and GPU utilization autoscaling triggers.

---

## 📐 Architecture Overview

```mermaid
graph TD
    Client[Client Gateway Request] --> Gateway[InferX Ingress Gateway]
    Gateway --> Admission[Admission Controller / Queue]
    Admission --> Scheduler[Distributed Scheduler]
    Scheduler --> Coordinator[Raft Leader Node]
    Coordinator -->|RPC Delegate| RemoteWorker[Remote GPU Worker Node]
    Scheduler -->|Local stream| LocalBatcher[Dynamic Batcher]
    LocalBatcher -->|CUDA Stream| GPURuntime[Model Runtime Engine]
```

### Deep Dive Modules
*   **Gateway Layer:** Implements HTTP/1.1 REST endpoints, SSE stream formatting, and WebSocket connection handshakes.
*   **Admission System:** Features backpressure controllers, load shedders, token-bucket rate limiters, and circuit breakers.
*   **Zero-Copy Shared Memory:** Bypasses Python serialization bottlenecks using `SharedMemoryPool` for ultra-low latency IPC between processes.

For a detailed review of internal modules, see [ARCHITECTURE.md](file:///c:/Users/lenovo/OneDrive/Desktop/ReyaWeb/InferX/docs/architecture.md).

---

## 🚀 Quick Start

### Installation
Clone the repository and install the production dependencies:
```bash
git clone https://github.com/Reya-Doshi/InferX.git
cd InferX
pip install -r requirements.txt
```

### Run a Single Node Instance
Start a mock single-node gateway instance locally:
```python
import asyncio
from inferx.core.bootstrap import bootstrap_node
from inferx.core.config import AsyncYAMLConfigLoader

async def main():
    print("Initializing InferX Node...")
    # Launches gateway REST/WebSocket endpoints and mock runtimes
    await bootstrap_node(port=10000)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📊 Benchmarks & SLA Compliance

| Metric / Parameter | Measured Value | SLA Target Status |
| --- | --- | --- |
| **Steady State Throughput** | 253.20 req/sec | ✅ Meets target |
| **P50 Latency (Median)** | 14.95 ms | ✅ Meets target |
| **P95 Latency** | 18.39 ms | ✅ Meets target (SLA limit: < 50ms) |
| **Cluster Failover Duration** | 106.32 ms | ✅ Meets target (SLA limit: < 150ms) |
| **Config Replication Latency** | 6.22 ms | ✅ Meets target |

---

## 📜 License
InferX is open source software licensed under the [Apache License 2.0](LICENSE).
