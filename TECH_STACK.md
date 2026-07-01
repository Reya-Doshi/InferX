# InferX Technology Stack Decisions (TECH_STACK.md)

This document is the Single Source of Truth (SSoT) for every technology choice, architectural tradeoff, and system constraint within the **InferX** high-throughput inference runtime.

---

## 1. Project Philosophy

InferX is engineered from the ground up under a **production-first, platform-as-a-service (PaaS)** philosophy. It is designed to serve as the critical infrastructure layer between deep learning models and client applications under high concurrent load. 

The project is governed by the following core design principles:
*   **High Performance**: Minimal overhead in request routing, serialization, queue operations, and inter-process data copying.
*   **Reliability & High Availability**: Failures in hardware (such as GPU memory exhaustion, PCIe hangs, or worker exceptions) must be isolated. The engine must survive worker crashes and maintain a high level of availability.
*   **Clean Architecture**: Separation of concerns between the API transport (Gateway), rate limiting (Admission Control), execution logic (Scheduler & Batcher), and deep learning backends (Workers & Models).
*   **Dependency Injection & Interface-First Design**: Components communicate strictly through typed interfaces. This allows engineers to swap schedulers, batchers, or model engines without rewriting core coordination code.
*   **Extensibility**: Developers can load and run pre/post-processing plugins within secure sandboxes without modifying the host core.
*   **Observability**: Every request is traceable across processes. System state metrics (queues, batch efficiency, GPU metrics) are exposed via standard interfaces.

---

## 2. Programming Language

The programming language selection dictates the system's performance ceiling, concurrency efficiency, and developer velocity.

### Comparison Table

| Language | CPU Execution Overhead | Concurrency & Threading Model | Ecosystem for ML Inference | Memory Management & Overhead |
| :--- | :--- | :--- | :--- | :--- |
| **Python** | High (Interpreter, GIL overhead) | Asyncio (Single-thread cooperative), Multiprocessing | Excellent (PyTorch, TensorRT-LLM, HuggingFace) | Automated GC (garbage collection latency) |
| **Go** | Low (Compiled native) | Goroutines (CSP channels, cheap concurrency) | Poor (Fails to easily bind CUDA / Python weights) | Garbage Collector (introduces latency jitter) |
| **Rust** | Zero (Native compilation) | Async-std/Tokio (Zero-cost abstractions, multi-thread) | Moderate (growing candle/tch ecosystems) | Manual/Static (No GC, zero runtime allocation jitter) |
| **C++** | Zero (Native compilation) | Native pthreads / custom schedulers | Excellent (TensorRT native C++, ONNX Runtime) | Manual (prone to memory leaks if unmanaged) |

### Language Selection: Python 3.13 (Hybrid)
*   **Decision:** Python 3.13 is selected as the primary orchestration language, wrapping highly optimized C++ inference backends (like TensorRT-LLM and vLLM).
*   **Trade-off Analysis:** While Rust or C++ offer superior CPU efficiency and predictable latency, the vast majority of machine learning models, weights, and tensor operations are native to the PyTorch/HuggingFace Python ecosystem. Building the orchestrator in Go or Rust introduces significant integration friction (such as building complex FFI bindings for PyTorch or ONNX) which reduces developer velocity. By using Python 3.13's advanced concurrency primitives, offloading heavy compute tasks to optimized C++ engines via FFI, and isolating GPU execution in separate processes, we achieve high throughput with excellent developer velocity.

---

## 3. Python Version

### Decision: Python 3.13 (CPython)
Python 3.13 introduces significant architectural changes that are crucial for high-concurrency systems.

### Trade-off Analysis:
*   **PEP 703 (Free-Threading / GIL-Free Roadmap):** Python 3.13 includes experimental support for running without the Global Interpreter Lock (GIL). This allows multi-threaded CPU-bound work (like pre-processing and scheduling) to run in parallel on a single process, laying the foundation for future vertical scaling.
*   **Performance Enhancements:** Introduces a Tier-2 Just-In-Time (JIT) compiler, leading to 5-15% improvements in hot-loop execution speeds.
*   **Advanced Typing (PEP 695 / PEP 702):** Enhances generic type definitions and adds `@deprecated` metadata, enabling cleaner interface verification at lint time.
*   **Asyncio Improvements:** Includes lower-overhead task creation and improved scheduling latency, which is critical for handling high request rates.

---

## 4. Dependency Management

The dependency manager must support reproducible builds, quick lock resolutions, and isolated environment tracking.

### Comparison Table

| Tool | Resolution Speed | Lock File Support | Virtualenv Integration | Build Reproducibility |
| :--- | :--- | :--- | :--- | :--- |
| **uv** | Ultra-Fast (Written in Rust) | Yes (`uv.lock`) | Automatic / Fast | High |
| **Poetry** | Slow | Yes (`poetry.lock`) | Automatic | High |
| **pip-tools** | Moderate | Yes (`requirements.txt`) | Manual | Moderate |
| **PDM** | Fast | Yes (`pdm.lock`) | Automatic | High |

### Decision: `uv`
*   **Trade-off Analysis:** `uv` is a fast Python package installer and resolver written in Rust. It replaces `pip`, `pip-tools`, and `virtualenv`. Its lock resolution is up to 10-100x faster than Poetry or pip-tools. By adopting `uv`, we reduce CI/CD build times, guarantee reproducible builds via `uv.lock`, and simplify developer onboarding.

---

## 5. Project Structure

We adopt a structured, domain-isolated package layout to prevent circular dependencies and enforce modular boundaries.

```
inferx/
├── api/                      # External API specifications (gRPC proto, OpenAPI schemas)
├── config/                   # Deployment and test configurations (YAML, TOML templates)
├── docs/                     # Architecture Decision Records (ADRs) and user guides
├── src/                      # Source root
│   └── inferx/
│       ├── __init__.py
│       ├── main.py            # Bootstrap entry point
│       ├── control_plane/     # Model life-cycle, cluster admin, health systems
│       ├── data_plane/        # Request router, admission control, scheduling, batching
│       ├── common/            # Error types, constants, context managers
│       ├── telemetry/         # OpenTelemetry, Prometheus metrics, structured log configurations
│       └── plugins/           # WebAssembly loaders and dynamic pre/post-processing handlers
├── tests/                    # Integration, load, and unit tests
├── benchmarks/               # Performance verification models and scripts
└── pyproject.toml            # Project manifest managed by uv
```

*   **Rationale:** Decoupling the `control_plane` from the `data_plane` ensures that latency-sensitive request processing logic is isolated from heavy administrative tasks (like loading model weights).

---

## 6. API Framework

The HTTP framework must support high-speed parsing, automatic validation, OpenAPI generation, and HTTP/2.

### Comparison Table

| Framework | Request Throughput (Req/Sec) | Latency Overhead | Native OpenAPI | Async Support |
| :--- | :--- | :--- | :--- | :--- |
| **FastAPI** | Moderate (~15k) | Medium (Pydantic overhead) | Yes | Native |
| **Starlette** | High (~25k) | Low | No | Native |
| **Litestar** | High (~30k) | Low (Optimized validation) | Yes | Native |
| **aiohttp** | High (~25k) | Low | No | Native |

### Decision: `Litestar`
*   **Trade-off Analysis:** While FastAPI is the industry standard for API development, it introduces serialization overhead due to older Pydantic integrations and routing designs. `Litestar` is a highly optimized, asynchronous ASGI framework. It is up to 2x faster than FastAPI while offering native OpenAPI schema generation, structured middleware chains, and modern dependency injection.

---

## 7. Async Runtime

### Decision: `asyncio` with `uvloop`
We replace Python's default event loop with `uvloop`, which is a fast, drop-in replacement implemented in Cython on top of the `libuv` engine (the same engine that powers Node.js).

### Trade-off Analysis:
*   **Performance:** `uvloop` brings asyncio's network performance close to that of Go and Node.js, doubling typical connection-handling capacity.
*   **Compatibility:** It integrates with standard library `asyncio` code, avoiding the ecosystem fragmentation of alternative async runtimes like `Trio`.

---

## 8. Validation Engine

The serialization engine must parse and validate high-frequency request payloads with minimal CPU overhead.

### Comparison Table

| Engine | Validation Speed | Serialization Overhead | Type Hint Support | Ecosystem Integration |
| :--- | :--- | :--- | :--- | :--- |
| **Pydantic v2** | Fast (C++ core) | Medium | Excellent | High |
| **msgspec** | Ultra-Fast (C core) | Low | Excellent (Structs) | Moderate |
| **attrs** | Moderate | Low | Good | Moderate |

### Decision: `msgspec`
*   **Trade-off Analysis:** In the data plane, input payload validation happens on the hot path. `msgspec` is up to 5-10x faster than Pydantic v2 and 2-4x faster than standard library `json` parsing. It parses JSON directly into structured type-annotated representations, minimizing latency. We use `msgspec` for high-frequency data plane APIs and reserve `Pydantic v2` for the slower control plane configuration setups.

---

## 9. Configuration Management

### Decision: YAML + `Pydantic Settings` (Control Plane)
*   **Strategy:** Configurations are written in YAML (supporting nested properties) and parsed at startup using `Pydantic Settings`.
*   **Validation:** Structs validate value bounds (e.g., checks if `max_batch_size > 0`).
*   **Hot Reload:** The control plane registers file watchers (using `watchfiles`) on the configuration path. When updates are detected, it updates parameters dynamically without restarting the server.
*   **Secrets:** Sensitive variables (API keys, DB URIs) are loaded from environment variables and merged into the configuration settings at runtime.

---

## 10. Dependency Injection

### Decision: Manual Providers (Composition Root)
*   **Strategy:** Dependency resolution is managed via a manual Dependency Injection container (`bootstrap.py`).
*   **Rationale:** While framework libraries like `dependency-injector` provide advanced configuration options, they add dependency footprint, make debugging stack traces difficult, and slow down execution. Manual injection is transparent, has zero runtime overhead, and simplifies unit-testing mocks.

---

## 11. Logging

### Decision: `structlog`
*   **Trade-off Analysis:** Python's standard `logging` module is synchronous and formats logs as plain text by default, which is difficult to index in log aggregators (e.g., ELK, Loki). `structlog` is an asynchronous logging engine designed for structured logging. It outputs logs as clean JSON lines, automatically injects correlation IDs, and formats traceback arrays.

---

## 12. Tracing

### Decision: `OpenTelemetry SDK` (OTel)
*   **Strategy:** We use the OpenTelemetry Python SDK. The gateway extracts incoming W3C headers (`traceparent`) and starts a root trace span. Spans are propagated across async worker boundaries via context variables and exported to an OTel Collector daemon (running in sidecar mode) using non-blocking gRPC protocols.

---

## 13. Metrics

### Decision: `Prometheus Client SDK`
*   **Strategy:** We instrument components using the official Prometheus Python SDK. Custom collectors export metrics via a `/metrics` HTTP endpoint. Latency histograms use exponential bucketing configured to resolve latencies between 1ms and 30 seconds. To prevent high-cardinality issues, metric labels are restricted to static fields (`model_id`, `priority`, `tenant_id`).

---

## 14. Dashboards

### Decision: `Grafana`
*   **Strategy:** Standardized dashboards are version-controlled as JSON files in the `/docs/grafana` folder. Key panels monitor system health indicators: queue delays, active batch sizes, request success/error rates, GPU utilization, and memory margins.

---

## 15. Worker Pool Concurrency

In Python, thread-based parallel execution is limited by the GIL. To achieve concurrent GPU execution, we need an alternative worker architecture.

### Comparison Table

| Architecture | Memory Footprint | GPU Context Safety | IPC Overhead | Resilience to Crashes |
| :--- | :--- | :--- | :--- | :--- |
| **ThreadPoolExecutor** | Low | Low (GIL blocks execution) | None (Shared memory) | Low (Crash kills process) |
| **ProcessPoolExecutor** | High | High (Separate CUDA context)| High (Pickle serialization)| Moderate |
| **Custom Multiprocessing** | High | High (Isolated processes) | Low (Zero-copy SHM) | High (Manual supervisor) |

### Decision: Custom Multiprocessing with Process Recycling
*   **Trade-off Analysis:** Standard `ProcessPoolExecutor` uses pickling to pass payloads, which is slow for large tensors. By building a custom `multiprocessing` worker pool, we can bind individual processes to specific GPUs, manage shared memory structures directly, and implement custom worker recovery logic.

---

## 16. Inter-Process Communication (IPC)

### Decision: POSIX Shared Memory (`shm_open`) + Unix Domain Sockets (UDS)
*   **Trade-off Analysis:** passing large tensor data over pipes, TCP sockets, or Redis streams introduces significant memory copying overhead. We use memory-mapped POSIX shared memory (`/dev/shm`) to pass tensor payloads. Unix Domain Sockets (UDS) are used as a control channel to pass indices and status flags, ensuring zero-copy data paths for inference payloads.

---

## 17. Model Runtime Abstraction

To support multiple backends (PyTorch, ONNX, TensorRT-LLM) without modifying core orchestration logic, we define a unified engine adapter interface:

```python
class IInferenceEngine(abc.ABC):
    @abc.abstractmethod
    async def load_model(self, model_path: str, args: dict[str, str]) -> None:
        """Initialize CUDA context and load weights into memory."""
        pass

    @abc.abstractmethod
    async def execute_batch(self, batch: ModelBatch) -> BatchResult:
        """Run the forward pass on the input batch."""
        pass
```

---

## 18. Plugin Runtime

Plugins run arbitrary user code. They must be isolated to prevent them from crashing the host process or accessing unauthorized resources.

### Comparison Table

| Environment | Sandbox Isolation | Startup Latency | Performance | Developer Experience |
| :--- | :--- | :--- | :--- | :--- |
| **Python Imports** | None | Low | High | Excellent |
| **WebAssembly** | High (Virtual Machine) | Low | High (~1.2x native) | Moderate |
| **Docker Sidecar** | High (Namespaces) | High | Moderate (gRPC overhead) | Good |

### Decision: WebAssembly (Wasm) Sandbox using `wasmtime`
*   **Trade-off Analysis:** While Python plugins are easy to write, they run in the same process space and can crash the host runtime. WebAssembly runs inside a sandboxed virtual machine, providing strong isolation with near-native execution performance.

---

## 19. State Persistence

### Decision: None (Stateless Data Plane)
*   **Trade-off Analysis:** An inference engine should operate as a stateless network endpoint. Keeping database lookups (like SQLite or Postgres) off the hot data path eliminates database latency. Model profiles and credentials are loaded dynamically from environment files or Kubernetes config maps.

---

## 20. Event Bus

### Decision: In-Memory Async Ring Buffer
*   **Trade-off Analysis:** External event systems like Kafka or NATS add deployment complexity and network latency. Because all telemetry and scheduling events occur within a single node, we use a lock-free, in-memory async ring buffer to publish metrics, logging, and trace events without blocking request execution.

---

## 21. Testing Strategy

*   **Unit Tests:** Built using `pytest` and `pytest-asyncio` with mocked engine and GPU drivers.
*   **Integration Tests:** Run on GPU-enabled runners, executing end-to-end runs with small, lightweight model weights.
*   **Chaos Testing:** Automatically kills worker processes during test runs to verify that the supervisor recovers gracefully and re-routes pending batches.

---

## 22. Benchmarking Strategy

*   **Locust:** Simulates multi-tenant user behavior, tracing TTFT (Time-To-First-Token) and latency distributions.
*   **wrk2:** Measures peak throughput and latency percentiles (p50, p99) under sustained high concurrency.

---

## 23. Documentation Workflow

*   **MkDocs Material:** Compiles markdown documents into a searchable static site.
*   **Mermaid:** Used for diagrams within documentation files.
*   **Architecture Decision Records (ADRs):** Major design changes must be documented as ADRs in `/docs/adr` before implementation.

---

## 24. Containerization

*   **Docker:** Uses multi-stage builds to keep final production images clean.
*   **Base Image:** Built on `nvidia/cuda:12.4.1-runtime-ubuntu22.04` to ensure CUDA support.
*   **Security:** Containers run as non-root users (`USER 10001`) with restricted read-only filesystems.

---

## 25. Kubernetes Orchestration

*   **GPU Scheduling:** Integrated via the NVIDIA Kubernetes GPU Operator, using multi-instance GPU (MIG) slices for tenant isolation.
*   **Autoscaling:** A Horizontal Pod Autoscaler (HPA) scales replica pools based on custom Prometheus metrics (specifically **Queue Dwell Duration** and **Active Batch Ratio**).

---

## 26. CI/CD Pipeline

*   **Static Analysis:** Enforces code quality checks on every commit:
    *   `Ruff` for linting and formatting.
    *   `MyPy` for strict static type checking.
    *   `Bandit` for security scanning.
*   **Automated Release:** Generates semantic version tags and pushes verified Docker images to container registries.

---

## 27. Security Protocol

*   **mTLS:** Secures internal cluster communication (e.g., scraping `/metrics` or administrative API calls).
*   **Token Validation:** Gateway validates client JWTs using RS256 signatures with public keys fetched from a secure JWKS endpoint.
*   **Network Isolation:** Kubernetes NetworkPolicies restrict ingress traffic to designated Gateway pods.

---

## 28. Coding Standards

*   **Type Hinting:** Mandatory on all function signatures. Type parameters are verified by `MyPy` in strict mode.
*   **SOLID & OOP:** Interfaces are defined as Abstract Base Classes. Dependency injection is used to compose components at startup.
*   **Structured Errors:** Custom exceptions inherit from `InferXError` and include diagnostic error codes, severity levels, and retry policies.

---

## 29. Third-Party Library Selection

| Package | Purpose | Reason for Selection | Alternative Considered |
| :--- | :--- | :--- | :--- |
| **uvloop** | Event Loop optimization | 2x request processing throughput | Native asyncio loop |
| **msgspec** | JSON/Struct validation | 5x faster serialization than Pydantic | Pydantic v2 |
| **wasmtime** | WASM Plugin sandboxing | Fast execution, robust isolation | Wasmer / Python eval |
| **structlog** | JSON structured logging | Asynchronous, zero-allocation logging | Python native `logging` |
| **watchfiles** | Config hot-reloading | Low-overhead file-system monitoring | watchdog |
| **pyyaml** | Configuration reading | Standard format, supports comments | json |

---

## 30. Final Technology Stack Summary

| Architecture Layer | Technology Selection | Primary Rationale |
| :--- | :--- | :--- |
| **Runtime Core** | Python 3.13 + CPython JIT | Integrates with ML framework ecosystems |
| **Event Loop** | uvloop (libuv) | Optimizes network throughput and connection handling |
| **API Transport** | Litestar (ASGI) | Native OpenAPI schema support with low-overhead routing |
| **Serialization** | msgspec | Low-overhead JSON validation on the hot path |
| **Concurrency** | Custom Multiprocessing | Bypasses the GIL and isolates CUDA contexts |
| **IPC** | POSIX Shared Memory (`shm_open`) | Zero-copy tensor data passing between processes |
| **Sandboxing** | Wasmtime VM | Secure, isolated execution of user-supplied plugins |
| **Observability** | OpenTelemetry + Prometheus | Vendor-neutral, standardized instrumentation |
| **Orchestration** | Kubernetes + Helm | Automates scheduling, load-balancing, and scaling of GPU nodes |
