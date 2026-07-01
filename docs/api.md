# API Reference - InferX

This guide documents the core API classes and interfaces of InferX.

---

## 1. Scheduling Interfaces

### `ScheduledRequest`
Data structure representing a request submitted to the admission control scheduler.
```python
class ScheduledRequest(BaseModel):
    request_id: str
    tenant_id: str
    priority: int
    max_latency_ms: float
    payload: Any
```

### `ISchedulingPolicy`
Structural boundary defining scheduling sorting hooks.
```python
class ISchedulingPolicy(ABC):
    @abstractmethod
    def push(self, request: ScheduledRequest) -> None: pass
    @abstractmethod
    def pop(self) -> Optional[ScheduledRequest]: pass
    @abstractmethod
    def size(self) -> int: pass
```

---

## 2. Distributed Control Plane Interfaces

### `IDeploymentController`
Coordinates containers rolling updates and canary rollbacks.
```python
class IDeploymentController(ABC):
    @abstractmethod
    async def start_rolling_update(self, target_version: str) -> bool: pass
    @abstractmethod
    async def start_canary_deployment(self, target_version: str) -> bool: pass
```

### `IConfigManager`
Handles environment configuration and hot-reloads.
```python
class IConfigManager(ABC):
    @abstractmethod
    def load_from_file(self, filepath: str) -> None: pass
    @abstractmethod
    def register_callback(self, key: str, callback: Callable) -> None: pass
```
