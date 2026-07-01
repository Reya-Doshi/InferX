# inferx/interfaces/core.py
"""
InferX Core Runtime Abstract Interfaces.

Defines the interface boundaries (Abstract Base Classes) for the runtime core.
These contracts guarantee loose coupling and enable mock injection during testing.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Type, TypeVar

T = TypeVar("T")


class IConfigLoader(ABC):
    """Responsible for loading, validating, and reloading system configurations asynchronously."""

    @abstractmethod
    async def load(self) -> Any:
        """Parses and validates configurations from a static path or environment."""
        pass

    @abstractmethod
    async def reload(self) -> Any:
        """Triggered on file updates to update running system parameters."""
        pass


class IHealthManager(ABC):
    """Monitors system-wide hardware telemetry and process health markers."""

    @abstractmethod
    def register_provider(
        self, name: str, check_fn: Callable[[], Coroutine[Any, Any, bool]]
    ) -> None:
        """Registers a named service callback to include in the periodic health loop."""
        pass

    @abstractmethod
    async def evaluate_health(self) -> Any:
        """Runs registered check functions and compiles system metrics status."""
        pass

    @abstractmethod
    def get_last_status(self) -> Any:
        """Returns the cached results of the most recent health check execution."""
        pass


class IRuntimeSupervisor(ABC):
    """Monitors asynchronous execution workers and orchestrates process recovery."""

    @abstractmethod
    async def start(self) -> None:
        """Starts the main worker monitor loops and verification tasks."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stops watchdog tracking and shuts down worker resources."""
        pass

    @abstractmethod
    async def recover_worker(self, worker_id: str) -> None:
        """Kills, recycles, and restarts an unhealthy worker process."""
        pass


class IRuntimeLifecycle(ABC):
    """Handles OS signal propagation, lifecycle states, and graceful teardown."""

    @abstractmethod
    async def run(self) -> None:
        """Starts the bootstrap sequence and enters the main execution loop."""
        pass

    @abstractmethod
    async def shutdown(self, signal_num: int) -> None:
        """Executes the step-by-step graceful teardown of active channels and files."""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """Checks if the runtime is in active execution state."""
        pass


class IDIContainer(ABC):
    """Dependency injection container interface supporting providers."""

    @abstractmethod
    def register(self, interface: Type[T], provider: Any) -> None:
        """Binds a provider to an interface type."""
        pass

    @abstractmethod
    def resolve(self, interface: Type[T]) -> T:
        """Resolves the instance bound to the requested interface."""
        pass
