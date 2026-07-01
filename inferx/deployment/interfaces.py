# inferx/deployment/interfaces.py
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional


class IConfigManager(ABC):
    """Interface for dynamic runtime configuration loading, validation and hot-reloads."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Retrieves a configuration value by key."""
        pass

    @abstractmethod
    def load_from_env(self) -> None:
        """Loads and updates configurations from environment variables."""
        pass

    @abstractmethod
    def load_from_file(self, filepath: str) -> None:
        """Loads and validates configuration values from a JSON/YAML file path."""
        pass

    @abstractmethod
    def register_callback(self, key: str, callback: Callable[[Any], None]) -> None:
        """Registers a listener callback triggered on value updates of the specified key."""
        pass

    @abstractmethod
    def rotate_secret(self, secret_key: str, new_secret_base64: str) -> None:
        """Dynamically rotates secrets or auth keys at runtime."""
        pass


class IDeploymentController(ABC):
    """Interface for orchestrating pod deployments, rolling updates, and canary rollbacks."""

    @abstractmethod
    async def start_rolling_update(
        self,
        target_version: str,
        max_surge: int = 1,
        max_unavailable: int = 0
    ) -> bool:
        """Simulates an incremental rolling update replacement rollout, verifying health check status."""
        pass

    @abstractmethod
    async def start_canary_deployment(
        self,
        target_version: str,
        canary_weight_percent: int = 10,
        rollback_error_threshold: float = 0.05
    ) -> bool:
        """Launches a Canary rollout, routing fractional traffic and evaluating errors for rollback."""
        pass

    @abstractmethod
    async def scale_replicas(self, target_replicas: int) -> None:
        """Triggers horizontal scaling scaling target pods count."""
        pass

    @abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """Returns current instances list, versions, and controller states."""
        pass
