# inferx/deployment/config.py
import os
import json
import logging
import base64
from typing import Any, Callable, Dict, List, Optional
from inferx.deployment.interfaces import IConfigManager

logger = logging.getLogger("inferx.deployment.config")


class RuntimeConfigurationManager(IConfigManager):
    """Manages system config maps, loading from environment/files and notifying on hot-reloads."""

    def __init__(self) -> None:
        self._config: Dict[str, Any] = {}
        self._callbacks: Dict[str, List[Callable[[Any], None]]] = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)

    def load_from_env(self) -> None:
        """Hydrates configuration values from OS environment variables."""
        for env_key, val in os.environ.items():
            if env_key.startswith("INFERX_"):
                # Clean prefix for internal configuration matching
                config_key = env_key[7:].lower()
                self._update_value(config_key, val)

    def load_from_file(self, filepath: str) -> None:
        """Loads and validates JSON config, triggering callbacks on value modifications."""
        try:
            if not os.path.exists(filepath):
                logger.warning(f"Config file not found: {filepath}")
                return
                
            with open(filepath, "r") as f:
                data = json.load(f)
                
            for key, new_val in data.items():
                old_val = self._config.get(key)
                if old_val != new_val:
                    self._update_value(key, new_val)
                    logger.info(f"Hot reloaded configuration key '{key}': {old_val} -> {new_val}")
        except Exception as e:
            logger.error(f"Failed to load config file {filepath}: {e}")

    def register_callback(self, key: str, callback: Callable[[Any], None]) -> None:
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)

    def rotate_secret(self, secret_key: str, new_secret_base64: str) -> None:
        """Rotates a secret, decodes the base64 value, and triggers registered callbacks."""
        try:
            decoded_val = base64.b64decode(new_secret_base64).decode("utf-8")
            self._update_value(secret_key, decoded_val)
            logger.info(f"Successfully rotated secret '{secret_key}'")
        except Exception as e:
            logger.error(f"Failed to rotate secret '{secret_key}': {e}")

    def _update_value(self, key: str, val: Any) -> None:
        self._config[key] = val
        # Notify callbacks
        if key in self._callbacks:
            for cb in self._callbacks[key]:
                try:
                    cb(val)
                except Exception as e:
                    logger.error(f"Error in config callback for '{key}': {e}")
