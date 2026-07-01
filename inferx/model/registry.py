# inferx/model/registry.py
"""
InferX Model Registry.

Tracks model metadata configurations, handles version alias resolution,
and manages fallback paths if loading fails.
"""
from typing import Dict, List, Optional, Tuple

from inferx.model.interfaces import ModelMetadata
from inferx.utils.logging import get_logger

logger = get_logger("model.registry")


class ModelRegistry:
    """
    Catalog of registered models and version metadata.
    """
    def __init__(self) -> None:
        # Maps (model_name, version) -> ModelMetadata
        self._models: Dict[Tuple[str, str], ModelMetadata] = {}
        # Maps model_name -> version_alias -> version
        self._aliases: Dict[str, Dict[str, str]] = {}

    def register_model(self, metadata: ModelMetadata) -> None:
        """Registers a model metadata config."""
        key = (metadata.model_name, metadata.version)
        self._models[key] = metadata
        logger.info(f"Registered model: {metadata.model_name}:{metadata.version} (Backend: {metadata.backend_type})", component="model_registry")

    def register_alias(self, model_name: str, alias: str, target_version: str) -> None:
        """Binds a version alias (e.g. 'latest' or 'production') to a concrete version."""
        if model_name not in self._aliases:
            self._aliases[model_name] = {}
        self._aliases[model_name][alias] = target_version
        logger.info(f"Registered alias for {model_name}: {alias} -> {target_version}", component="model_registry")

    def get_model_metadata(self, name: str, version: str) -> ModelMetadata:
        """
        Retrieves the metadata configuration for a model and version.
        
        Raises:
            KeyError: If the model and version combination is not registered.
        """
        resolved_version = self.resolve_version(name, version)
        key = (name, resolved_version)
        if key not in self._models:
            raise KeyError(f"Model {name} version {resolved_version} is not registered.")
        return self._models[key]

    def resolve_version(self, name: str, version_or_alias: str) -> str:
        """
        Maps a version alias to its concrete version.
        
        Falls back to lexicographical sorting if 'latest' alias is not registered explicitly.
        """
        # 1. Check registered alias mapping
        if name in self._aliases and version_or_alias in self._aliases[name]:
            return self._aliases[name][version_or_alias]

        # 2. Handle default 'latest' alias sorting
        if version_or_alias == "latest":
            versions = [version for model_name, version in self._models.keys() if model_name == name]
            if not versions:
                raise KeyError(f"No versions registered for model {name}")
            # Return highest lexicographical version name
            return sorted(versions)[-1]

        # 3. Otherwise treat as concrete version
        return version_or_alias

    def get_fallback(self, name: str, version: str) -> Optional[Tuple[str, str]]:
        """Returns the configured fallback model name and version, if registered."""
        try:
            metadata = self.get_model_metadata(name, version)
            if metadata.fallback_model_name and metadata.fallback_version:
                return metadata.fallback_model_name, metadata.fallback_version
        except KeyError:
            pass
        return None
