# inferx/core/config.py
"""
InferX Configuration Manager.

Implements immutable, validated configurations using Pydantic v2.
Supports async file reading, environment variable overrides, and dynamic reload constraints.
"""
import asyncio
from typing import Any, Optional
import os
import yaml
from pydantic import BaseModel, Field, model_validator, ValidationError as PydanticValidationError

from inferx.errors.taxonomy import ConfigurationError
from inferx.interfaces.core import IConfigLoader


class GatewayConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1024, le=65535)
    timeout_ms: int = Field(default=30000, gt=0)
    
    model_config = {"frozen": True}


class AdmissionConfig(BaseModel):
    max_concurrency: int = Field(default=1000, gt=0)
    rate_limit_capacity: int = Field(default=100, ge=0)
    rate_limit_refill_rate: float = Field(default=20.0, ge=0.0)
    vram_high_watermark: float = Field(default=0.90, gt=0.0, le=1.0)
    
    model_config = {"frozen": True}


class SchedulerConfig(BaseModel):
    type: str = "priority"
    max_priority_levels: int = Field(default=5, gt=0)
    priority_aging_threshold_ms: int = Field(default=5000, ge=0)
    
    model_config = {"frozen": True}


class BatcherConfig(BaseModel):
    max_batch_size: int = Field(default=32, gt=0)
    max_queue_delay_ms: int = Field(default=10, gt=0)
    
    model_config = {"frozen": True}


class WorkerConfig(BaseModel):
    gpus: list[int] = Field(default_factory=lambda: [0])
    heartbeat_timeout_ms: int = Field(default=2000, gt=0)
    shm_size_bytes: int = Field(default=1073741824, gt=0)
    
    model_config = {"frozen": True}

    @model_validator(mode="after")
    def validate_gpu_entries(self) -> "WorkerConfig":
        if not self.gpus:
            raise ValueError("Worker GPU list cannot be empty.")
        if any(gpu < 0 for gpu in self.gpus):
            raise ValueError("GPU indices must be non-negative integers.")
        return self


class RuntimeConfig(BaseModel):
    log_level: str = "INFO"
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    admission: AdmissionConfig = Field(default_factory=AdmissionConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    batcher: BatcherConfig = Field(default_factory=BatcherConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    
    model_config = {"frozen": True}


class AsyncYAMLConfigLoader(IConfigLoader):
    """
    Asynchronous configuration loader.
    
    Loads configuration settings from YAML on disk, performs environment overrides,
    and returns an immutable RuntimeConfig instance.
    """
    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self._active_config: Optional[RuntimeConfig] = None
        self._lock = asyncio.Lock()

    async def load(self) -> RuntimeConfig:
        """Reads configuration file asynchronously and returns a validated config object."""
        async with self._lock:
            if not os.path.exists(self.config_path):
                raise ConfigurationError(
                    message=f"Configuration file not found: {self.config_path}",
                    cause="Path validation failed."
                )

            # Read configuration offloading I/O blocking to a thread executor
            try:
                def read_file() -> str:
                    with open(self.config_path, "r") as f:
                        return f.read()

                content = await asyncio.to_thread(read_file)
                raw_data = yaml.safe_load(content) or {}
            except Exception as e:
                raise ConfigurationError(
                    message=f"Failed to read or parse configuration file.",
                    cause=str(e)
                )

            # Map raw fields and perform environment overrides
            self._apply_env_overrides(raw_data)

            # Parse with Pydantic model
            try:
                config = RuntimeConfig(**raw_data)
            except PydanticValidationError as e:
                raise ConfigurationError(
                    message="Configuration parameters failed validation check.",
                    cause=str(e)
                )

            self._active_config = config
            return config

    async def reload(self) -> RuntimeConfig:
        """Asynchronously reloads configuration options."""
        return await self.load()

    def get_active(self) -> RuntimeConfig:
        """Returns the active loaded config object."""
        if self._active_config is None:
            raise ConfigurationError("No configuration has been loaded. Call load() first.")
        return self._active_config

    def _apply_env_overrides(self, data: dict[str, Any]) -> None:
        """Merges environment variables into loaded raw configurations."""
        if "gateway" not in data:
            data["gateway"] = {}
        
        if host := os.getenv("INFERX_GATEWAY_HOST"):
            data["gateway"]["host"] = host
        if port := os.getenv("INFERX_GATEWAY_PORT"):
            data["gateway"]["port"] = int(port)
        if level := os.getenv("INFERX_LOG_LEVEL"):
            data["log_level"] = level
