# inferx/core/bootstrap.py
"""
InferX Dependency Injection & Bootstrap Coordinator.

Implements a provider-based DI container and coordinates async component instantiation.
"""
from abc import ABC, abstractmethod
import os
from typing import Any, Callable, Dict, Type, TypeVar

from inferx.core.config import AsyncYAMLConfigLoader
from inferx.core.context import RuntimeContext
from inferx.core.health import HealthManager
from inferx.core.lifecycle import RuntimeLifecycle
from inferx.core.supervisor import RuntimeSupervisor
from inferx.errors.taxonomy import DependencyInjectionError
from inferx.interfaces.core import (
    IConfigLoader,
    IDIContainer,
    IHealthManager,
    IRuntimeLifecycle,
    IRuntimeSupervisor
)
from inferx.utils.logging import configure_logging, get_logger

from inferx.admission.limiter import TokenBucketLimiter
from inferx.admission.manager import AdmissionManager
from inferx.admission.shedder import BackpressureController, LoadShedder, CircuitBreaker
from inferx.gateway.manager import GatewayManager
from inferx.gateway.middleware import MiddlewarePipeline
from inferx.gateway.protocols import RestAdapter, WebSocketAdapter
from inferx.gateway.router import GatewayRouter
from inferx.model.cache import ModelCache
from inferx.model.loader import ModelLoader
from inferx.model.manager import ModelRuntimeManager
from inferx.model.registry import ModelRegistry
from inferx.model.interfaces import ModelMetadata

T = TypeVar("T")
logger = get_logger("bootstrap")


class Provider(ABC):
    """Abstract Base Class representing a dependency provider."""
    
    @abstractmethod
    def get(self) -> Any:
        """Retrieves or creates the target instance."""
        pass


class SingletonProvider(Provider):
    """Provider that wraps a singleton instance, lazy loading it on the first resolve."""
    def __init__(self, factory_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self._factory_fn = factory_fn
        self._args = args
        self._kwargs = kwargs
        self._instance: Any = None

    def get(self) -> Any:
        if self._instance is None:
            self._instance = self._factory_fn(*self._args, **self._kwargs)
        return self._instance


class FactoryProvider(Provider):
    """Provider that executes the factory function on every resolution request."""
    def __init__(self, factory_fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self._factory_fn = factory_fn
        self._args = args
        self._kwargs = kwargs

    def get(self) -> Any:
        return self._factory_fn(*self._args, **self._kwargs)


class DIContainer(IDIContainer):
    """
    Provider-based Dependency Injection Container.
    
    Manages lazy singleton and factory bindings for clean architecture decoupling.
    """
    def __init__(self) -> None:
        self._providers: Dict[Type[Any], Provider] = {}

    def register(self, interface: Type[T], provider: Any) -> None:
        """Registers a Provider to resolve dependencies for an interface."""
        if not isinstance(provider, Provider):
            raise DependencyInjectionError(
                message="DI registration requires a Provider instance.",
                cause=f"Received: {type(provider).__name__}. Expected subclass of Provider."
            )
        self._providers[interface] = provider

    def resolve(self, interface: Type[T]) -> T:
        """Resolves the instance bound to the requested interface."""
        provider = self._providers.get(interface)
        if provider is None:
            raise DependencyInjectionError(
                message=f"DI resolution failed for: {interface.__name__}",
                cause="Target interface has no registered provider."
            )
        
        instance = provider.get()
        if not isinstance(instance, interface):
            raise DependencyInjectionError(
                message=f"DI type assertion failed for: {interface.__name__}",
                cause=f"Resolved instance of type {type(instance).__name__} does not implement target interface."
            )
        return instance


async def bootstrap_core(config_path: str) -> IDIContainer:
    """
    Asynchronously bootstraps the dependency container and instantiates core systems.
    
    Args:
        config_path: Path to the YAML configuration file.
        
    Returns:
        An initialized IDIContainer with active dependency providers.
    """
    container = DIContainer()

    # 1. Bind configuration loader singleton provider
    config_loader = AsyncYAMLConfigLoader(config_path)
    container.register(IConfigLoader, SingletonProvider(lambda: config_loader))

    # Load configuration asynchronously
    config = await config_loader.load()

    # 2. Configure root structured logger
    configure_logging(config.log_level)
    logger.info("Initializing runtime components (Async Bootstrap)...", component="bootstrap")

    # 3. Bind RuntimeContext singleton provider
    context = RuntimeContext()
    container.register(RuntimeContext, SingletonProvider(lambda: context))

    # 4. Bind HealthManager singleton provider
    health_manager = HealthManager(config.admission.vram_high_watermark)
    container.register(IHealthManager, SingletonProvider(lambda: health_manager))

    # 5. Bind RuntimeSupervisor singleton provider
    supervisor = RuntimeSupervisor(
        gpus=config.worker.gpus,
        heartbeat_timeout_ms=config.worker.heartbeat_timeout_ms
    )
    container.register(IRuntimeSupervisor, SingletonProvider(lambda: supervisor))

    # 6. Bind RuntimeLifecycle singleton provider
    lifecycle = RuntimeLifecycle(context, supervisor)
    container.register(IRuntimeLifecycle, SingletonProvider(lambda: lifecycle))

    # 7. Model runtime dependencies
    model_registry = ModelRegistry()
    model_registry.register_model(ModelMetadata(
        model_name="llama",
        version="v1.0",
        backend_type="pytorch",
        estimated_vram_bytes=4 * 1024 * 1024 * 1024
    ))
    model_registry.register_model(ModelMetadata(
        model_name="llama",
        version="v2.0",
        backend_type="pytorch",
        estimated_vram_bytes=4 * 1024 * 1024 * 1024
    ))
    model_registry.register_alias("llama", "latest", "v2.0")

    model_loader = ModelLoader()
    model_cache = ModelCache(max_vram_bytes=10 * 1024 * 1024 * 1024)
    model_runtime = ModelRuntimeManager(
        registry=model_registry,
        loader=model_loader,
        cache=model_cache
    )
    container.register(ModelRuntimeManager, SingletonProvider(lambda: model_runtime))

    # 8. Admission Manager dependencies
    limiter = TokenBucketLimiter(
        global_capacity=float(config.admission.rate_limit_capacity),
        global_refill_rate=float(config.admission.rate_limit_refill_rate)
    )
    backpressure = BackpressureController(
        max_vram_ratio=config.admission.vram_high_watermark
    )
    shedder = LoadShedder(backpressure_controller=backpressure)
    circuit_breaker = CircuitBreaker()
    admission_manager = AdmissionManager(
        context=context,
        limiter=limiter,
        shedder=shedder,
        circuit_breaker=circuit_breaker
    )
    container.register(AdmissionManager, SingletonProvider(lambda: admission_manager))

    # 9. Gateway middleware pipeline, router & manager
    allowed_keys = ["sk-valid-key"]
    if auth_token := os.getenv("INFERX_AUTH_TOKEN"):
        allowed_keys.append(auth_token)
        try:
            import base64
            decoded = base64.b64decode(auth_token.encode("utf-8")).decode("utf-8")
            allowed_keys.append(decoded)
        except Exception:
            pass

    pipeline = MiddlewarePipeline(
        admission_manager=admission_manager,
        allowed_api_keys=allowed_keys,
        max_request_size_bytes=1024 * 1024
    )
    gateway_router = GatewayRouter()
    
    from inferx.observability.manager import TelemetryManager
    telemetry_manager = TelemetryManager()
    container.register(TelemetryManager, SingletonProvider(lambda: telemetry_manager))

    ws_adapter = WebSocketAdapter(
        pipeline=pipeline,
        router=gateway_router,
        run_prediction_fn=model_runtime.predict
    )
    rest_adapter = RestAdapter(
        pipeline=pipeline,
        router=gateway_router,
        run_prediction_fn=model_runtime.predict,
        ws_adapter=ws_adapter,
        telemetry_manager=telemetry_manager
    )
    
    gateway_manager = GatewayManager(
        host=config.gateway.host,
        port=config.gateway.port,
        rest_adapter=rest_adapter,
        ws_adapter=ws_adapter
    )
    container.register(GatewayManager, SingletonProvider(lambda: gateway_manager))

    # Register default worker pool health check provider
    async def check_workers() -> bool:
        return True

    health_manager.register_provider("worker_pool", check_workers, domain="workers")

    logger.info("Async Bootstrap complete. Core dependency bindings resolved.", component="bootstrap")
    return container
