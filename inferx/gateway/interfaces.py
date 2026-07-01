# inferx/gateway/interfaces.py
"""
InferX Gateway Interfaces.

Declares the RequestContext structures, protocol adapters, request routers,
and middleware boundaries.
"""
from abc import ABC, abstractmethod
from typing import Any, Callable, Coroutine, Tuple
from pydantic import BaseModel, Field


class GatewayRequestContext(BaseModel):
    """
    Data model tracking a single request's context, tracing headers,
    and routing targets throughout the gateway pipeline.
    """
    request_id: str
    trace_id: str
    tenant_id: str
    priority: int = Field(default=1, ge=0)
    model_name: str
    version: str
    payload: str
    is_streaming: bool = False

    model_config = {"arbitrary_types_allowed": True}


class IProtocolAdapter(ABC):
    """Abstract interface defining standard connection termination adapters."""

    @abstractmethod
    async def handle_connection(self, reader: Any, writer: Any) -> None:
        """Handles socket protocol frame negotiations and routes to the execution pipeline."""
        pass


class IGatewayRouter(ABC):
    """Abstract interface defining dynamic request target routing controllers."""

    @abstractmethod
    def route(self, context: GatewayRequestContext) -> Tuple[str, str]:
        """Resolves target model and version configurations for a request context."""
        pass
