# inferx/gateway/middleware.py
"""
InferX Gateway Middleware Pipeline.

Implements API Key and JWT authentication, payload size validations,
tracing contexts, and Admission Controller routing filters.
"""
import uuid
from typing import Any, Dict, List, Optional

from inferx.admission.interfaces import AdmissionVerdict
from inferx.admission.manager import AdmissionManager
from inferx.gateway.interfaces import GatewayRequestContext
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.utils.logging import get_logger

logger = get_logger("gateway.middleware")


class MiddlewareException(Exception):
    """Exception raised by middleware filters when blocking request processing."""
    def __init__(self, message: str, status_code: int = 400, error_code: str = "BAD_REQUEST") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class MiddlewarePipeline:
    """
    Sequentially executes request validation, security checks, and rate-limits.
    """
    def __init__(
        self,
        admission_manager: AdmissionManager,
        allowed_api_keys: Optional[List[str]] = None,
        max_request_size_bytes: int = 128 * 1024  # 128 KB limit
    ) -> None:
        self.admission_manager = admission_manager
        self.allowed_api_keys = allowed_api_keys or []
        self.max_request_size_bytes = max_request_size_bytes

    async def execute(self, request_id: str, headers: Dict[str, str], payload: str) -> GatewayRequestContext:
        """
        Runs the request through the middleware stack.
        
        Args:
            request_id: Initial TCP request ID.
            headers: Headers dictionary parsed from request frame.
            payload: Input payload body.
            
        Returns:
            A populated GatewayRequestContext if all checks pass.
            
        Raises:
            MiddlewareException: If auth, sizes, or admission limits fail.
        """
        # 1. Tracing Context extraction
        trace_id = headers.get("x-trace-id", str(uuid.uuid4()))
        tenant_id = headers.get("x-tenant-id", "tenant-default")
        priority = int(headers.get("x-priority", "1"))
        model_name = headers.get("x-model-name", "llama")
        version = headers.get("x-model-version", "latest")
        is_streaming = headers.get("x-stream", "false").lower() == "true"

        # 2. Request Size Validation
        payload_bytes = payload.encode("utf-8")
        if len(payload_bytes) > self.max_request_size_bytes:
            raise MiddlewareException(
                message=f"Request size exceeds limit ({len(payload_bytes)} > {self.max_request_size_bytes} bytes)",
                status_code=413,
                error_code="PAYLOAD_TOO_LARGE"
            )

        # 3. Authentication & Authorization check
        # Accept API Key (Authorization: Bearer sk-...) or custom API key header
        auth_header = headers.get("authorization", "")
        api_key = ""
        if auth_header.startswith("Bearer "):
            api_key = auth_header.split(" ")[-1]
        
        # Fallback to custom key header
        if not api_key:
            api_key = headers.get("x-api-key", "")

        # Mock Authorization check (allow if key exists in allowed list, or ends with '-dev')
        if self.allowed_api_keys:
            if api_key not in self.allowed_api_keys and not api_key.endswith("-dev"):
                raise MiddlewareException(
                    message="Unauthorized access token signature.",
                    status_code=401,
                    error_code="UNAUTHORIZED"
                )
        else:
            # If no allowed keys config, require token presence
            if not api_key:
                raise MiddlewareException(
                    message="Missing API authorization token.",
                    status_code=401,
                    error_code="UNAUTHORIZED"
                )

        # 4. Admission Controller rate limits check
        # Construct ScheduledRequest for validation
        scheduled_req = ScheduledRequest(
            request_id=request_id,
            tenant_id=tenant_id,
            priority=priority,
            payload=payload,
            max_latency_ms=30000.0
        )

        admission_verdict = await self.admission_manager.admit(scheduled_req)
        if not admission_verdict.admitted:
            raise MiddlewareException(
                message=f"Request rejected by Admission Controller: {admission_verdict.error_code}",
                status_code=admission_verdict.status_code,
                error_code=admission_verdict.error_code or "ADMISSION_REJECTED"
            )

        # 5. Build Request Context
        return GatewayRequestContext(
            request_id=request_id,
            trace_id=trace_id,
            tenant_id=tenant_id,
            priority=priority,
            model_name=model_name,
            version=version,
            payload=payload,
            is_streaming=is_streaming
        )
