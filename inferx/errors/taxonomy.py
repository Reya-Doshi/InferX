# inferx/errors/taxonomy.py
"""
InferX Structured Error Taxonomy.

Defines the system-wide base exception class and concrete domain-specific errors
to ensure uniform error handling, telemetry tracking, and API status mapping.
"""

from typing import Optional


class InferXError(Exception):
    """
    Base exception class for all errors generated within the InferX runtime.

    Provides standard fields to assist API routers, retry policies, and
    observability systems in diagnosing issues.
    """

    def __init__(
        self,
        code: str,
        message: str,
        cause: Optional[str] = None,
        retryable: bool = False,
        severity: str = "ERROR",
    ) -> None:
        self.code = code
        self.message = message
        self.cause = cause
        self.retryable = retryable
        self.severity = severity
        super().__init__(message)

    def to_dict(self) -> dict[str, any]:
        """Serializes the error structure for JSON logging and API responses."""
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "cause": self.cause,
                "retryable": self.retryable,
                "severity": self.severity,
            }
        }


class ConfigurationError(InferXError):
    """Raised when configuration parsing or validation boundaries are violated."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_SYS_CONFIG",
            message=message,
            cause=cause,
            retryable=False,
            severity="FATAL",
        )


class ValidationError(InferXError):
    """Raised when request payload schemas, sizes, or parameters are invalid."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_VAL_INVALID_INPUT",
            message=message,
            cause=cause,
            retryable=False,
            severity="WARN",
        )


class DependencyInjectionError(InferXError):
    """Raised when the DI container fails to resolve or bind a component interface."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_SYS_DI",
            message=message,
            cause=cause,
            retryable=False,
            severity="FATAL",
        )


class HardwareError(InferXError):
    """Raised when hardware drivers, CUDA contexts, or NVML calls fail."""

    def __init__(
        self, message: str, cause: Optional[str] = None, retryable: bool = True
    ) -> None:
        super().__init__(
            code="ERR_HW_FAILURE",
            message=message,
            cause=cause,
            retryable=retryable,
            severity="FATAL",
        )


class WorkerError(InferXError):
    """Raised when worker subprocesses crash, fail heartbeats, or fail to start."""

    def __init__(
        self, message: str, cause: Optional[str] = None, retryable: bool = True
    ) -> None:
        super().__init__(
            code="ERR_WORKER_FAILURE",
            message=message,
            cause=cause,
            retryable=retryable,
            severity="ERROR",
        )


class SystemTimeoutError(InferXError):
    """Raised when queues, execution cycles, or worker calls exceed allotted TTL limits."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_SYS_TIMEOUT",
            message=message,
            cause=cause,
            retryable=True,
            severity="ERROR",
        )


class ResourceExhaustedError(InferXError):
    """Raised when the runtime is overloaded (VRAM limits exceeded or queue is full)."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_AC_RESOURCE_EXHAUSTED",
            message=message,
            cause=cause,
            retryable=True,
            severity="WARN",
        )


class StateTransitionError(InferXError):
    """Raised when an illegal transition is requested on the RuntimeState model."""

    def __init__(self, message: str, cause: Optional[str] = None) -> None:
        super().__init__(
            code="ERR_STATE_TRANSITION",
            message=message,
            cause=cause,
            retryable=False,
            severity="ERROR",
        )
