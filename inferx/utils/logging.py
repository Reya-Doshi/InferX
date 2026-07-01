# inferx/utils/logging.py
"""
InferX Structured JSON Logging Utility.

Configures the standard library logging pipeline to output structured JSON strings,
automatically harvesting contextual telemetry parameters from Task/Thread-local ContextVars.
"""
from datetime import datetime, timezone
import json
import logging
import sys
import contextvars
from typing import Any, Optional

# ContextVar storing telemetry metadata (e.g. trace_id, request_id, model_name)
telemetry_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "telemetry_context", default={}
)

# Standardized keys extracted from context for index routing
TELEMETRY_KEYS = [
    "request_id",
    "trace_id",
    "span_id",
    "worker_id",
    "batch_id",
    "scheduler_id",
    "runtime_state",
    "model_name",
    "tenant_id"
]


class JSONFormatter(logging.Formatter):
    """
    Custom formatter that serializes log records into structured JSON lines.
    
    Harvests variables from active contextvars and explicit extra logging parameters.
    """
    def format(self, record: logging.LogRecord) -> str:
        # 1. Base log structure
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "filename": record.filename,
            "line_number": record.lineno,
        }

        # 2. Inject exception details if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # 3. Harvest variables from the global async context
        ctx = telemetry_context.get()
        for key in TELEMETRY_KEYS:
            if key in ctx and ctx[key] is not None:
                log_data[key] = ctx[key]

        # 4. Harvest variables from standard LogRecord attributes (passed via extra=...)
        for key in TELEMETRY_KEYS:
            if hasattr(record, key):
                val = getattr(record, key)
                if val is not None:
                    log_data[key] = val

        # 5. Harvest any generic custom parameters passed to the log
        for key, val in record.__dict__.items():
            if key not in TELEMETRY_KEYS and key not in [
                "args", "created", "msg", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process"
            ]:
                log_data[key] = val

        return json.dumps(log_data)


class InferXLogger(logging.Logger):
    """
    Extended logger that supports custom level (FATAL) and wraps
    context keys from contextvars into standard log records.
    """
    FATAL = 50

    def fatal(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        """Log a message with severity 'FATAL'."""
        if self.isEnabledFor(self.FATAL):
            self._log(self.FATAL, msg, args, **kwargs)

    def _log(
        self,
        level: int,
        msg: Any,
        args: Any,
        exc_info: Any = None,
        extra: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        **kwargs: Any
    ) -> None:
        # Extract direct custom keyword arguments and move them into the extra dict
        if kwargs:
            if extra is None:
                extra = {}
            else:
                extra = dict(extra)
            extra.update(kwargs)

        super()._log(
            level,
            msg,
            args,
            exc_info=exc_info,
            extra=extra,
            stack_info=stack_info,
            stacklevel=stacklevel
        )


# Register custom logger class globally on import
logging.setLoggerClass(InferXLogger)


def configure_logging(level: str = "INFO") -> logging.Logger:
    """
    Bootstraps the root logging engine.
    
    Replaces default console handlers with the JSONFormatter and redirects
    output to stdout to prevent buffering delays inside containerized environments.
    """
    root_logger = logging.getLogger("inferx")
    root_logger.setLevel(level.upper())
    
    # Avoid duplicate handlers if re-called
    if root_logger.handlers:
        return root_logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)
    root_logger.propagate = False
    
    return root_logger


def get_logger(name: str) -> InferXLogger:
    """Retrieves a named logger instance prefixed under the inferx namespace."""
    logger = logging.getLogger(f"inferx.{name}")
    # Verify correct typing wrapper
    if isinstance(logger, InferXLogger):
        return logger
    raise TypeError("Logging initialization encountered type mismatch.")
