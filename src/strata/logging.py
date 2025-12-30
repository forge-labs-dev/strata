"""Structured logging with correlation IDs for Strata.

This module provides structured JSON logging with automatic correlation ID
propagation. Request IDs are generated per-request and included in all log
entries within that request context. When OpenTelemetry tracing is enabled,
trace_id and span_id are also included.

Usage:
    from strata.logging import get_logger, RequestContext

    logger = get_logger(__name__)

    # In a request handler:
    with RequestContext(request_id="abc123"):
        logger.info("Processing request", table="ns.events", rows=1000)

    # Or use the FastAPI middleware which handles this automatically

Environment variables:
    STRATA_LOG_LEVEL - Log level (default: INFO)
    STRATA_LOG_FORMAT - "json" or "text" (default: json)
"""

import contextvars
import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from typing import Any

# Context variable for request-scoped data
_request_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "request_context", default={}
)


def generate_request_id() -> str:
    """Generate a unique request ID."""
    return uuid.uuid4().hex[:16]


def get_request_context() -> dict[str, Any]:
    """Get the current request context."""
    return _request_context.get()


def set_request_context(**kwargs: Any) -> contextvars.Token:
    """Set request context values. Returns token for reset."""
    current = _request_context.get().copy()
    current.update(kwargs)
    return _request_context.set(current)


def clear_request_context() -> None:
    """Clear all request context."""
    _request_context.set({})


@contextmanager
def RequestContext(**kwargs: Any):
    """Context manager for request-scoped logging context.

    Usage:
        with RequestContext(request_id="abc123", scan_id="scan-456"):
            logger.info("Processing")  # includes request_id and scan_id
    """
    token = set_request_context(**kwargs)
    try:
        yield
    finally:
        _request_context.reset(token)


def get_trace_context() -> dict[str, str]:
    """Get OpenTelemetry trace context if available.

    Returns trace_id and span_id if tracing is enabled and there's an
    active span, otherwise returns empty dict.
    """
    try:
        from strata.tracing import is_tracing_enabled

        if not is_tracing_enabled():
            return {}

        from opentelemetry import trace

        span = trace.get_current_span()
        if span is None:
            return {}

        ctx = span.get_span_context()
        if ctx is None or not ctx.is_valid:
            return {}

        return {
            "trace_id": format(ctx.trace_id, "032x"),
            "span_id": format(ctx.span_id, "016x"),
        }
    except ImportError:
        return {}


class StructuredFormatter(logging.Formatter):
    """JSON formatter that includes request context and trace IDs."""

    def __init__(self, include_timestamp: bool = True):
        super().__init__()
        self.include_timestamp = include_timestamp

    def format(self, record: logging.LogRecord) -> str:
        # Base log entry
        log_entry: dict[str, Any] = {
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        if self.include_timestamp:
            log_entry["timestamp"] = time.time()

        # Add request context
        ctx = get_request_context()
        if ctx:
            log_entry.update(ctx)

        # Add trace context if available
        trace_ctx = get_trace_context()
        if trace_ctx:
            log_entry.update(trace_ctx)

        # Add extra attributes passed via logger.info("msg", extra={...})
        # or via our custom logging methods
        if hasattr(record, "structured_data"):
            log_entry.update(record.structured_data)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add source location for errors/warnings
        if record.levelno >= logging.WARNING:
            log_entry["source"] = {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            }

        return json.dumps(log_entry)


class TextFormatter(logging.Formatter):
    """Human-readable text formatter with context for development."""

    def format(self, record: logging.LogRecord) -> str:
        # Build context string
        ctx_parts = []

        # Request context
        ctx = get_request_context()
        if "request_id" in ctx:
            ctx_parts.append(f"req={ctx['request_id'][:8]}")
        if "scan_id" in ctx:
            ctx_parts.append(f"scan={ctx['scan_id'][:8]}")

        # Trace context
        trace_ctx = get_trace_context()
        if "trace_id" in trace_ctx:
            ctx_parts.append(f"trace={trace_ctx['trace_id'][:8]}")

        ctx_str = f"[{' '.join(ctx_parts)}] " if ctx_parts else ""

        # Structured data
        data_str = ""
        if hasattr(record, "structured_data") and record.structured_data:
            data_parts = [f"{k}={v}" for k, v in record.structured_data.items()]
            data_str = " | " + ", ".join(data_parts)

        return f"{record.levelname:7} {ctx_str}{record.name}: {record.getMessage()}{data_str}"


class StructuredLogger(logging.Logger):
    """Logger that supports structured data as keyword arguments.

    Usage:
        logger.info("Request completed", rows=1000, elapsed_ms=42.5)
    """

    def _log_with_data(
        self,
        level: int,
        msg: str,
        args: tuple,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 2,
        **kwargs: Any,
    ) -> None:
        """Internal method to log with structured data."""
        if self.isEnabledFor(level):
            # Create record with extra structured data
            record = self.makeRecord(
                self.name,
                level,
                "(unknown file)",
                0,
                msg,
                args,
                exc_info,
                func=None,
                extra=None,
                sinfo=stack_info,
            )
            # Attach structured data to the record
            record.structured_data = kwargs  # type: ignore
            self.handle(record)

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._log_with_data(logging.DEBUG, msg, args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._log_with_data(logging.INFO, msg, args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._log_with_data(logging.WARNING, msg, args, **kwargs)

    def error(self, msg: str, *args, exc_info: Any = None, **kwargs) -> None:
        self._log_with_data(logging.ERROR, msg, args, exc_info=exc_info, **kwargs)

    def critical(self, msg: str, *args, exc_info: Any = None, **kwargs) -> None:
        self._log_with_data(logging.CRITICAL, msg, args, exc_info=exc_info, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._log_with_data(logging.ERROR, msg, args, exc_info=True, **kwargs)


# Register our custom logger class
logging.setLoggerClass(StructuredLogger)

# Module-level state
_configured = False
_log_format = os.environ.get("STRATA_LOG_FORMAT", "json").lower()


def configure_logging(
    level: str | None = None,
    format: str | None = None,
) -> None:
    """Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to
            STRATA_LOG_LEVEL env var or INFO.
        format: "json" or "text". Defaults to STRATA_LOG_FORMAT env var or "json".
    """
    global _configured, _log_format

    if _configured:
        return

    level = level or os.environ.get("STRATA_LOG_LEVEL", "INFO").upper()
    format = format or os.environ.get("STRATA_LOG_FORMAT", "json").lower()
    _log_format = format

    # Create handler
    handler = logging.StreamHandler(sys.stderr)

    if format == "text":
        handler.setFormatter(TextFormatter())
    else:
        handler.setFormatter(StructuredFormatter())

    # Configure root logger for strata
    root_logger = logging.getLogger("strata")
    root_logger.setLevel(getattr(logging, level, logging.INFO))
    root_logger.addHandler(handler)
    root_logger.propagate = False

    # Also configure uvicorn loggers to use our format
    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers = [handler]

    _configured = True


def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger.

    Args:
        name: Logger name, typically __name__

    Returns:
        StructuredLogger instance
    """
    # Ensure logging is configured
    if not _configured:
        configure_logging()

    return logging.getLogger(name)  # type: ignore


# FastAPI middleware for request context
async def request_context_middleware(request, call_next):
    """FastAPI middleware that sets up request context with correlation IDs.

    Adds:
    - request_id: Generated unique ID for this request (or from X-Request-ID header)
    - method: HTTP method
    - path: Request path
    - tenant_id: Tenant identifier (from tenant context, for multi-tenancy)

    When OpenTelemetry tracing is enabled, trace_id and span_id are automatically
    included in log entries via get_trace_context().
    """
    # Import tenant context (lazy import to avoid circular dependencies)
    from strata.tenant import get_tenant_id

    # Get or generate request ID
    request_id = request.headers.get("X-Request-ID") or generate_request_id()

    # Get tenant_id from tenant context (set by tenant middleware)
    # Note: tenant middleware runs AFTER this middleware in the stack,
    # so we access the context at response time for accurate tenant_id
    tenant_id = get_tenant_id()

    # Set up request context
    token = set_request_context(
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        tenant_id=tenant_id,
    )

    # Add request_id to response headers for client correlation
    try:
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        _request_context.reset(token)
