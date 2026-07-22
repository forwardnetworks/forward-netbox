"""Safe diagnostic helpers for persisted and user-visible job evidence."""

from copy import deepcopy


REDACTED_DIAGNOSTIC = "<redacted diagnostic>"
SAFE_FAILURE_LOG_MESSAGE = (
    "The operation failed. Use the job identifier and exception type for "
    "server-side investigation."
)
_FAILURE_LEVELS = {"critical", "error", "failure", "warning"}
_SENSITIVE_DIAGNOSTIC_KEYS = {
    "error",
    "traceback",
    "worker_terminal_error",
}


def exception_type(exc) -> str:
    """Return a stable exception classifier without exception content."""
    return exc.__class__.__name__


def safe_operation_failure(operation: str, exc) -> str:
    """Return an operator-safe failure message with a stable classifier."""
    return f"{operation} failed ({exception_type(exc)})."


def diagnostic_shape(value):
    """Describe diagnostic data without retaining customer-provided values."""
    if isinstance(value, dict):
        return {
            "type": "mapping",
            "fields": sorted(str(key) for key in value),
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return {"type": "collection", "count": len(value)}
    return {"type": type(value).__name__}


def _sanitize_log_rows(rows):
    sanitized = []
    for row in rows or []:
        if isinstance(row, (list, tuple)):
            rendered = list(row)
            level = str(rendered[1] if len(rendered) > 1 else "").lower()
            if level in _FAILURE_LEVELS and len(rendered) > 4:
                rendered[4] = SAFE_FAILURE_LOG_MESSAGE
            sanitized.append(rendered)
            continue
        if isinstance(row, dict):
            rendered = sanitize_job_diagnostics(row)
            level = str(rendered.get("level") or "").lower()
            if level in _FAILURE_LEVELS and "message" in rendered:
                rendered["message"] = SAFE_FAILURE_LOG_MESSAGE
            sanitized.append(rendered)
            continue
        sanitized.append(REDACTED_DIAGNOSTIC)
    return sanitized


def sanitize_job_diagnostics(value):
    """Remove exception content from stored job data before presentation/export."""
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            normalized_key = str(key).lower()
            if normalized_key in _SENSITIVE_DIAGNOSTIC_KEYS:
                sanitized[key] = REDACTED_DIAGNOSTIC
            elif normalized_key in {"logs", "log_entries"} and isinstance(item, list):
                sanitized[key] = _sanitize_log_rows(item)
            else:
                sanitized[key] = sanitize_job_diagnostics(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_job_diagnostics(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_job_diagnostics(item) for item in value]
    return deepcopy(value)
