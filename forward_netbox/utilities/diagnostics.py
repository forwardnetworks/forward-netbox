"""Safe diagnostic helpers for persisted and user-visible job evidence."""

import re
from collections import Counter
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
_SAFE_DIAGNOSTIC_TOKEN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def exception_type(exc) -> str:
    """Return a stable exception classifier without exception content."""
    return exc.__class__.__name__


def safe_operation_failure(operation: str, exc) -> str:
    """Return an operator-safe failure message with a stable classifier."""
    return f"{operation} failed ({exception_type(exc)})."


def diff_fallback_summary(model_results) -> list[dict]:
    """Return aggregate diff fallback classifiers without query/customer data."""

    counts = Counter()
    for result in model_results or []:
        if not isinstance(result, dict):
            continue
        model = str(result.get("model") or result.get("model_string") or "")
        parameters = result.get("fetch_parameters")
        reason = (
            str(parameters.get("fallback_reason") or "")
            if isinstance(parameters, dict)
            else ""
        )
        if not reason:
            continue
        safe_model = model if _SAFE_DIAGNOSTIC_TOKEN.fullmatch(model) else "redacted"
        safe_reason = reason if _SAFE_DIAGNOSTIC_TOKEN.fullmatch(reason) else "redacted"
        counts[(safe_model, safe_reason)] += 1
    return [
        {
            "model": model,
            "reason": reason,
            "count": count,
        }
        for (model, reason), count in sorted(counts.items())
    ]


def safe_job_error_summary(error) -> str:
    """Expose only an exception classifier from a persisted core Job error."""
    error = str(error or "")
    match = re.fullmatch(
        r"[^()\r\n]* failed \(([A-Za-z_][A-Za-z0-9_.]*)\)\.",
        error,
    )
    if not match:
        return REDACTED_DIAGNOSTIC if error else ""
    return f"Job failed ({match.group(1)})."


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


# Non-field validation errors report `__all__` as their field name, which is
# the absence of a field rather than the name of one - so a diagnosis that only
# records field names says nothing at all about them. The messages themselves
# cannot be persisted: NetBox interpolates before raising
# (`_("{ip} is a network ID...").format(ip=...)`), so by the time the exception
# exists the address is inside the string.
#
# Matching against an allowlist keeps the rule identifiable while persisting
# only slugs this module defines. An unrecognised message records that it was
# unrecognised and nothing else, which is a prompt to extend this table rather
# than a reason to start storing message text.
_NON_FIELD_VALIDATION_RULES = (
    ("network-id-not-assignable", "is a network id, which may not be assigned"),
    ("broadcast-not-assignable", "is a broadcast address, which may not be assigned"),
    (
        "primary-ip-reassignment-blocked",
        "cannot reassign ip address while it is designated as the primary ip",
    ),
    (
        "oob-ip-reassignment-blocked",
        "cannot reassign ip address while it is designated as the oob ip",
    ),
    (
        "primary-mac-reassignment-blocked",
        "cannot reassign mac address while it is designated as the primary mac",
    ),
)


def redacted_message_shape(message: str) -> str:
    """The wording of a message with every value-bearing token removed.

    An allowlist can only describe rules already known, so an unrecognised
    message would still reach the UI saying nothing - which is the exact failure
    this whole line of work exists to remove. Keeping the *wording* makes any
    future rule legible on first sight instead of after a release.

    A token survives only if it is purely alphabetic. Addresses, prefixes,
    interface names, device names and hostnames all carry a digit, dot, colon,
    slash, hyphen or underscore, so they are masked; ordinary English words are
    not. This is deliberately stricter than it needs to be for the messages
    known today, because the ones it has to stay safe for are the ones nobody
    has read yet.
    """
    kept = []
    for token in str(message).split():
        stripped = token.strip(".,;:()[]{}'\"")
        if stripped.isalpha():
            kept.append(stripped)
        elif kept and kept[-1] != REDACTED_DIAGNOSTIC:
            kept.append(REDACTED_DIAGNOSTIC)
        elif not kept:
            kept.append(REDACTED_DIAGNOSTIC)
    return " ".join(kept)


def _non_field_validation_rules(exc) -> tuple[list[str], list[str]]:
    """Recognised rule slugs, plus redacted wording for anything unrecognised."""
    # Only a dict-constructed ValidationError has `message_dict`; a bare one
    # raises AttributeError, which `getattr` swallows into None. `full_clean()`
    # produces the dict form, but a ValidationError raised directly still has
    # to be diagnosable - reading only `message_dict` would silently record
    # nothing for it.
    if hasattr(exc, "error_dict"):
        non_field = list(getattr(exc, "message_dict", {}).get("__all__", ()))
    else:
        non_field = list(getattr(exc, "messages", ()) or ())
    matched = set()
    unrecognized = []
    for message in non_field:
        haystack = str(message).casefold()
        for slug, needle in _NON_FIELD_VALIDATION_RULES:
            if needle in haystack:
                matched.add(slug)
                break
        else:
            shape = redacted_message_shape(message)
            if shape and shape not in unrecognized:
                unrecognized.append(shape)
    return sorted(matched), unrecognized


def describe_failure(message: str, diagnosis: dict) -> str:
    """Append the schema-level cause to an operator-facing failure message.

    Three recorders - sync orchestration, sync reporting and merge - each grew
    their own copy of this, and the merge one was added a release late, so the
    same failure read differently depending on which phase caught it. One
    function, three call sites.
    """
    constraint = diagnosis.get("constraint_name") or ""
    rules = diagnosis.get("validation_rules") or []
    unrecognized = diagnosis.get("unrecognized_validation_rules") or []
    invalid_fields = diagnosis.get("invalid_fields") or []
    stem = message[:-1] if message.endswith(".") else message
    if constraint:
        return f"{stem} on constraint {constraint}."
    if rules:
        return f"{stem} violating {', '.join(rules)}."
    if unrecognized:
        # Wording only - every value-bearing token has already been masked.
        return f"{stem}: {'; '.join(unrecognized)}."
    if invalid_fields:
        return f"{stem} on invalid field(s) {', '.join(invalid_fields)}."
    return message


def structured_failure_diagnosis(exc) -> dict:
    """Schema-level detail about a failure, never the values that caused it.

    A merge failure previously persisted only the exception class name and an
    empty ``raw_data``. That is safe but undiagnosable: four `IntegrityError`
    rows blocked a customer's baseline for a full day, and nothing anywhere
    recorded *which constraint* they violated, so the failures could not be
    acted on from the GUI, the API, the CLI or a support bundle.

    Constraint, table, column and field names are schema identifiers the plugin
    itself defines — they carry no customer data, unlike the key *values* a
    Postgres DETAIL line embeds, which are deliberately not captured here.
    """
    diagnosis = {"exception_type": exception_type(exc)}

    # psycopg surfaces unique/foreign-key violation metadata on the cause's
    # `diag`; these are catalogue names, not row contents.
    diag = getattr(getattr(exc, "__cause__", None), "diag", None)
    for attribute in ("constraint_name", "table_name", "column_name"):
        value = str(getattr(diag, attribute, "") or "").strip()
        if value and _SAFE_DIAGNOSTIC_TOKEN.fullmatch(value):
            diagnosis[attribute] = value

    # Django ValidationError names the offending fields; the messages can quote
    # submitted values, so only the field names are kept.
    message_dict = (
        getattr(exc, "message_dict", None) if hasattr(exc, "error_dict") else None
    )
    if isinstance(message_dict, dict) and message_dict:
        diagnosis["invalid_fields"] = sorted(
            str(field) for field in message_dict if str(field)
        )
    # `__all__` names no field, so field names alone leave a non-field
    # validation failure exactly as opaque as recording nothing. This runs for
    # a bare ValidationError too, which has no `message_dict` at all and would
    # otherwise be recorded as nothing but its exception class.
    rules, unrecognized = _non_field_validation_rules(exc)
    if rules:
        diagnosis["validation_rules"] = rules
    if unrecognized:
        diagnosis["unrecognized_validation_rules"] = unrecognized

    return diagnosis
