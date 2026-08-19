"""Safe diagnostic helpers for persisted and user-visible job evidence."""

import re
from collections import Counter
from copy import deepcopy

REDACTED_DIAGNOSTIC = "<redacted diagnostic>"
SAFE_FAILURE_LOG_PREFIX = "The operation failed"
# The previous wording told operators to use "the job identifier and exception
# type for server-side investigation". No such server-side record exists: the
# plugin writes no `logger.exception` and passes `exc_info` nowhere, so every
# Python-logger call it makes records the exception *class* and nothing more -
# exactly what the row already showed. Directing a support engineer at a log
# that never held the answer is what made five identical ingestions
# undiagnosable. Point at the evidence that is actually written instead.
SAFE_FAILURE_LOG_MESSAGE = (
    "The operation failed. No classifier was recorded on this row; see this "
    "run's ingestion issues and per-model failure evidence."
)
# A warning is not a failure, and saying so cost a real diagnosis.
#
# `_FAILURE_LEVELS` includes "warning" because a warning body can carry
# customer data - a query name is customer-chosen - so it must be redacted like
# any other. But the replacement sentence asserted that an operation had FAILED,
# which is a different claim from "this text was redacted".
#
# A deployment's support bundle showed thirty of these seconds into a healthy
# run, one per model, each pointing at ingestion issues and per-model failure
# evidence that were empty - because nothing had failed. They were the routine
# preflight notice that a model cannot run a diff and "still syncs". The run's
# actual defect was elsewhere, and thirty invented failures is what the operator
# and the support engineer had to read past to find it.
#
# So the redaction stays and the claim matches the level.
# Deliberately does not contain `SAFE_FAILURE_LOG_PREFIX`. A support engineer
# greps a bundle for that phrase, and a warning that spells it out - even to
# deny it - is found by that grep and counted again.
SAFE_WARNING_LOG_MESSAGE = (
    "A warning was recorded and its detail is redacted. No classifier was "
    "recorded on this row; a warning is not a failure."
)
_WARNING_LEVELS = {"warning"}
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


# An OwnershipConflictError is raised bare from nine call sites and its
# message is stripped before anything persists it, so every job record and UI
# error read "failed (OwnershipConflictError)" and nothing more - identical
# whatever refused the reconciliation. The messages embed source device keys
# and tag names, both of which are customer identifiers, so the message itself
# cannot be persisted: an allowlist maps it to a slug this module defines, and
# an unmatched message records that it was unrecognised rather than falling
# silent.
#
# This lives here, in the one function every failure path formats through,
# rather than at a call site. The first attempt enriched a single job's error
# path; the job a customer actually hit was a different one that formats the
# same sentence, so the fix appeared to work and changed nothing they saw.
#
# The second attempt catalogued only the four device-identity refusals, which
# are raised while merging an ingestion. Every refusal raised while
# materializing tags went uncatalogued, so the whole scope-tag reconciliation
# job could only ever report `unrecognized-ownership-conflict` - which is what
# a customer hit, and the reason had to be recovered by reading source. The
# catalogue covers both halves now, and `test_ownership_conflict_reason`
# asserts every raise site in the ownership modules maps to a slug, so a new
# raise site cannot quietly reintroduce the gap.
_OWNERSHIP_CONFLICT_EXCEPTION = "OwnershipConflictError"
_OWNERSHIP_CONFLICT_REASONS = (
    # Device-identity refusals, raised while merging an ingestion.
    ("identity-ambiguous", "forward device identity is ambiguous"),
    ("identity-evidence-mismatch", "identity evidence does not match merged"),
    ("source-key-multiple-devices", "maps to multiple live netbox devices"),
    ("device-already-mapped", "is already mapped to forward source key"),
    # Tag-ownership refusals, raised while materializing scope and status tags.
    ("tag-claim-type-conflict", "is already controlled as"),
    (
        "tag-slug-reserved-without-provenance",
        "is reserved for forward status ownership",
    ),
    ("scope-tag-name-slug-disagree", "identify different netbox tags"),
    # No longer raised: an ambiguous name is now held rather than refused. The
    # entry stays because job records persisted under it are still on customer
    # systems and must keep resolving to the same slug.
    ("tag-mutation-identity-unresolved", "refusing name-only tag mutation"),
    ("virtual-parent-claims-disagree", "virtual-parent claims disagree"),
)


def ownership_conflict_reason(exc) -> str:
    """A schema-safe slug for why ownership reconciliation refused."""
    haystack = str(exc).casefold()
    for slug, needle in _OWNERSHIP_CONFLICT_REASONS:
        if needle in haystack:
            return slug
    return "unrecognized-ownership-conflict"


# Three bespoke exemptions have now been carved out of "persist the class name
# and nothing else": OwnershipConflictError, the ValidationError rule
# catalogue, and - the reason this table exists - a wholesale fetch failure that
# reduced thirty distinct model failures to thirty copies of "ForwardQueryError."
# A fourth exemption would be the same mistake again, so the *rule* changed: a
# failure is characterised by an allowlisted slug wherever one matches, and by
# leading wording with every value-bearing token dropped where none does.
#
# Ordering is significant only in that the ownership rules stay first, so
# `ownership_conflict_reason` and `failure_reason` agree on the same message.
_FAILURE_REASON_RULES = (
    # Transport and protocol: which of timeout / auth / shape / parse failed is
    # the first question asked of any Forward call, and none of these needles
    # can match a device name, address or tenant label.
    ("auth-unauthorized", "unauthorized"),
    ("auth-forbidden", "forbidden"),
    ("auth-invalid-credentials", "invalid credentials"),
    ("auth-token-expired", "token expired"),
    ("timeout", "timed out"),
    ("timeout", "timeout"),
    ("connection-refused", "connection refused"),
    ("connection-reset", "connection reset"),
    ("connection-aborted", "connection aborted"),
    ("connection-failed", "could not connect"),
    ("tls-verification-failed", "certificate verify failed"),
    ("dns-resolution-failed", "name or service not known"),
    ("rate-limited", "too many requests"),
    ("service-unavailable", "temporarily unavailable"),
    ("parse-error", "expecting value"),
    ("parse-error", "json decode"),
    ("parse-error", "could not parse"),
    ("shape-error", "unexpected response shape"),
    ("shape-error", "missing required field"),
    # Plugin-authored refusals. These are the messages the plugin composes
    # itself, so the slug is a rename of wording the plugin already controls -
    # it discloses nothing the operator did not configure.
    ("license-tier-denied", "license"),
    ("fetch-budget-exceeded", "exceeded its wall-clock budget"),
    ("diff-required-no-baseline", "no compatible baseline"),
    ("diff-required-full-only-contract", "is full-only"),
    ("diff-required-unavailable", "safe diff execution for"),
    ("diff-required-budget-exceeded", "diff budget was exceeded"),
    ("full-execution-not-contractually-safe", "not contractually safe"),
    ("full-execution-rejected-by-contract", "full execution is not allowed"),
    ("diff-execution-rejected-by-contract", "diff execution is not allowed"),
    ("unsafe-full-contract", "rejected an unsafe full contract"),
    ("no-enabled-query-maps", "no enabled nqe maps were resolved"),
    ("no-resolved-query-maps", "no enabled built-in or custom query maps"),
    ("duplicate-query-spec-execution", "duplicate logical nqe execution"),
    ("missing-network-id", "requires a network id"),
    ("missing-snapshot-id", "requires a snapshot id"),
    ("device-tag-scope-query-failed", "device tag filter query failed"),
    ("shard-fetch-failed", "shard-scoped nqe fetch failed"),
)

# `403` and `503` are HTTP status codes, not customer data, and they are the
# single most actionable token in a Forward client error - but they carry a
# digit, so the wording masker drops them. Recover them as a slug instead.
_HTTP_STATUS_IN_MESSAGE = re.compile(r"\bHTTP\s+(\d{3})\b")
_MAX_FAILURE_REASON_SLUGS = 3
# Wording is kept only until the first value-bearing token, and never for more
# than this many words. Both bounds matter: a Forward error body is arbitrary
# customer-shaped text, and a message that starts with plausible English can
# continue into a tenant label.
_MAX_SUMMARY_WORDS = 12


def _http_status_slug(exc) -> str:
    """`http-<code>` for a failure whose HTTP status is recoverable."""
    # httpx keeps the status on the response of the raised cause, which is the
    # structured form and cannot be confused with anything in the body.
    response = getattr(getattr(exc, "__cause__", None), "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int) and 100 <= status <= 599:
        return f"http-{status}"
    match = _HTTP_STATUS_IN_MESSAGE.search(str(exc))
    if match:
        return f"http-{match.group(1)}"
    return ""


_MISSING_KEY_EXCEPTION = "KeyError"
_SCHEMA_FIELD_NAMES = None


def _schema_field_names() -> frozenset:
    """Field names this plugin's own model contracts declare.

    A `KeyError` is the one exception whose entire diagnostic value is the key,
    and the key is also the token most likely to be customer data - a device
    name used to index a lookup. So it was redacted wholesale, and a customer's
    sync failed with `Forward ingestion failed (KeyError).` and no model, no
    row and no key. Nothing in the bundle could narrow it further.

    The distinction the redaction needs is not "is this a string" but "did this
    repository choose this name". `MODEL_SYNC_CONTRACTS` is exactly that: every
    field name the sync contracts declare is vocabulary this code wrote, not
    anything a fabric supplied. A key drawn from it can be named safely; a key
    that is not stays redacted, which keeps a device-name key silent even
    though it is the one that would be most useful.
    """
    global _SCHEMA_FIELD_NAMES
    if _SCHEMA_FIELD_NAMES is None:
        from .sync_contracts import MODEL_SYNC_CONTRACTS

        names = set()
        # The model labels themselves, not only their fields. A dict keyed by
        # `model_string` is as common in this codebase as one keyed by a field
        # name, and `ipam.ipaddress` is no more customer data than `status` is.
        names.update(MODEL_SYNC_CONTRACTS)
        for contract in MODEL_SYNC_CONTRACTS.values():
            names.update(contract.required_fields or ())
            names.update(contract.allowed_coalesce_fields or ())
            names.update(contract.preserve_existing_on_blank_fields or ())
            for group in contract.default_coalesce_fields or ():
                names.update(group)
        _SCHEMA_FIELD_NAMES = frozenset(
            name for name in names if isinstance(name, str) and name
        )
    return _SCHEMA_FIELD_NAMES


def missing_key_reason(exc) -> str:
    """`missing-key-<field>` when a KeyError names a field this schema declares.

    The slug is normalised to the shape `recovered_classifiers` can read back -
    lowercase, hyphen separated - because a reason that cannot be recovered
    from the rendered message is discarded by the log renderer and the support
    bundle, which is how the classifier work was undone once before.
    """
    args = getattr(exc, "args", ()) or ()
    if len(args) != 1 or not isinstance(args[0], str):
        return ""
    key = args[0]
    if key not in _schema_field_names():
        return ""
    # `.` and `_` both become `-`: the readback pattern accepts neither, and a
    # reason that cannot be recovered from the rendered message is discarded by
    # the log renderer and the support bundle.
    slug = key.replace("_", "-").replace(".", "-").casefold()
    if not re.fullmatch(r"[a-z][a-z0-9]*(?:[+-][a-z0-9]+)*", slug):
        return ""
    return f"missing-key-{slug}"


def failure_reason(exc) -> str:
    """A value-free characterisation of why something failed, or ``""``.

    Returns allowlisted slugs only. Nothing derived from the exception message
    text reaches the caller through this function.
    """
    if exception_type(exc) == _OWNERSHIP_CONFLICT_EXCEPTION:
        return ownership_conflict_reason(exc)
    if exception_type(exc) == _MISSING_KEY_EXCEPTION:
        # A KeyError's message is the key and nothing else, so the generic
        # needle rules below can only match it by coincidence. Answer from the
        # key or answer nothing.
        return missing_key_reason(exc)
    slugs = []
    status_slug = _http_status_slug(exc)
    if status_slug:
        slugs.append(status_slug)
    haystack = str(exc).casefold()
    for slug, needle in _FAILURE_REASON_RULES:
        if needle in haystack and slug not in slugs:
            slugs.append(slug)
        if len(slugs) >= _MAX_FAILURE_REASON_SLUGS:
            break
    return "+".join(slugs)


def redacted_message_prefix(message: str) -> str:
    """Leading wording of a message, stopping at the first value-bearing token.

    `redacted_message_shape` keeps the *whole* wording of a message because the
    messages it serves are Django validation strings, whose vocabulary the
    plugin can reason about. An exception message can be an arbitrary Forward
    API response body, so the same tolerance is not safe there: stopping at the
    first masked token means a device name, address, prefix, interface name,
    hostname or tenant label can never appear, because every one of them either
    carries a digit, dot, colon, slash, hyphen or underscore, or sits after
    something that does (a status code, a quote, a brace, a key).
    """
    kept = []
    for token in str(message).split():
        stripped = token.strip(".,;:()[]{}'\"`")
        if not stripped.isalpha():
            break
        kept.append(stripped)
        if len(kept) >= _MAX_SUMMARY_WORDS:
            break
    return " ".join(kept)


def failure_classifier(exc) -> str:
    """How a failure is named, everywhere: `ClassName` or `ClassName: reason`.

    Every composer of an operator-facing or persisted failure string resolves
    the name here. That is the whole point of the module: `exception_type`
    alone answers *what class* raised and never *why*, and each place that
    reached for it separately grew its own idea of how much of the reason to
    keep. `record_issue` was the one that still did - it composed
    `(IntegrityError; constraint ...)` from its own diagnosis while formatting
    the classifier by hand, so a `ForwardQueryError` that knew perfectly well
    it was a shape error persisted as `(ForwardQueryError)` into
    `ForwardIngestionIssue.message`, the single row an operator actually reads.

    Callers that want schema-level detail append it to this; they do not
    re-derive the name.
    """
    classifier = exception_type(exc)
    reason = failure_reason(exc)
    return f"{classifier}: {reason}" if reason else classifier


def safe_exception_summary(exc) -> str:
    """Classifier plus a value-free characterisation, never message content.

    This replaced `f"{exc.__class__.__name__}."`, which destroyed the reason
    before the logger or the database ever saw it - unrecoverable by any
    downstream tooling, and the reason five identical customer ingestions could
    not be diagnosed at all.
    """
    named = failure_classifier(exc)
    if failure_reason(exc):
        return f"{named}."
    # Uncatalogued: fall back to bounded leading wording. This is the one thing
    # `safe_exception_summary` does that `failure_classifier` does not, and it
    # stays here rather than in the shared namer deliberately - the wording
    # fallback is a best-effort read of arbitrary text, so it belongs to the
    # inline summary and not to the name every other composer interpolates.
    prefix = redacted_message_prefix(str(exc))
    if prefix:
        return f"{named}: {prefix}."
    return f"{named}."


def safe_operation_failure(operation: str, exc) -> str:
    """Return an operator-safe failure message with a stable classifier.

    The reason lives here, at the single function every failure path formats
    through, rather than at any call site - and it is resolved by the shared
    rule for every exception, not by a per-class branch.
    """
    return f"{operation} failed ({failure_classifier(exc)})."


def model_failure_summary(model_results) -> list[dict]:
    """Per-model failure evidence: which models failed, and why.

    A successful dependency preview carries `model_results` with per-model
    `failure_count`, `fetch_mode` and `query_name`. The run that actually failed
    carried none of it into the support bundle, so a wholesale fetch failure
    read as one sentence repeated once per model with no way to tell them apart.
    Only the model string, exception class and allowlisted reason slug are kept;
    each is checked against the safe-token pattern before it is emitted. Query
    names are deliberately not carried here - a custom NQE map is named by the
    customer, and this summary must stay safe without needing to reason about
    that.
    """

    def _safe(value: str) -> str:
        return value if _SAFE_DIAGNOSTIC_TOKEN.fullmatch(value) else "redacted"

    summary = []
    for result in model_results or []:
        if not isinstance(result, dict):
            continue
        failure_count = int(result.get("failure_count") or 0)
        if not failure_count:
            continue
        summary.append(
            {
                "model": _safe(
                    str(result.get("model") or result.get("model_string") or "")
                ),
                "exception": _safe(str(result.get("failure_exception") or "")),
                "reason": _safe(str(result.get("failure_reason") or "")),
                "failure_count": failure_count,
            }
        )
    return sorted(summary, key=lambda item: (item["model"], item["reason"]))


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


# The classifier shapes this module itself emits: `safe_operation_failure`
# writes `(ClassName)` or `(ClassName: reason)`, and `safe_exception_summary`
# writes `ClassName: reason.` inline. Both are matched here, and nothing else
# is: the class name must be CamelCase ending in a recognised exception suffix,
# and the reason must be a lowercase slug of the shape this module's own
# catalogue produces. A device name, address, hostname or tenant label matches
# neither, so recovering these two groups discloses nothing the row's own
# classifier did not already.
_LOGGED_CLASSIFIER = re.compile(
    r"(?<![A-Za-z0-9_.])"
    r"([A-Z][A-Za-z0-9_]*(?:Error|Exception|Timeout|Warning))"
    r"(?:\s*:\s*([a-z][a-z0-9]*(?:[+-][a-z0-9]+)*))?"
)
_MAX_LOGGED_CLASSIFIERS = 3


def recovered_classifiers(text) -> list[str]:
    """The classifiers this module wrote into `text`, in order, deduplicated.

    One rule for reading a classifier back out of a message, shared by the log
    renderer and the Job-error readback. They had two, and the stricter of them
    - a full-sentence match requiring the literal word "failed" - silently
    discarded the classifier from every message that phrased itself any other
    way.
    """
    parts = []
    for classifier, reason in _LOGGED_CLASSIFIER.findall(str(text or "")):
        rendered = f"{classifier}: {reason}" if reason else classifier
        if rendered not in parts:
            parts.append(rendered)
        if len(parts) >= _MAX_LOGGED_CLASSIFIERS:
            break
    return parts


def safe_job_error_summary(error) -> str:
    """Expose only an exception classifier from a persisted core Job error.

    The pattern must accept the reason slug `safe_operation_failure` now writes
    for every classified failure. It did not, so a job error that named its
    reason was recognised as *unparseable* and exported as
    `<redacted diagnostic>` - strictly worse than the bare classifier it
    replaced. That was already latent for `OwnershipConflictError`; the reason
    catalogue would have made it the common case.

    The same was true of every job error that does not use the word "failed".
    A merge whose finalization "requires recovery (ForwardQueryError)", or that
    "was preserved (ForwardQueryError)", carries its classifier in plain sight
    and was still exported as `<redacted diagnostic>` - the readback threw away
    what the writer had just been fixed to record.

    Two tiers, in this order. The first is the exact inverse of what
    `safe_operation_failure` writes, so any identifier inside those parentheses
    is known to be a class name and is kept whatever it is spelled like -
    `ContributorBaselineUnavailable` and `JobAlreadyActive` carry no `Error`
    suffix and must still read back. The second is the shared loose recovery,
    which has to survive arbitrary sentences and therefore demands the suffix:
    without it `Mgmt_Vl211` is a valid-looking class name. Anything matching
    neither is redacted.
    """
    error = str(error or "")
    if not error:
        return ""
    match = re.fullmatch(
        r"[^()\r\n]* failed \(([A-Za-z_][A-Za-z0-9_.]*)"
        r"(?::\s*([a-z][a-z0-9]*(?:[+-][a-z0-9]+)*))?\)\.",
        error,
    )
    if match:
        classifier, reason = match.group(1), match.group(2)
        return (
            f"Job failed ({classifier}: {reason})."
            if reason
            else f"Job failed ({classifier})."
        )
    parts = recovered_classifiers(error)
    if not parts:
        return REDACTED_DIAGNOSTIC
    return f"Job failed ({'; '.join(parts)})."


def safe_log_message(message, level: str = "") -> str:
    """Redact a log row's body, claiming only what its level supports."""
    parts = recovered_classifiers(message)
    if parts:
        return f"{SAFE_FAILURE_LOG_PREFIX} ({'; '.join(parts)})."
    if str(level).lower() in _WARNING_LEVELS:
        return SAFE_WARNING_LOG_MESSAGE
    return SAFE_FAILURE_LOG_MESSAGE


def safe_failure_log_message(message) -> str:
    """Keep a failure row's classifier instead of flattening it to one sentence.

    Every critical/error/failure/warning row used to render as the same fixed
    sentence, so a run in which thirty models failed for one reason and a run in
    which they failed for thirty presented identically - in the UI, in the log
    export and in the support bundle. The redaction still applies to the message
    body; what changes is that the classifier the row was given survives it.
    """
    parts = recovered_classifiers(message)
    if not parts:
        return SAFE_FAILURE_LOG_MESSAGE
    return f"{SAFE_FAILURE_LOG_PREFIX} ({'; '.join(parts)})."


def _sanitize_log_rows(rows):
    sanitized = []
    for row in rows or []:
        if isinstance(row, (list, tuple)):
            rendered = list(row)
            level = str(rendered[1] if len(rendered) > 1 else "").lower()
            if level in _FAILURE_LEVELS and len(rendered) > 4:
                rendered[4] = safe_log_message(rendered[4], level)
            sanitized.append(rendered)
            continue
        if isinstance(row, dict):
            rendered = sanitize_job_diagnostics(row)
            level = str(rendered.get("level") or "").lower()
            if level in _FAILURE_LEVELS and "message" in rendered:
                rendered["message"] = safe_log_message(rendered["message"], level)
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
# A field-scoped error is no better off. A field name identifies *where* the
# rejection landed, not *which rule* rejected it, and one field routinely
# carries several unrelated rules: NetBox raises on `untagged_vlan` both for an
# interface whose mode does not admit one and for a VLAN outside the device's
# site. Those have different causes and different fixes, and a customer sync
# recorded only the field name for one of them - leaving the failure as
# undiagnosable as the `__all__` case this table was built for. So every
# message is matched here, whatever field it names.
#
# Matching against an allowlist keeps the rule identifiable while persisting
# only slugs this module defines. An unrecognised message records that it was
# unrecognised and nothing else, which is a prompt to extend this table rather
# than a reason to start storing message text.
_VALIDATION_RULES = (
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
    (
        "untagged-vlan-needs-interface-mode",
        "interface mode does not support an untagged vlan",
    ),
    (
        "untagged-vlan-outside-device-site",
        "must belong to the same site as the interface's parent",
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


def _validation_rules(exc) -> tuple[list[str], list[str]]:
    """Recognised rule slugs, plus redacted wording for anything unrecognised."""
    # Only a dict-constructed ValidationError has `message_dict`; a bare one
    # raises AttributeError, which `getattr` swallows into None. `full_clean()`
    # produces the dict form, but a ValidationError raised directly still has
    # to be diagnosable - reading only `message_dict` would silently record
    # nothing for it.
    #
    # Every field is read, not just `__all__`. Reading `__all__` alone was the
    # whole gap: it made a rule legible exactly when NetBox declined to say
    # which field it concerned, and illegible whenever NetBox did say.
    if hasattr(exc, "error_dict"):
        messages = [
            message
            for field_messages in getattr(exc, "message_dict", {}).values()
            for message in field_messages
        ]
    else:
        messages = list(getattr(exc, "messages", ()) or ())
    matched = set()
    unrecognized = []
    for message in messages:
        haystack = str(message).casefold()
        for slug, needle in _VALIDATION_RULES:
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
    failed_models = diagnosis.get("failed_models") or []
    stem = message[:-1] if message.endswith(".") else message
    if failed_models:
        listed = ", ".join(failed_models[:8])
        if len(failed_models) > 8:
            listed = f"{listed}, +{len(failed_models) - 8} more"
        return f"{stem} for {len(failed_models)} model(s): {listed}."
    if constraint:
        return f"{stem} on constraint {constraint}."
    # `__all__` is the absence of a field name, so it is never worth printing;
    # a real field name is, and now that rules are read from every field the two
    # are no longer alternatives. Which field was rejected and which rule
    # rejected it are different facts, and the field alone was what left a
    # customer's `untagged_vlan` failure ambiguous between two unrelated causes.
    named_fields = [field for field in invalid_fields if field != "__all__"]
    if named_fields:
        stem = f"{stem} on invalid field(s) {', '.join(named_fields)}"
    if rules:
        return f"{stem} violating {', '.join(rules)}."
    if unrecognized:
        # Wording only - every value-bearing token has already been masked.
        return f"{stem}: {'; '.join(unrecognized)}."
    if named_fields:
        return f"{stem}."
    return message


def is_preexisting_rule_rejection(exc, written_fields) -> bool:
    """True when a rejection is about state the change did not write.

    `full_clean` validates the whole object, but a sync writes only the fields
    that differ, so a rejection can name a field the row never touched — an
    untagged VLAN left behind by a device that moved sites is still on the
    interface when a later sync changes only its MTU. Refusing to write is right
    either way; calling it *our* failure is not, because a failure also fails the
    row's dependents and nothing about the incoming data makes it retryable.

    Both halves matter. The rule must be one the catalogue can name: skipping is
    the disposition for a rejection we understand, and treating anything we
    cannot name as someone else's problem is how a real defect gets quietly
    downgraded. And the rejected field must be outside what this change writes,
    or it is ours by definition.

    One predicate rather than one per apply path — the bulk engine and the
    row-oriented adapter reached opposite conclusions about the same interface
    while each was locally correct.
    """
    diagnosis = structured_failure_diagnosis(exc)
    if not diagnosis.get("validation_rules"):
        return False
    rejected = set(diagnosis.get("invalid_fields") or ())
    return not (rejected & set(written_fields or ()))


def is_caused_rule_rejection(exc, written_fields) -> bool:
    """True when a rejection is about a field this change itself writes.

    NOT the complement of `is_preexisting_rule_rejection`. Both are False for a
    rule the catalogue cannot name, and that is the point: the two are used
    where the *default* differs, so each has to answer for itself rather than
    invert the other.

    At merge the default is to skip. Every `ValidationError` there was treated
    as unsatisfiable, which is right for a rule that is a property of the
    destination — retrying cannot change the answer, and counting it a failure
    is what turns a handful of refused rows into a baseline that can never
    promote. But it also swallowed rejections the merge itself caused, where a
    retry is exactly the right response. Those are the ones this names, and only
    those change disposition: an uncatalogued rule keeps skipping, so nothing
    the catalogue has not been taught can newly wedge a baseline.
    """
    diagnosis = structured_failure_diagnosis(exc)
    if not diagnosis.get("validation_rules"):
        return False
    rejected = set(diagnosis.get("invalid_fields") or ())
    return bool(rejected & set(written_fields or ()))


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
    rules, unrecognized = _validation_rules(exc)
    if rules:
        diagnosis["validation_rules"] = rules
    if unrecognized:
        diagnosis["unrecognized_validation_rules"] = unrecognized

    # An exception may carry its own structured, value-free diagnosis. That is
    # the general form of what OwnershipConflictError and the ValidationError
    # rule catalogue each got as a bespoke exemption: rather than teach this
    # function about one more exception class, let a raiser that already knows
    # the safe facts attach them. Free text is still never persisted - every
    # value is checked against the safe-token pattern here, whatever the raiser
    # believed about it.
    supplied = getattr(exc, "safe_diagnosis", None)
    if isinstance(supplied, dict):
        for key, value in supplied.items():
            safe_key = str(key)
            if not _SAFE_DIAGNOSTIC_TOKEN.fullmatch(safe_key) or safe_key in diagnosis:
                continue
            if isinstance(value, (list, tuple)):
                kept = [
                    str(item)
                    for item in value
                    if _SAFE_DIAGNOSTIC_TOKEN.fullmatch(str(item))
                ]
                if kept:
                    diagnosis[safe_key] = kept
            elif isinstance(value, bool) or isinstance(value, int):
                diagnosis[safe_key] = value
            elif _SAFE_DIAGNOSTIC_TOKEN.fullmatch(str(value)):
                diagnosis[safe_key] = str(value)

    return diagnosis
