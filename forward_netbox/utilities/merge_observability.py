import os
import re
import signal
import threading
import traceback as traceback_module
from contextlib import contextmanager

from django.db import transaction
from django.db.models import Max
from django.utils import timezone
from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardPartialMergeError
from .diagnostics import safe_operation_failure

_WAITPID_RE = re.compile(
    r"waitpid returned (?P<wait_status>-?\d+)" r"(?: \(signal (?P<signal>\d+)\))?"
)
_CAPTURED_SIGNALS = tuple(
    item
    for item in (
        getattr(signal, "SIGHUP", None),
        getattr(signal, "SIGINT", None),
        getattr(signal, "SIGQUIT", None),
        getattr(signal, "SIGTERM", None),
    )
    if item is not None
)


class ForwardMergeSignalError(RuntimeError):
    """Turn a catchable process signal into persistable merge evidence."""

    def __init__(self, signum):
        self.signum = int(signum)
        self.signal_name = _signal_name(self.signum)
        super().__init__(f"Merge process received {self.signal_name}.")


def _signal_name(signum):
    try:
        return signal.Signals(int(signum)).name
    except (TypeError, ValueError):
        return f"SIGNAL_{signum}"


@contextmanager
def capture_merge_signals():
    """Raise a structured exception for catchable worker-termination signals."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous = {}

    def handler(signum, _frame):
        raise ForwardMergeSignalError(signum)

    try:
        for signum in _CAPTURED_SIGNALS:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, handler)
        yield
    finally:
        for signum, prior_handler in previous.items():
            signal.signal(signum, prior_handler)


def begin_merge_attempt(ingestion, *, job=None):
    """Create one durable attempt for one complete logical branch replay."""
    from ..models import ForwardIngestion, ForwardMergeAttempt

    job_id = getattr(job, "pk", None)
    if not isinstance(job_id, int):
        job_id = None
    elif (
        not ForwardMergeAttempt._meta.get_field("job")
        .remote_field.model.objects.filter(pk=job_id)
        .exists()
    ):
        # A management/test caller can provide a job-shaped object which has
        # no durable Core Job row. Keep the merge attempt rather than deferring
        # a foreign-key failure until the surrounding transaction commits.
        job_id = None
    with transaction.atomic():
        locked = ForwardIngestion.objects.select_for_update().get(pk=ingestion.pk)
        if job_id is not None:
            existing = (
                ForwardMergeAttempt.objects.filter(
                    ingestion=locked,
                    job_id=job_id,
                    status=ForwardMergeAttempt.Status.RUNNING,
                )
                .order_by("-attempt_number")
                .first()
            )
            if existing is not None:
                return existing
        last_number = (
            ForwardMergeAttempt.objects.filter(ingestion=locked).aggregate(
                highest=Max("attempt_number")
            )["highest"]
            or 0
        )
        return ForwardMergeAttempt.objects.create(
            ingestion=locked,
            job_id=job_id,
            attempt_number=last_number + 1,
        )


def initialize_merge_attempt(attempt, *, total_changes):
    if attempt is None:
        return
    now = timezone.now()
    values = {
        "status": attempt.Status.RUNNING,
        "phase": "applying",
        "total_changes": max(0, int(total_changes or 0)),
        "heartbeat_at": now,
    }
    attempt.__class__.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)


def checkpoint_merge_attempt(
    attempt,
    *,
    merged_changes,
    failed_changes,
    current_model,
    model_progress,
):
    """Persist an application lower bound after committed merge work."""
    if attempt is None:
        return
    now = timezone.now()
    sequence = int(attempt.checkpoint_sequence or 0) + 1
    values = {
        "phase": "applying",
        "merged_changes": max(0, int(merged_changes or 0)),
        "failed_changes": max(0, int(failed_changes or 0)),
        "current_model": str(current_model or "")[:100],
        "model_progress": dict(model_progress or {}),
        "checkpoint_sequence": sequence,
        "heartbeat_at": now,
    }
    attempt.__class__.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)


def mark_merge_attempt_applied(attempt):
    if attempt is None:
        return
    now = timezone.now()
    values = {
        "status": attempt.Status.APPLIED,
        "phase": "finalizing",
        "heartbeat_at": now,
    }
    attempt.__class__.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)


def complete_merge_attempt(attempt):
    if attempt is None:
        return
    now = timezone.now()
    values = {
        "status": attempt.Status.COMPLETED,
        "phase": "completed",
        "heartbeat_at": now,
        "finished_at": now,
    }
    attempt.__class__.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)


def _traceback_without_exception_messages(exc):
    """Format every traceback frame/chain without persisting row-derived messages."""
    parts = []
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        stack = traceback_module.extract_tb(current.__traceback__)
        if stack:
            parts.append("Traceback (most recent call last):\n")
            parts.extend(traceback_module.format_list(stack))
        parts.append(
            f"{current.__class__.__module__}.{current.__class__.__qualname__}\n"
        )
        if current.__cause__ is not None:
            parts.append("The above exception was the direct cause of:\n")
            current = current.__cause__
        elif current.__context__ is not None and not current.__suppress_context__:
            parts.append("During handling of the above exception:\n")
            current = current.__context__
        else:
            current = None
    return "".join(parts)


def failure_evidence(exc):
    signal_number = getattr(exc, "signum", None)
    exit_code = None
    failure_kind = "exception"
    if isinstance(exc, ForwardMergeSignalError):
        failure_kind = "signal"
    elif isinstance(exc, SystemExit):
        failure_kind = "process_exit"
        if isinstance(exc.code, int):
            exit_code = exc.code
    elif isinstance(exc, JobTimeoutException):
        failure_kind = "timeout"
    elif isinstance(exc, ForwardPartialMergeError):
        failure_kind = "partial_merge"
    elif isinstance(exc, KeyboardInterrupt):
        failure_kind = "interrupt"

    return {
        "failure_kind": failure_kind,
        "exception_type": exc.__class__.__name__,
        "failure_summary": safe_operation_failure("Forward merge", exc),
        "traceback": _traceback_without_exception_messages(exc),
        "process_exit_code": exit_code,
        "process_signal": signal_number,
        "process_signal_name": _signal_name(signal_number) if signal_number else "",
    }


def fail_merge_attempt(attempt, exc):
    if attempt is None:
        return
    now = timezone.now()
    evidence = failure_evidence(exc)
    values = {
        "status": attempt.Status.FAILED,
        "phase": "failed",
        "heartbeat_at": now,
        "finished_at": now,
        **evidence,
    }
    attempt.__class__.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)


def _rq_exception_text(job):
    if job is None:
        return ""
    rq_job_id = str(getattr(job, "job_id", "") or "").strip()
    if not rq_job_id:
        return ""
    queue_name = str(getattr(job, "queue_name", "") or "default").strip()
    try:
        import django_rq
        from rq.job import Job as RQJob

        queue = django_rq.get_queue(queue_name or "default")
        rq_job = RQJob.fetch(rq_job_id, connection=queue.connection)
        result = rq_job.latest_result()
        return str(
            getattr(result, "exc_string", "") or getattr(rq_job, "exc_info", "") or ""
        )
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - recovery must survive missing RQ evidence
        return ""


def rq_process_failure_evidence(job):
    """Extract only process status/signal evidence from RQ's parent-worker record."""
    exc_text = _rq_exception_text(job)
    match = _WAITPID_RE.search(exc_text)
    if match is None:
        return {
            "failure_kind": "unknown_termination",
            "exception_type": "UnknownProcessTermination",
            "failure_summary": (
                "Merge worker disappeared without inspectable process-exit evidence."
            ),
        }

    wait_status = int(match.group("wait_status"))
    signal_number = (
        int(match.group("signal")) if match.group("signal") is not None else None
    )
    exit_code = None
    if signal_number is None and wait_status >= 0:
        try:
            if os.WIFSIGNALED(wait_status):
                signal_number = os.WTERMSIG(wait_status)
            elif os.WIFEXITED(wait_status):
                exit_code = os.WEXITSTATUS(wait_status)
        except (TypeError, ValueError):
            pass
    return {
        "failure_kind": "signal" if signal_number else "process_exit",
        "exception_type": "WorkHorseKilledError",
        "failure_summary": ("RQ recorded an unexpected merge work-horse termination."),
        "process_wait_status": wait_status,
        "process_exit_code": exit_code,
        "process_signal": signal_number,
        "process_signal_name": _signal_name(signal_number) if signal_number else "",
    }


def interrupt_running_merge_attempt(ingestion, *, job=None):
    """Close the latest orphaned running attempt without implying row resume."""
    from ..models import ForwardMergeAttempt

    queryset = ForwardMergeAttempt.objects.filter(
        ingestion=ingestion,
        status__in=(
            ForwardMergeAttempt.Status.RUNNING,
            ForwardMergeAttempt.Status.APPLIED,
        ),
    )
    if job is not None and getattr(job, "pk", None) is not None:
        queryset = queryset.filter(job_id=job.pk)
    attempt = queryset.order_by("-attempt_number").first()
    if attempt is None:
        return None

    evidence = rq_process_failure_evidence(job)
    now = timezone.now()
    values = {
        "status": ForwardMergeAttempt.Status.INTERRUPTED,
        "phase": "interrupted",
        "heartbeat_at": now,
        "finished_at": now,
        **evidence,
    }
    ForwardMergeAttempt.objects.filter(pk=attempt.pk).update(**values)
    for key, value in values.items():
        setattr(attempt, key, value)
    return attempt


def merge_attempt_progress(attempt, *, now=None):
    if attempt is None:
        return None
    now = now or timezone.now()
    ended_at = attempt.finished_at or now
    elapsed = max(0.0, (ended_at - attempt.started_at).total_seconds())
    merged = max(0, int(attempt.merged_changes or 0))
    failed = max(0, int(attempt.failed_changes or 0))
    processed = merged + failed
    total = max(0, int(attempt.total_changes or 0))
    rate = processed / elapsed if elapsed > 0 else 0.0
    remaining = max(0, total - processed)
    eta = remaining / rate if rate > 0 and remaining else 0.0
    percent = min(processed, total) / total * 100.0 if total else 0.0
    return {
        "attempt_number": attempt.attempt_number,
        "status": attempt.status,
        "phase": attempt.phase,
        "total_changes": total,
        "merged_changes": merged,
        "failed_changes": failed,
        "processed_changes": processed,
        "current_model": attempt.current_model,
        "rate_changes_per_second": rate,
        "elapsed_seconds": elapsed,
        "eta_seconds": eta,
        "percent_complete": percent,
        "checkpoint_sequence": attempt.checkpoint_sequence,
        "started_at": attempt.started_at,
        "heartbeat_at": attempt.heartbeat_at,
        "finished_at": attempt.finished_at,
        "failure_kind": attempt.failure_kind,
        "exception_type": attempt.exception_type,
        "failure_summary": attempt.failure_summary,
        "traceback": attempt.traceback,
        "process_wait_status": attempt.process_wait_status,
        "process_exit_code": attempt.process_exit_code,
        "process_signal": attempt.process_signal,
        "process_signal_name": attempt.process_signal_name,
        "model_progress": attempt.model_progress,
        "recovery_semantics": "full_logical_replay",
    }


def merge_attempt_history(ingestion):
    """Return every durable attempt so prior interruption evidence stays visible."""
    attempts = getattr(ingestion, "merge_attempts", None)
    if attempts is None:
        return []
    return [
        merge_attempt_progress(attempt)
        for attempt in attempts.order_by("-attempt_number")
    ]
