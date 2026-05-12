import logging
import time
from ipaddress import ip_interface

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.db.models import Model

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError
from .sync_state import touch_branch_run_progress

PROGRESS_HEARTBEAT_ROW_INTERVAL = 500
PROGRESS_HEARTBEAT_SECONDS = 60

logger = logging.getLogger("forward_netbox.sync")


def record_aggregated_conflict_warning(
    runner, *, model_string, reason, warning_message
):
    key = (model_string, reason)
    count = runner._aggregated_conflict_warning_counts.get(key, 0)
    if count < runner.CONFLICT_WARNING_DETAIL_LIMIT:
        runner.logger.log_warning(
            warning_message,
            obj=runner.sync,
        )
    else:
        runner._aggregated_conflict_warning_suppressed[key] = (
            runner._aggregated_conflict_warning_suppressed.get(key, 0) + 1
        )
    runner._aggregated_conflict_warning_counts[key] = count + 1


def emit_aggregated_conflict_warning_summaries(runner, model_string):
    for (warning_model, reason), suppressed_count in sorted(
        runner._aggregated_conflict_warning_suppressed.items()
    ):
        if warning_model != model_string or suppressed_count <= 0:
            continue
        runner.logger.log_warning(
            f"Suppressed {suppressed_count} additional {model_string} conflict warnings "
            f"for `{reason}` after the first {runner.CONFLICT_WARNING_DETAIL_LIMIT}.",
            obj=runner.sync,
        )


def record_aggregated_skip_warning(runner, *, model_string, reason, warning_message):
    key = (model_string, reason)
    count = runner._aggregated_skip_warning_counts.get(key, 0)
    if count < runner.CONFLICT_WARNING_DETAIL_LIMIT:
        runner.logger.log_warning(
            warning_message,
            obj=runner.sync,
        )
    else:
        runner._aggregated_skip_warning_suppressed[key] = (
            runner._aggregated_skip_warning_suppressed.get(key, 0) + 1
        )
    runner._aggregated_skip_warning_counts[key] = count + 1


def emit_aggregated_skip_warning_summaries(runner, model_string):
    for (warning_model, reason), suppressed_count in sorted(
        runner._aggregated_skip_warning_suppressed.items()
    ):
        if warning_model != model_string or suppressed_count <= 0:
            continue
        runner.logger.log_warning(
            f"Suppressed {suppressed_count} additional {model_string} skip warnings "
            f"for `{reason}` after the first {runner.CONFLICT_WARNING_DETAIL_LIMIT}.",
            obj=runner.sync,
        )


def ipaddress_assignment_skip_reason(address):
    try:
        interface = ip_interface(str(address))
    except ValueError:
        return None

    network = interface.network
    ip_address = interface.ip
    if network.version == 4 and network.prefixlen < 31:
        if ip_address == network.network_address:
            return "network-id"
        if ip_address == network.broadcast_address:
            return "broadcast-address"
    if network.version == 6 and network.prefixlen < 127:
        if ip_address == network.network_address:
            return "network-id"
    return None


def dependency_key(model_string, row):
    if model_string == "dcim.device":
        return (row.get("name"),)
    if model_string == "dcim.interface":
        return (row.get("device"), row.get("name"))
    if model_string == "dcim.virtualchassis":
        return (row.get("device"), row.get("vc_name") or row.get("name"))
    return None


def mark_dependency_failed(runner, model_string, row):
    key = dependency_key(model_string, row)
    if key and all(item not in (None, "") for item in key):
        runner._failed_dependencies.setdefault(model_string, set()).add(key)


def dependency_failed(runner, model_string, key):
    return key in runner._failed_dependencies.get(model_string, set())


def _emit_progress_heartbeat(
    runner,
    *,
    activity_verb,
    model_string,
    processed_rows,
    total_rows,
    state,
    last_emit_at,
):
    current_time = time.monotonic()
    if (
        processed_rows == 1
        or processed_rows % PROGRESS_HEARTBEAT_ROW_INTERVAL == 0
        or current_time - last_emit_at >= PROGRESS_HEARTBEAT_SECONDS
    ):
        shard_index = state.get("current_shard_index")
        total_plan_items = state.get("total_plan_items")
        if shard_index and total_plan_items:
            message = (
                f"{activity_verb} shard {shard_index}/{total_plan_items} for "
                f"{model_string}: {processed_rows}/{total_rows} rows."
            )
        else:
            message = f"{activity_verb} {processed_rows}/{total_rows} rows for {model_string}."
        runner.logger.log_info(message, obj=runner.sync)
        touch_branch_run_progress(
            runner.sync,
            phase_message=message,
            model_string=model_string,
            shard_index=shard_index,
            total_plan_items=total_plan_items,
            row_count=processed_rows,
            row_total=total_rows,
        )
        return current_time
    return last_emit_at


def record_issue(
    runner,
    model_string,
    message,
    row,
    *,
    exception=None,
    context=None,
    defaults=None,
):
    if runner.ingestion is None:
        return None
    from ..models import ForwardIngestionIssue

    if exception is not None and getattr(exception, "issue_id", None):
        issue = ForwardIngestionIssue.objects.filter(
            pk=exception.issue_id,
            ingestion=runner.ingestion,
        ).first()
        if issue:
            return issue

    exception_name = (
        exception.__class__.__name__
        if exception is not None
        else "ForwardSyncDataError"
    )
    context_data = _json_safe_value(dict(context or {}))
    defaults_data = _json_safe_value(dict(defaults or {}))
    issue_key = (
        runner.ingestion.pk if runner.ingestion else None,
        ForwardIngestionPhaseChoices.SYNC,
        model_string,
        exception_name,
        str(message),
        str(sorted(context_data.items())),
        str(sorted(defaults_data.items())),
    )
    if issue_key in runner._recorded_issue_ids:
        existing = ForwardIngestionIssue.objects.filter(
            ingestion=runner.ingestion,
            phase=ForwardIngestionPhaseChoices.SYNC,
            model=model_string,
            message=message,
            exception=exception_name,
            coalesce_fields=context_data,
            defaults=defaults_data,
        ).first()
        if existing:
            if exception is not None and hasattr(exception, "issue_id"):
                exception.issue_id = existing.pk
            return existing
        return None
    issue = ForwardIngestionIssue.objects.create(
        ingestion=runner.ingestion,
        phase=ForwardIngestionPhaseChoices.SYNC,
        model=model_string,
        message=message,
        coalesce_fields=context_data,
        defaults=defaults_data,
        raw_data=_json_safe_value(row or {}),
        exception=exception_name,
    )
    runner._recorded_issue_ids.add(issue_key)
    if exception is not None and hasattr(exception, "issue_id"):
        exception.issue_id = issue.pk
    runner.logger.log_failure(f"{model_string}: {message}", obj=runner.ingestion)
    return issue


def _json_safe_value(value):
    if isinstance(value, Model):
        return {
            "model": value._meta.label_lower,
            "pk": value.pk,
            "display": str(value),
        }
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return value


def apply_model_rows(runner, model_string, rows):
    if model_string == "dcim.interface":
        rows = sorted(rows, key=lambda row: bool(row.get("lag")))
    handler_name = f"_apply_{model_string.replace('.', '_')}"
    handler = getattr(runner, handler_name, None)
    if handler is None:
        runner.logger.log_warning(
            f"No adapter is defined yet for {model_string}; skipping {len(rows)} rows.",
            obj=runner.sync,
        )
        return
    runner.logger.log_info(
        f"Applying {len(rows)} rows for {model_string}.",
        obj=runner.sync,
    )
    state = runner.sync.get_branch_run_state()
    last_emit_at = 0.0
    processed_rows = 0
    for row in rows:
        processed_rows += 1
        try:
            with transaction.atomic():
                result = handler(row)
            runner.events_clearer.increment()
            if result is False:
                runner.logger.increment_statistics(model_string, outcome="skipped")
            else:
                runner.logger.increment_statistics(model_string, outcome="applied")
        except ForwardDependencySkipError as exc:
            logger.exception("Failed applying %s row", model_string)
            runner.logger.increment_statistics(model_string, outcome="skipped")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
            )
        except (ForwardSearchError, ForwardQueryError, ForwardSyncDataError) as exc:
            logger.exception("Failed applying %s row", model_string)
            mark_dependency_failed(runner, model_string, row)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", {}),
                defaults=getattr(exc, "defaults", {}),
            )
        except (ValidationError, IntegrityError) as exc:
            logger.exception("Failed applying %s row", model_string)
            mark_dependency_failed(runner, model_string, row)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
            )
        except Exception as exc:
            logger.exception("Failed applying %s row", model_string)
            mark_dependency_failed(runner, model_string, row)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
            )
        last_emit_at = _emit_progress_heartbeat(
            runner,
            activity_verb="Applying",
            model_string=model_string,
            processed_rows=processed_rows,
            total_rows=len(rows),
            state=state,
            last_emit_at=last_emit_at,
        )
    runner.logger.log_info(
        f"Finished applying rows for {model_string}.",
        obj=runner.sync,
    )
    emit_aggregated_conflict_warning_summaries(runner, model_string)
    emit_aggregated_skip_warning_summaries(runner, model_string)
    runner.events_clearer.clear()


def delete_model_rows(runner, model_string, rows):
    handler_name = f"_delete_{model_string.replace('.', '_')}"
    handler = getattr(runner, handler_name, None)
    if handler is None:
        runner.logger.log_warning(
            f"No delete adapter is defined yet for {model_string}; skipping {len(rows)} rows.",
            obj=runner.sync,
        )
        return
    runner.logger.log_info(
        f"Deleting {len(rows)} rows for {model_string}.",
        obj=runner.sync,
    )
    state = runner.sync.get_branch_run_state()
    last_emit_at = 0.0
    processed_rows = 0
    for row in rows:
        processed_rows += 1
        try:
            with transaction.atomic():
                deleted = handler(row)
            runner.events_clearer.increment()
            if deleted:
                runner.logger.increment_statistics(model_string, outcome="applied")
            else:
                runner.logger.increment_statistics(model_string, outcome="skipped")
        except (ForwardSearchError, ForwardQueryError) as exc:
            logger.exception("Failed deleting %s row", model_string)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
                context=getattr(exc, "context", {}),
                defaults=getattr(exc, "defaults", {}),
            )
        except (ValidationError, IntegrityError) as exc:
            logger.exception("Failed deleting %s row", model_string)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
            )
        except Exception as exc:
            logger.exception("Failed deleting %s row", model_string)
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
            )
        last_emit_at = _emit_progress_heartbeat(
            runner,
            activity_verb="Deleting",
            model_string=model_string,
            processed_rows=processed_rows,
            total_rows=len(rows),
            state=state,
            last_emit_at=last_emit_at,
        )
    runner.logger.log_info(
        f"Finished deleting rows for {model_string}.",
        obj=runner.sync,
    )
    runner.events_clearer.clear()
