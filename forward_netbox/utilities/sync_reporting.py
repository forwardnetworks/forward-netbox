import logging
import time
from ipaddress import ip_interface

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.db.models import F

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError
from .json_safe import json_safe_value
from .sync_primitives import dependency_parent_coverage_summary
from .sync_primitives import prime_dependency_lookup_caches

PROGRESS_HEARTBEAT_ROW_INTERVAL = 500
PROGRESS_HEARTBEAT_SECONDS = 60

logger = logging.getLogger("forward_netbox.sync")


def _increment_ingestion_delete_totals(runner, amount):
    if amount <= 0 or runner.ingestion is None:
        return
    ingestion = runner.ingestion
    ingestion.__class__.objects.filter(pk=ingestion.pk).update(
        applied_change_count=F("applied_change_count") + amount,
        deleted_change_count=F("deleted_change_count") + amount,
    )
    ingestion.applied_change_count = int(ingestion.applied_change_count or 0) + amount
    ingestion.deleted_change_count = int(ingestion.deleted_change_count or 0) + amount


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


# Examples kept for a rollup-reason summary; the rest are counted, not listed.
SKIP_WARNING_ROLLUP_SAMPLES = 5

# One-line summary per rollup reason ({total},{model},{reason},{examples},{suffix}).
ROLLUP_SUMMARY_TEMPLATES = {
    "missing-module-bay": (
        "Skipped {total} {model} row(s) because the Forward row did not provide "
        "a module-bay name. Correct the source query data and re-run the sync. "
        "Examples: {examples}{suffix}."
    ),
    "shared-vip": (
        "{total} {model} row(s) share a virtual IP with another FHRP group; the "
        "VIP stays on the first group and the others are kept without a duplicate "
        "VIP (no data lost). Examples: {examples}{suffix}."
    ),
}
_DEFAULT_ROLLUP_TEMPLATE = (
    "Skipped {total} {model} row(s) for `{reason}`. Examples: {examples}{suffix}."
)


def _skip_warning_detail_limit(runner, reason):
    return getattr(runner, "SKIP_WARNING_DETAIL_LIMITS", {}).get(
        reason, runner.CONFLICT_WARNING_DETAIL_LIMIT
    )


def record_aggregated_skip_warning(
    runner, *, model_string, reason, warning_message, sample=None
):
    key = (model_string, reason)
    count = runner._aggregated_skip_warning_counts.get(key, 0)
    if reason in getattr(runner, "SKIP_WARNING_ROLLUP_REASONS", frozenset()):
        # Systemic readiness gap: never log per row. Keep a few examples and let
        # emit_aggregated_skip_warning_summaries collapse the rest into one line.
        if sample:
            samples = runner._aggregated_skip_warning_samples.setdefault(key, [])
            if sample not in samples and len(samples) < SKIP_WARNING_ROLLUP_SAMPLES:
                samples.append(sample)
        runner._aggregated_skip_warning_counts[key] = count + 1
        return
    if count < _skip_warning_detail_limit(runner, reason):
        runner.logger.log_warning(
            warning_message,
            obj=runner.sync,
        )
    else:
        runner._aggregated_skip_warning_suppressed[key] = (
            runner._aggregated_skip_warning_suppressed.get(key, 0) + 1
        )
    runner._aggregated_skip_warning_counts[key] = count + 1


def emit_dependency_skip_issue_summary(runner, model_string):
    """One rolled-up issue when dependency skips for a model exceeded the
    per-model detail cap (the individual rows past the cap were suppressed)."""
    total = runner._dependency_skip_issue_counts.get(model_string, 0)
    limit = runner.DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT
    if total <= limit:
        return
    samples = runner._dependency_skip_issue_samples.get(model_string, [])
    remainder = total - limit
    examples = ", ".join(samples)
    example_str = f" e.g. {examples}" if examples else ""
    remedy = (
        " Enable the parent sync (device types / devices) first. For DLM "
        "hardware notices with the alias-aware device query, use the "
        "'Forward DLM Hardware Notices with NetBox Aliases' map."
    )
    message = (
        f"{total} {model_string} row(s) skipped because their NetBox parent "
        f"is not synced yet ({remainder} beyond the first {limit} shown "
        f"individually){example_str}.{remedy}"
    )
    context = {
        "dependency_skip_summary": True,
        "dependency_skip_count": total,
        "detail_limit": limit,
    }
    from ..models import ForwardIngestionIssue

    existing = ForwardIngestionIssue.objects.filter(
        ingestion=runner.ingestion,
        phase=ForwardIngestionPhaseChoices.SYNC,
        model=model_string,
        exception="ForwardDependencySkipError",
        coalesce_fields__dependency_skip_summary=True,
    ).first()
    if existing is not None:
        existing.message = message
        existing.coalesce_fields = json_safe_value(context)
        existing.save(update_fields=["message", "coalesce_fields"])
        runner.logger.log_warning(f"{model_string}: {message}", obj=runner.ingestion)
        return existing

    return record_issue(
        runner,
        model_string,
        message,
        {},
        exception=ForwardDependencySkipError("dependency-skip-summary"),
        context=context,
        log_level="warning",
    )


def emit_aggregated_skip_warning_summaries(runner, model_string):
    rollup_reasons = getattr(runner, "SKIP_WARNING_ROLLUP_REASONS", frozenset())
    # Rollup reasons: one actionable summary (total + a few examples + remedy).
    for (warning_model, reason), total in sorted(
        runner._aggregated_skip_warning_counts.items()
    ):
        if warning_model != model_string or reason not in rollup_reasons or total <= 0:
            continue
        samples = runner._aggregated_skip_warning_samples.get(
            (warning_model, reason), []
        )
        remainder = total - len(samples)
        examples = ", ".join(samples)
        suffix = f" (+{remainder} more)" if remainder > 0 else ""
        template = ROLLUP_SUMMARY_TEMPLATES.get(reason, _DEFAULT_ROLLUP_TEMPLATE)
        runner.logger.log_warning(
            template.format(
                total=total,
                model=model_string,
                reason=reason,
                examples=examples,
                suffix=suffix,
            ),
            obj=runner.sync,
        )
    # All other reasons: the first-N-then-suppressed-count summary.
    for (warning_model, reason), suppressed_count in sorted(
        runner._aggregated_skip_warning_suppressed.items()
    ):
        if (
            warning_model != model_string
            or suppressed_count <= 0
            or reason in rollup_reasons
        ):
            continue
        runner.logger.log_warning(
            f"Suppressed {suppressed_count} additional {model_string} skip warnings "
            f"for `{reason}` after the first "
            f"{_skip_warning_detail_limit(runner, reason)}.",
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
    last_emit_at,
):
    current_time = time.monotonic()
    if (
        processed_rows == 1
        or processed_rows % PROGRESS_HEARTBEAT_ROW_INTERVAL == 0
        or current_time - last_emit_at >= PROGRESS_HEARTBEAT_SECONDS
    ):
        message = (
            f"{activity_verb} {processed_rows}/{total_rows} rows for {model_string}."
        )
        runner.logger.log_info(message, obj=runner.sync)
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
    log_level="failure",
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
    # Collapse a flood of per-parent dependency-skip rows (each distinct missing
    # device type / device is a unique message, so record_issue's dedup never
    # merges them). Keep the first N as detail, then count the rest into one
    # summary issue emitted by emit_dependency_skip_issue_summary.
    if exception_name == "ForwardDependencySkipError" and not (context or {}).get(
        "dependency_skip_summary"
    ):
        seen = runner._dependency_skip_issue_counts.get(model_string, 0) + 1
        runner._dependency_skip_issue_counts[model_string] = seen
        if seen > runner.DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT:
            samples = runner._dependency_skip_issue_samples.setdefault(model_string, [])
            example = str(
                (context or {}).get("device_type")
                or (context or {}).get("device")
                or ""
            )
            if example and example not in samples and len(samples) < 5:
                samples.append(example)
            if log_level == "info":
                runner.logger.log_info(
                    f"{model_string}: {message}", obj=runner.ingestion
                )
            return None
    context_data = json_safe_value(dict(context or {}))
    defaults_data = json_safe_value(dict(defaults or {}))
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
        raw_data=json_safe_value(row or {}),
        exception=exception_name,
    )
    runner._recorded_issue_ids.add(issue_key)
    if exception is not None and hasattr(exception, "issue_id"):
        exception.issue_id = issue.pk
    if log_level == "info":
        runner.logger.log_info(f"{model_string}: {message}", obj=runner.ingestion)
    elif log_level == "warning":
        runner.logger.log_warning(f"{model_string}: {message}", obj=runner.ingestion)
    else:
        runner.logger.log_failure(f"{model_string}: {message}", obj=runner.ingestion)
    return issue


def apply_model_rows(runner, model_string, rows):
    rows = list(rows)
    total_rows = len(rows)
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
    dependency_lookup_summary = prime_dependency_lookup_caches(
        runner, model_string, rows
    )
    runner.logger.add_dependency_lookup_summary(dependency_lookup_summary)
    dependency_parent_coverage = dependency_parent_coverage_summary(
        runner,
        model_string,
        rows,
    )
    runner.logger.add_dependency_parent_coverage_summary(dependency_parent_coverage)
    if dependency_parent_coverage.get("available"):
        rows = _filter_dependency_parent_coverage_rows(
            model_string,
            rows,
            dependency_parent_coverage,
        )
        _record_dependency_parent_coverage_issue(
            runner,
            model_string,
            dependency_parent_coverage,
        )
    last_emit_at = 0.0
    processed_rows = 0
    for row in rows:
        processed_rows += 1
        pre_row_events = runner.events_clearer.snapshot()
        try:
            with transaction.atomic():
                result = handler(row)
                runner.events_clearer.increment()
            if result == "unchanged":
                runner.logger.increment_statistics(model_string, outcome="unchanged")
            elif result is False:
                runner.logger.increment_statistics(model_string, outcome="skipped")
            else:
                runner.logger.increment_statistics(model_string, outcome="applied")
        except ForwardDependencySkipError as exc:
            runner.events_clearer.restore(pre_row_events)
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
                log_level="info",
            )
        except (ForwardSearchError, ForwardQueryError, ForwardSyncDataError) as exc:
            runner.events_clearer.restore(pre_row_events)
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
            runner.events_clearer.restore(pre_row_events)
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
            runner.events_clearer.restore(pre_row_events)
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
            total_rows=total_rows,
            last_emit_at=last_emit_at,
        )
    runner.logger.log_info(
        f"Finished applying rows for {model_string}.",
        obj=runner.sync,
    )
    emit_aggregated_conflict_warning_summaries(runner, model_string)
    emit_aggregated_skip_warning_summaries(runner, model_string)
    emit_dependency_skip_issue_summary(runner, model_string)
    runner.events_clearer.clear()


def _filter_dependency_parent_coverage_rows(model_string, rows, summary):
    blocked = {
        (str(group.get("parent_field") or ""), str(group.get("parent_name") or ""))
        for group in summary.get("groups") or []
    }
    if not blocked:
        return rows
    filtered_rows = []
    for row in rows:
        if _row_matches_missing_parent(model_string, row, blocked):
            continue
        filtered_rows.append(row)
    return filtered_rows


def _row_matches_missing_parent(model_string, row, blocked):
    if model_string not in {
        "dcim.interface",
        "dcim.macaddress",
        "dcim.cable",
        "dcim.inventoryitem",
        "dcim.module",
        "dcim.virtualchassis",
        "extras.taggeditem",
        "ipam.fhrpgroup",
        "ipam.ipaddress",
        "netbox_peering_manager.peeringsession",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeer",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
    }:
        return False
    for field in ("device", "remote_device"):
        key = (field, str(row.get(field) or "").strip())
        if key in blocked:
            return True
    return False


def _record_dependency_parent_coverage_issue(runner, model_string, summary):
    from ..exceptions import ForwardDependencySkipError

    blocked_row_count = int(summary.get("blocked_row_count") or 0)
    if blocked_row_count <= 0:
        return
    missing_names = [
        f"`{name}`" for name in summary.get("missing_parent_names") or [] if name
    ]
    groups = summary.get("groups") or []
    sample_rows = []
    for group in groups:
        sample_rows.extend(group.get("sample_rows") or [])
    names_text = ", ".join(missing_names) if missing_names else "unknown parent"
    plural = len(groups) != 1
    message = (
        f"Skipping {blocked_row_count} {model_string} row(s) because referenced "
        f"device{'' if not plural else 's'} {names_text} "
        f"{'were' if plural else 'was'} not imported."
    )
    context = {
        "model": model_string,
        "blocked_row_count": blocked_row_count,
        "missing_parent_count": int(summary.get("missing_parent_count") or 0),
        "missing_parent_names": summary.get("missing_parent_names") or [],
        "missing_parent_fields": sorted(
            {str(group.get("parent_field") or "") for group in groups if group}
        ),
        "sample_rows": sample_rows[:5],
    }
    record_issue(
        runner,
        model_string,
        message,
        {
            "model": model_string,
            "blocked_row_count": blocked_row_count,
            "missing_parent_names": summary.get("missing_parent_names") or [],
            "sample_rows": sample_rows[:5],
        },
        exception=ForwardDependencySkipError(
            message,
            model_string=model_string,
            context=context,
            data=sample_rows[0] if sample_rows else {},
        ),
        context=context,
        log_level="info",
    )
    runner.logger.increment_statistics(
        model_string,
        outcome="skipped",
        amount=blocked_row_count,
    )


def delete_model_rows(runner, model_string, rows):
    rows = list(rows)
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
    dependency_lookup_summary = prime_dependency_lookup_caches(
        runner, model_string, rows
    )
    runner.logger.add_dependency_lookup_summary(dependency_lookup_summary)
    last_emit_at = 0.0
    processed_rows = 0
    pending_deleted = 0
    for row in rows:
        processed_rows += 1
        pre_row_events = runner.events_clearer.snapshot()
        try:
            with transaction.atomic():
                deleted = handler(row)
                runner.events_clearer.increment()
            if deleted:
                runner.logger.increment_statistics(model_string, outcome="applied")
                pending_deleted += 1
                if pending_deleted >= PROGRESS_HEARTBEAT_ROW_INTERVAL:
                    _increment_ingestion_delete_totals(runner, pending_deleted)
                    pending_deleted = 0
            else:
                runner.logger.increment_statistics(model_string, outcome="skipped")
        except ForwardDependencySkipError as exc:
            runner.events_clearer.restore(pre_row_events)
            logger.info("Skipped deleting %s row due to dependency", model_string)
            runner.logger.increment_statistics(model_string, outcome="skipped")
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
                context=exc.context,
                defaults=exc.defaults,
                log_level="info",
            )
        except (ForwardSearchError, ForwardQueryError) as exc:
            runner.events_clearer.restore(pre_row_events)
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
            runner.events_clearer.restore(pre_row_events)
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
            runner.events_clearer.restore(pre_row_events)
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
            last_emit_at=last_emit_at,
        )
    _increment_ingestion_delete_totals(runner, pending_deleted)
    runner.logger.log_info(
        f"Finished deleting rows for {model_string}.",
        obj=runner.sync,
    )
    runner.events_clearer.clear()
