import logging
import time
from ipaddress import ip_interface

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from django.db.models import F
from rq.timeouts import JobTimeoutException

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError
from .diagnostics import diagnostic_shape
from .diagnostics import exception_type
from .diagnostics import failure_classifier
from .diagnostics import is_preexisting_rule_rejection
from .diagnostics import structured_failure_diagnosis
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
    "component-claimed-by-another-module": (
        "Skipped {total} {model} row(s) whose module type would create a "
        "component whose name another module on the same device already uses. "
        "NetBox adopts an existing component only when it belongs to no module, "
        "so this one can be neither adopted nor recreated. Examples: "
        "{examples}{suffix}."
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


def dependency_skip_direction(exception) -> str:
    """``protecting`` when a surviving child refuses a delete, else ``missing``.

    The two are opposite conditions with opposite remedies - "nothing to build
    on" wants the parent synced first; "something still needs this" wants the
    child gone first - and every rollup used to word both as the first.
    """
    if getattr(exception, "dependency_is_protecting", False):
        return "protecting"
    return "missing"


def dependency_skip_reason(exception) -> str:
    """The catalogued reason a skip persists with, derived from the exception.

    The vocabulary (`missing-device`, `missing-interface`, ...) existed in
    `record_aggregated_skip_warning`'s callers and never reached the database:
    twenty-four raisers name their dependency model on the exception and one
    marks it protecting, so the reason is derivable from what they already
    say rather than something each raiser has to be taught separately. A
    raiser that names nothing records `dependency-unnamed`, which is honest
    about the gap rather than a guess.
    """
    dependency = str(getattr(exception, "dependency", "") or "")
    if not dependency:
        return "dependency-unnamed"
    short = dependency.rsplit(".", 1)[-1]
    if dependency_skip_direction(exception) == "protecting":
        return f"still-referenced-by-{short}"
    return f"missing-{short}"


def _dependency_skip_directions(runner):
    """Per-model, per-direction counts. Lazily created: not every runner-shaped
    object that records issues declares the attribute."""
    directions = getattr(runner, "_dependency_skip_issue_directions", None)
    if directions is None:
        directions = {}
        try:
            runner._dependency_skip_issue_directions = directions
        except AttributeError:  # pragma: no cover - frozen runner stand-ins
            pass
    return directions


def _record_dependency_skip_direction(runner, model_string, exception):
    buckets = _dependency_skip_directions(runner).setdefault(
        model_string, {"missing": 0, "protecting": 0, "dependencies": {}}
    )
    direction = dependency_skip_direction(exception)
    buckets[direction] += 1
    dependency = str(getattr(exception, "dependency", "") or "")
    if dependency:
        per_direction = buckets["dependencies"].setdefault(direction, [])
        if dependency not in per_direction and len(per_direction) < 5:
            per_direction.append(dependency)


def emit_dependency_skip_issue_summary(runner, model_string):
    """One rolled-up issue when dependency skips for a model exceeded the
    per-model detail cap (the individual rows past the cap were suppressed).

    Worded per DIRECTION. The rollup used to say "their NetBox parent is not
    synced yet" and recommend enabling the parent sync for every skip in the
    model - including a protected-delete skip, where a surviving child is
    refusing the prune and the remedy is the opposite. The per-row path had
    already learned the difference (`dependency_phrase`); the rollup had not.
    """
    total = runner._dependency_skip_issue_counts.get(model_string, 0)
    limit = runner.DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT
    if total <= limit:
        return
    remainder = total - limit
    buckets = _dependency_skip_directions(runner).get(model_string) or {
        "missing": total,
        "protecting": 0,
        "dependencies": {},
    }
    sentences = []
    if buckets["missing"]:
        named = ", ".join(buckets["dependencies"].get("missing", ())) or "a parent"
        sentences.append(
            f"{buckets['missing']} skipped because a NetBox parent is not synced "
            f"yet (waiting on {named}). Enable the parent sync first; for DLM "
            "hardware notices with the alias-aware device query, use the "
            "'Forward DLM Hardware Notices with NetBox Aliases' map."
        )
    if buckets["protecting"]:
        named = ", ".join(buckets["dependencies"].get("protecting", ())) or "a child"
        sentences.append(
            f"{buckets['protecting']} skipped because a surviving NetBox child "
            f"still references a row this run would delete (still referenced by "
            f"{named}). These are refused deletes, not missing parents: the row "
            "is kept until the referencing rows are removed by their own "
            "model's reconciliation."
        )
    message = (
        f"{total} {model_string} row(s) skipped ({remainder} beyond the first "
        f"{limit} shown individually). " + " ".join(sentences)
    )
    context = {
        "dependency_skip_summary": True,
        "dependency_skip_count": total,
        "detail_limit": limit,
        "missing_parent_count": buckets["missing"],
        "protected_delete_count": buckets["protecting"],
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


def dependency_phrase(exception) -> str:
    """How a skip's dependency reads, in the direction it actually points.

    Naming the model alone made two opposite conditions identical. A customer's
    rows recorded `netbox_dlm.softwareversion row processing skipped (...;
    netbox_dlm.inventoryitemsoftware)`, which reads as a missing parent but
    means a surviving child refusing the prune - `inventoryitemsoftware`
    depends on `softwareversion`, not the reverse. "Nothing to build on" and
    "something still needs this" call for opposite responses, so they must not
    share a sentence.
    """
    dependency = str(getattr(exception, "dependency", "") or "")
    if not dependency:
        return ""
    if getattr(exception, "dependency_is_protecting", False):
        return f"still referenced by {dependency}"
    return f"waiting on {dependency}"


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
    dependency_skip_detail_number = None
    is_dependency_skip_summary = bool((context or {}).get("dependency_skip_summary"))
    if (
        exception_name == "ForwardDependencySkipError"
        and not is_dependency_skip_summary
    ):
        seen = runner._dependency_skip_issue_counts.get(model_string, 0) + 1
        runner._dependency_skip_issue_counts[model_string] = seen
        _record_dependency_skip_direction(runner, model_string, exception)
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
                    f"{model_string}: row skipped ({exception_name}).",
                    obj=runner.ingestion,
                )
            return None
        # Redacted diagnostics cannot distinguish different dependency rows.
        # Use the bounded sequence number only for in-memory deduplication.
        dependency_skip_detail_number = seen
    # Schema-level detail about the failure. 2.6.6 added this for merge issues
    # and left the sync phase recording only an exception class name, so a
    # customer's terminating `dcim.module` IntegrityError said "row processing
    # failed (IntegrityError)" and nothing about *which* constraint — the one
    # fact that identifies it. Constraint/table/column and invalid-field names
    # are schema identifiers the plugin itself defines; the key *values* a
    # Postgres DETAIL line embeds are deliberately not captured.
    diagnosis = structured_failure_diagnosis(exception) if exception is not None else {}
    constraint = diagnosis.get("constraint_name") or ""
    rules = diagnosis.get("validation_rules") or []
    unrecognized = diagnosis.get("unrecognized_validation_rules") or []
    invalid_fields = diagnosis.get("invalid_fields") or []
    # The name of the failure is resolved by `failure_classifier`, not composed
    # here. Composing it here is exactly what left `ForwardIngestionIssue` -
    # the row an operator actually reads - recording `(ForwardQueryError)` for
    # a row rejected on its shape while the logger, formatting the same failure
    # through `safe_operation_failure`, said `shape-error`. Schema-level detail
    # is appended to the shared name; it does not replace it.
    named = failure_classifier(exception) if exception is not None else exception_name
    # The field and the rule are different facts, and a rule is now recognised
    # whatever field it names, so they are no longer alternatives - reporting
    # only the field is what left a customer's `untagged_vlan` rejection
    # ambiguous between two unrelated NetBox rules. `__all__` is the absence of
    # a field name rather than one, so it is still never printed.
    named_fields = [field for field in invalid_fields if field != "__all__"]
    field_detail = f"invalid fields {', '.join(named_fields)}" if named_fields else ""
    # The model whose absence — or whose surviving children, on the delete path
    # — caused the skip. A schema identifier, so unlike everything in `context`
    # it can be recorded. Six identical `(ForwardDependencySkipError)` rows told
    # a customer nothing about which parent was missing; a raiser that has not
    # been taught to name one still records exactly what it did before.
    dependency = str(getattr(exception, "dependency", "") or "")
    # Say which way the dependency points. Naming the model alone made two
    # opposite conditions read identically: a customer's DLM rows recorded
    # `netbox_dlm.softwareversion row processing skipped (...;
    # netbox_dlm.inventoryitemsoftware)`, which reads as a missing parent but
    # means a surviving child refusing the prune - `inventoryitemsoftware`
    # depends on `softwareversion`, not the reverse. The reader has no way to
    # tell "nothing to build on" from "something still needs this".
    dependency_detail = dependency_phrase(exception)
    if is_dependency_skip_summary:
        detail = ""
    elif dependency:
        detail = f"{named}; {dependency_detail}"
    elif constraint:
        detail = f"{named}; constraint {constraint}"
    elif rules:
        detail = "; ".join(p for p in (named, field_detail, ", ".join(rules)) if p)
    elif unrecognized:
        # Wording only; value-bearing tokens are masked before they get here.
        detail = "; ".join(
            p for p in (named, field_detail, "; ".join(unrecognized)) if p
        )
    elif field_detail:
        detail = f"{named}; {field_detail}"
    else:
        detail = named
    # "failed" was written for every row regardless of what the caller decided,
    # so a row deliberately skipped read exactly like one that blocks a
    # baseline. The merge recorder says "Recorded and skipped" for its own
    # skips; the sync recorder could not say anything of the kind.
    #
    # NOT keyed on `log_level` alone. The row-oriented handler records a
    # dependency skip at "info" while the bulk engines record theirs at the
    # default, so the same condition would be worded two ways depending on which
    # engine ran — a test asserting `outcome="skipped"` two lines below a
    # message reading "failed" is what surfaced it. A dependency skip is a skip
    # by definition of the exception, whatever level it was logged at.
    outcome_word = (
        "skipped"
        if exception_name == "ForwardDependencySkipError" or log_level != "failure"
        else "failed"
    )
    # Name the NetBox row the failure is about, when the raiser had it in hand.
    # The merge recorder was given this in 2.8.1 and the sync recorder was not,
    # so a customer's five protected-delete skips read as five identical
    # sentences: `dcim.site row processing skipped (...; still referenced by
    # dcim.device).` The model was named, the direction was named, and the one
    # fact needed to go look - which site - was the only thing missing, because
    # every value that would have said so is a name or a slug and is reduced to
    # its key names by `diagnostic_shape` before it persists. A pk is not.
    #
    # Appended, never substituted: a raiser that has no row in hand records
    # exactly the message it recorded before, byte for byte.
    netbox_pk = getattr(exception, "netbox_pk", None) if exception is not None else None
    identity_sentence = f" Affected NetBox row: pk {netbox_pk}." if netbox_pk else ""
    # The last per-row issue before the cap says the cap is here. A panel
    # showing exactly ten rows used to be indistinguishable from one showing
    # ten of many: the rollup issue that carries the rest is a separate row an
    # operator has to know to look for.
    cap_sentence = (
        " Further rows for this model are rolled up into one summary issue."
        if dependency_skip_detail_number is not None
        and dependency_skip_detail_number == runner.DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT
        else ""
    )
    message = (
        message
        if is_dependency_skip_summary
        else (
            f"{model_string} row processing {outcome_word} ({detail})."
            f"{identity_sentence}{cap_sentence}"
        )
    )
    context_data = (
        json_safe_value(context or {})
        if is_dependency_skip_summary
        else diagnostic_shape(dict(context or {}))
    )
    if (
        exception_name == "ForwardDependencySkipError"
        and not is_dependency_skip_summary
    ):
        # The catalogued reason, persisted where the per-row vocabulary never
        # reached before. A schema-derived token, never a value.
        context_data = {
            **context_data,
            "skip_reason": dependency_skip_reason(exception),
            "skip_direction": dependency_skip_direction(exception),
        }
    defaults_data = diagnostic_shape(dict(defaults or {}))
    # Row shape plus the schema-level diagnosis. The keys are disjoint
    # (`type`/`fields` vs `exception_type`/`constraint_name`/...), so this stays
    # readable for anything already consuming the shape.
    raw_data = {**diagnostic_shape(row or {}), **diagnosis}
    if netbox_pk:
        # Structured alongside the sentence, the way the merge recorder stores
        # it, so anything reading issues programmatically does not have to
        # parse English out of `message`.
        raw_data["netbox_pk"] = netbox_pk
    issue_key = (
        runner.ingestion.pk if runner.ingestion else None,
        ForwardIngestionPhaseChoices.SYNC,
        model_string,
        exception_name,
        str(message),
        str(sorted(context_data.items())),
        str(sorted(defaults_data.items())),
        dependency_skip_detail_number,
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
        raw_data=raw_data,
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
    from .row_collapsing import collapse_rows

    rows = list(rows)
    total_rows = len(rows)
    # Before the row count is taken for anything else. Some queries report one
    # row per observation while the apply writes one object per identity, and
    # writing that object once per observation is how a row set that never
    # converges is produced.
    rows = collapse_rows(model_string, rows)
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
            logger.error(
                "Failed applying %s row (%s).",
                model_string,
                exception_type(exc),
            )
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
            logger.error(
                "Failed applying %s row (%s).",
                model_string,
                exception_type(exc),
            )
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
            # A rejection naming a catalogued rule on a field this row does not
            # write is pre-existing state: recorded and skipped, not failed. The
            # writer attaches what it wrote, because nothing about the exception
            # says it. Absent that, the row keeps failing — inferring "not ours"
            # from silence is exactly the wrong default.
            written_fields = getattr(exc, "forward_written_fields", None)
            preexisting = written_fields is not None and is_preexisting_rule_rejection(
                exc, written_fields
            )
            logger.error(
                "Failed applying %s row (%s).",
                model_string,
                exception_type(exc),
            )
            if preexisting:
                runner.logger.increment_statistics(model_string, outcome="skipped")
                record_issue(
                    runner,
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                    log_level="warning",
                )
            else:
                mark_dependency_failed(runner, model_string, row)
                runner.logger.increment_statistics(model_string, outcome="failed")
                record_issue(
                    runner,
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                )
        except JobTimeoutException:
            raise
        except Exception as exc:
            runner.events_clearer.restore(pre_row_events)
            logger.error(
                "Failed applying %s row (%s).",
                model_string,
                exception_type(exc),
            )
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


REFUSED_DELETE_IDENTITIES_KEY = "refused_delete_identities"


def record_refused_delete(runner, model_string, row):
    """Remember a delete this run did NOT perform, so it is not tombstoned.

    The durable workload state is staged before the branch applies and promoted
    at merge, so it records every delete the DELTA computed - not every delete
    that happened. A refused one was therefore written as `delete` in the
    promoted state, `newly_explicit_deletes` skipped it on the next run
    because the state already said `delete`, and nothing ever retried it: the
    row stayed in NetBox, the plugin believed it was gone, and the report went
    quiet. 2.8.9 closed the PROTECT half by never staging those; this closes
    the rest, for a delete refused by any cause at all.

    Identities only - the same canonical identity the state is keyed by, which
    is derived from the model's coalesce fields and carries no free-text values
    beyond them. Held on the runner and persisted with the ingestion, because
    promotion happens in a different transaction at merge time.
    """
    identities = getattr(runner, "_refused_delete_identities", None)
    if identities is None:
        identities = runner._refused_delete_identities = {}
    identities.setdefault(model_string, set()).add(
        _row_identity(runner, model_string, row)
    )


def persist_refused_delete_identities(runner, ingestion):
    """Carry this run's refused deletes on the ingestion, for promotion.

    Sets `snapshot_info` on the instance; the caller saves it alongside its
    own fields. Writes nothing when no delete was refused, so an ordinary run
    leaves the payload untouched.
    """
    identities = getattr(runner, "_refused_delete_identities", None)
    if not identities or ingestion is None:
        return 0
    snapshot_info = dict(getattr(ingestion, "snapshot_info", None) or {})
    recorded = {
        model_string: sorted(values)
        for model_string, values in identities.items()
        if values
    }
    if not recorded:
        return 0
    snapshot_info[REFUSED_DELETE_IDENTITIES_KEY] = recorded
    ingestion.snapshot_info = snapshot_info
    return sum(len(values) for values in recorded.values())


def _row_identity(runner, model_string, row):
    from .workload_state import canonical_row_identity

    coalesce_fields = getattr(runner, "_model_coalesce_fields", {}).get(model_string)
    try:
        return canonical_row_identity(model_string, row, coalesce_fields or [])
    except JobTimeoutException:
        # The worker is being torn down. Swallowing this to record a refusal
        # would let the job look like it finished - the boundary rule every
        # broad `except` in this module observes, and the one a helper added
        # for a diagnostic is most likely to forget.
        raise
    except Exception:  # noqa: BLE001 - an unkeyable row cannot be tombstoned anyway
        return ""


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
                record_refused_delete(runner, model_string, row)
        except ForwardDependencySkipError as exc:
            runner.events_clearer.restore(pre_row_events)
            logger.info("Skipped deleting %s row due to dependency", model_string)
            runner.logger.increment_statistics(model_string, outcome="skipped")
            record_refused_delete(runner, model_string, row)
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
            logger.error(
                "Failed deleting %s row (%s).",
                model_string,
                exception_type(exc),
            )
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_refused_delete(runner, model_string, row)
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
            logger.error(
                "Failed deleting %s row (%s).",
                model_string,
                exception_type(exc),
            )
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_refused_delete(runner, model_string, row)
            record_issue(
                runner,
                model_string,
                str(exc),
                row,
                exception=exc,
            )
        except JobTimeoutException:
            raise
        except Exception as exc:
            runner.events_clearer.restore(pre_row_events)
            logger.error(
                "Failed deleting %s row (%s).",
                model_string,
                exception_type(exc),
            )
            runner.logger.increment_statistics(model_string, outcome="failed")
            record_refused_delete(runner, model_string, row)
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
