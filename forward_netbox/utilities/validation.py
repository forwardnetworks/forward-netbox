from core.exceptions import SyncError
from django.utils import timezone

from ..choices import ForwardDriftPolicyBaselineChoices
from ..choices import ForwardValidationStatusChoices
from ..exceptions import ForwardSyncError
from .diagnostics import safe_operation_failure
from .query_fetch import ForwardQueryFetcher
from .sync_primitives import DEPENDENCY_PARENT_DEVICE_MODELS


FOUNDATIONAL_DEVICE_MODELS = ("dcim.platform", "dcim.devicetype")

# How far a model's row count may fall below the last successful ingestion
# before the run is refused. Deliberately loose: the guard exists to stop a
# silent collapse, not to audit ordinary churn. On a multi-thousand-device
# estate the routine causes of shrinkage - a decommissioned rack, a handful of
# devices Forward failed to collect, one retired site - move the count by single
# digit percentages, while the failure this stands in front of (a narrowed
# `where` clause on an unpinned query head, or an emptied collection region) is
# a step change of tens of percent. A tighter default would fire on ordinary
# weeks, and a guard that fires on ordinary weeks gets turned off.
DEFAULT_MAX_ROW_SHRINK_PERCENT = 30

# A model must also lose at least this many rows outright. Without it, small
# reference models trip constantly on arithmetic alone: a `dcim.manufacturer`
# map going from 12 rows to 8 is a 33% drop and means nothing. Below this many
# rows the blast radius is small enough for the per-model reporting and the
# staged-branch review to be the right instruments.
MIN_ROW_SHRINK_ROWS = 20

# Stable marker on every row-count reason. Operators read it, and the one-time
# acceptance below matches on it, so it must not be reworded casually.
ROW_SHRINK_REASON_PREFIX = "Row-count drop:"


def _comparable_row_counts(model_results):
    """Total full-execution rows per model, for models that can be compared.

    Only full execution is comparable. A diff run's `row_count` is the number of
    changed rows, not the size of the row set, so comparing it against a full
    run's count would read a quiet snapshot as a collapse. A model that reported
    any failure is excluded too: its rows are missing because the fetch broke,
    which is already a loud, separately reported failure, and counting it here
    would only bury that under a second one.

    Returns `(totals_by_model, scope_fingerprints_by_model)`.
    """
    totals: dict[str, int] = {}
    scopes: dict[str, set[str]] = {}
    excluded: set[str] = set()
    for result in model_results or []:
        model_string = str(result.get("model") or "")
        if not model_string:
            continue
        if int(result.get("failure_count") or 0):
            excluded.add(model_string)
            continue
        if str(result.get("sync_mode") or "") != "full":
            excluded.add(model_string)
            continue
        totals[model_string] = totals.get(model_string, 0) + int(
            result.get("row_count") or 0
        )
        fingerprint = str(result.get("scope_config_fingerprint") or "")
        if fingerprint:
            scopes.setdefault(model_string, set()).add(fingerprint)
    return (
        {
            model_string: count
            for model_string, count in totals.items()
            if model_string not in excluded
        },
        scopes,
    )


def _scope_configuration_changed(before, after, model_string):
    """Whether the operator's own scope configuration moved under this model.

    `scope_config_fingerprint` covers the sync's declared scope - include and
    exclude tags, the match mode, out-of-scope pruning, the endpoint and
    device-tag toggles - and nothing derived from the snapshot. When it changes,
    a smaller row set is the operator getting what they asked for, so there is
    nothing to refuse. Membership fingerprints are deliberately not consulted:
    they move whenever the network moves, which is the very thing being
    measured.
    """
    before_fingerprints = before.get(model_string) or set()
    after_fingerprints = after.get(model_string) or set()
    if not before_fingerprints or not after_fingerprints:
        # One side predates the fingerprint or ran without a contract. Unknown
        # is not evidence of a change, so the comparison stands.
        return False
    return before_fingerprints != after_fingerprints


def row_shrink_findings(
    *,
    current_results,
    baseline_results,
    enabled_models,
    max_shrink_percent,
    min_shrink_rows=MIN_ROW_SHRINK_ROWS,
):
    """Models whose fetched row count fell too far below the last baseline.

    Pure and side-effect free so the thresholds can be exercised directly. A
    model is compared only when both runs executed it in full, neither reported
    a failure, the operator's scope configuration is unchanged, and the baseline
    actually had rows. Every other case yields nothing: growth, a first run, a
    newly enabled model, a model dropped from the sync.
    """
    current_counts, current_scopes = _comparable_row_counts(current_results)
    baseline_counts, baseline_scopes = _comparable_row_counts(baseline_results)
    enabled = set(enabled_models or ())

    findings = []
    for model_string in sorted(set(current_counts) & set(baseline_counts) & enabled):
        baseline_rows = baseline_counts[model_string]
        current_rows = current_counts[model_string]
        if baseline_rows <= 0:
            continue
        dropped = baseline_rows - current_rows
        if dropped < min_shrink_rows:
            continue
        if _scope_configuration_changed(
            baseline_scopes,
            current_scopes,
            model_string,
        ):
            continue
        dropped_percent = dropped / baseline_rows * 100
        if dropped_percent <= max_shrink_percent:
            continue
        findings.append(
            {
                "model": model_string,
                "baseline_rows": baseline_rows,
                "current_rows": current_rows,
                "dropped_rows": dropped,
                "dropped_percent": round(dropped_percent, 1),
            }
        )
    return findings


def row_shrink_reason(finding, *, max_shrink_percent):
    """The operator-facing sentence for one finding.

    Names the model, both counts, why it matters, and the way through.
    """
    return (
        f"{ROW_SHRINK_REASON_PREFIX} `{finding['model']}` returned "
        f"{finding['current_rows']} row(s), down from {finding['baseline_rows']} "
        f"in the last successful ingestion - a drop of {finding['dropped_rows']} "
        f"({finding['dropped_percent']}%), past the {max_shrink_percent}% limit. "
        "Rows the query no longer returns are reconciled as deletions, so this "
        "run is refused before anything is staged. If the shrinkage is real - "
        "devices decommissioned, a site retired, scope narrowed - force-allow "
        "this validation run to accept it once, and the next run will compare "
        "against the new, smaller baseline. If it is not, the query behind one "
        "of this model's maps is returning less than it used to; pin a commit "
        "on the map, or re-publish the bundled query."
    )


class ForwardValidationRunner:
    def __init__(self, sync, client, logger_, *, job=None):
        self.sync = sync
        self.client = client
        self.logger = logger_
        self.job = job

    def run_query_validation(self):
        from ..models import ForwardValidationRun

        validation_run = self._create_run()
        try:
            fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
            context = fetcher.resolve_context()
            workloads = fetcher.fetch_workloads(context, include_diagnostics=True)
            return self.record_plan_validation(
                context.as_dict(),
                workloads,
                [result.as_dict() for result in fetcher.model_results],
                validation_run=validation_run,
                raise_on_block=False,
            )
        except Exception as exc:
            ForwardValidationRun.objects.filter(pk=validation_run.pk).update(
                status=ForwardValidationStatusChoices.FAILED,
                allowed=False,
                completed=timezone.now(),
                blocking_reasons=[safe_operation_failure("Forward validation", exc)],
            )
            raise

    def record_plan_validation(
        self,
        context,
        plan,
        model_results,
        *,
        validation_run=None,
        raise_on_block=True,
    ):
        validation_run = validation_run or self._create_run()
        policy = self.sync.drift_policy
        blocking_reasons = self._blocking_reasons(
            context,
            plan,
            model_results,
            policy,
            validation_run=validation_run,
        )
        allowed = not blocking_reasons
        status = (
            ForwardValidationStatusChoices.PASSED
            if allowed
            else ForwardValidationStatusChoices.BLOCKED
        )
        drift_summary = self._drift_summary(plan, model_results)

        validation_run.policy = policy
        validation_run.status = status
        validation_run.allowed = allowed
        validation_run.snapshot_selector = context["snapshot_selector"]
        validation_run.snapshot_id = context["snapshot_id"]
        validation_run.baseline_snapshot_id = self._baseline_snapshot_id(
            model_results,
            policy,
        )
        validation_run.snapshot_info = context["snapshot_info"]
        validation_run.snapshot_metrics = context["snapshot_metrics"]
        validation_run.model_results = list(model_results or [])
        validation_run.drift_summary = drift_summary
        validation_run.blocking_reasons = blocking_reasons
        validation_run.completed = timezone.now()
        validation_run.save(
            update_fields=[
                "policy",
                "status",
                "allowed",
                "snapshot_selector",
                "snapshot_id",
                "baseline_snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
                "model_results",
                "drift_summary",
                "blocking_reasons",
                "completed",
            ]
        )
        if not allowed and raise_on_block:
            raise ForwardSyncError(
                "Forward validation blocked sync: " + "; ".join(blocking_reasons)
            )
        return validation_run

    def _create_run(self):
        from ..models import ForwardValidationRun

        return ForwardValidationRun.objects.create(
            sync=self.sync,
            policy=self.sync.drift_policy,
            job=self.job,
            status=ForwardValidationStatusChoices.RUNNING,
            started=timezone.now(),
        )

    def _blocking_reasons(
        self,
        context,
        plan,
        model_results,
        policy,
        *,
        validation_run=None,
    ):
        if self._forced_validation_override_applies(
            context, policy, validation_run=validation_run
        ):
            return []

        reasons = []
        reasons.extend(self._required_query_failure_reasons(model_results))
        # Deliberately above the policy early return. The row-count floor is on
        # by default and applies to a sync with no drift policy at all, because
        # it replaces a source-hash check that used to run unconditionally. A
        # policy that exists can tune or disable it; the absence of a policy
        # must not.
        reasons.extend(
            self._row_shrink_reasons(
                model_results,
                policy,
                validation_run=validation_run,
            )
        )

        if policy is None or not policy.enabled:
            return reasons

        if policy.require_processed_snapshot and not self._snapshot_is_processed(
            context
        ):
            reasons.append("Target snapshot is not processed.")

        if policy.block_on_query_errors:
            failures = sum(
                int(result.get("failure_count") or 0) for result in model_results
            )
            if failures:
                reasons.append(f"{failures} query failures were reported.")

        if policy.block_on_zero_rows:
            counts_by_model = {}
            for result in model_results:
                counts_by_model.setdefault(result.get("model"), 0)
                counts_by_model[result.get("model")] += int(
                    result.get("row_count") or 0
                ) + int(result.get("delete_count") or 0)
            empty_models = [
                model_string
                for model_string in self.sync.get_model_strings()
                if counts_by_model.get(model_string, 0) == 0
            ]
            if empty_models:
                reasons.append(
                    "No rows were returned for enabled models: "
                    + ", ".join(sorted(empty_models))
                    + "."
                )

        total_deletes = sum(
            int(result.get("delete_count") or 0) for result in model_results
        )
        if (
            policy.max_deleted_objects is not None
            and total_deletes > policy.max_deleted_objects
        ):
            reasons.append(
                f"Delete count {total_deletes} exceeds policy limit {policy.max_deleted_objects}."
            )
        total_changes = sum(
            int(result.get("row_count") or 0) + int(result.get("delete_count") or 0)
            for result in model_results
        )
        if (
            policy.max_deleted_percent is not None
            and total_changes
            and (total_deletes / total_changes * 100) > policy.max_deleted_percent
        ):
            reasons.append(
                f"Delete percentage exceeds policy limit {policy.max_deleted_percent}%."
            )
        return reasons

    def _row_shrink_reasons(self, model_results, policy, *, validation_run=None):
        """Refuse a run whose models came back materially smaller than baseline.

        This is the detection for a query head that is parameter-compatible and
        shape-compatible but returns a narrower row set. Parameters validate,
        row shape validates, the sync reports success, and the rows that are no
        longer returned are reconciled as deletions. Nothing else in the
        pipeline sees it, so it is measured here, against the only trustworthy
        reference the plugin holds: what the same models returned in the last
        ingestion that actually promoted a baseline.

        Needs no Forward call.
        """
        if policy is not None and not policy.enabled:
            return []
        if policy is not None and not policy.block_on_row_shrink:
            return []
        if (
            policy is not None
            and policy.baseline_mode == ForwardDriftPolicyBaselineChoices.NONE
        ):
            # The operator has said this sync has no baseline. There is nothing
            # to compare against by their own configuration.
            return []
        max_shrink_percent = (
            policy.max_row_shrink_percent
            if policy is not None
            else DEFAULT_MAX_ROW_SHRINK_PERCENT
        )

        baseline = self.sync.latest_baseline_ingestion()
        if baseline is None:
            # First run, or no run has ever promoted a baseline. Nothing to
            # compare against, and a first run must never be blocked.
            return []

        findings = row_shrink_findings(
            current_results=model_results,
            baseline_results=list(baseline.model_results or []),
            enabled_models=self.sync.get_model_strings(),
            max_shrink_percent=max_shrink_percent,
        )
        if not findings:
            return []
        if self._row_shrink_already_accepted(baseline, validation_run):
            return []
        return [
            row_shrink_reason(finding, max_shrink_percent=max_shrink_percent)
            for finding in findings
        ]

    def _row_shrink_already_accepted(self, baseline, validation_run):
        """Whether an operator has already force-allowed this exact shrinkage.

        Scoped to the baseline, not to the snapshot. What the operator accepted
        is "smaller than baseline N", and that stays true for as long as N is
        the baseline - which matters because a snapshot-scoped acceptance would
        lapse the moment Forward processed the next snapshot and put the
        operator in a loop they would escape by disabling the guard. Once a run
        gets through and promotes a new baseline, the acceptance stops applying
        on its own, because the comparison is then against the smaller count
        the operator accepted.

        `_forced_validation_override_applies` is not reused here: it reads
        `sync.latest_validation_run`, which by this point is the run being
        recorded, so it cannot see the previous run's override.
        """
        baseline_snapshot_id = str(getattr(baseline, "snapshot_id", "") or "")
        if not baseline_snapshot_id:
            return False
        runs = self.sync.validation_runs.filter(override_applied=True)
        pk = getattr(validation_run, "pk", None)
        if pk is not None:
            runs = runs.exclude(pk=pk)
        previous = runs.order_by("-pk").first()
        if previous is None:
            return False
        if str(previous.baseline_snapshot_id or "") != baseline_snapshot_id:
            return False
        return any(
            str(reason or "").startswith(ROW_SHRINK_REASON_PREFIX)
            for reason in previous.override_blocking_reasons or []
        )

    def _required_query_failure_reasons(self, model_results):
        enabled_models = set(self.sync.get_model_strings())
        failed_models = {
            str(result.get("model") or "")
            for result in model_results or []
            if int(result.get("failure_count") or 0) > 0
        }
        reasons = []

        enabled_parent_device_models = sorted(
            model
            for model in DEPENDENCY_PARENT_DEVICE_MODELS
            if model in enabled_models
        )
        if "dcim.device" in failed_models and enabled_parent_device_models:
            reasons.append(
                "`dcim.device` query failed while enabled child models depend on "
                "device coverage: " + ", ".join(enabled_parent_device_models) + "."
            )

        failed_foundational_models = sorted(
            model
            for model in FOUNDATIONAL_DEVICE_MODELS
            if model in failed_models and model in enabled_models
        )
        if "dcim.device" in enabled_models and failed_foundational_models:
            reasons.append(
                "Foundational device metadata query failed before `dcim.device`: "
                + ", ".join(failed_foundational_models)
                + "."
            )

        if reasons and self._diff_fallback_is_require_diff():
            reasons.append(
                "These query failures blocked the sync because its diff fallback "
                "mode is `Require diff`: a diff run that cannot fetch rows is treated "
                "as a hard failure instead of retrying a full fetch. Set the sync's "
                "diff fallback mode to `Allow full fallback` to recover automatically, "
                "and use Publish Bundled Queries on the Health page if built-in "
                "query maps drifted."
            )

        return reasons

    def _diff_fallback_is_require_diff(self):
        from ..choices import ForwardDiffFallbackModeChoices

        parameters = dict(getattr(self.sync, "parameters", {}) or {})
        configured = str(parameters.get("diff_fallback_mode") or "").strip()
        return configured == ForwardDiffFallbackModeChoices.REQUIRE_DIFF

    def _forced_validation_override_applies(
        self, context, policy, *, validation_run=None
    ):
        """Whether an operator's force-allow carries forward to this run.

        Must resolve against the PREVIOUS run, excluding the one being recorded.
        It read `sync.latest_validation_run`, and on the sync path
        `record_plan_validation` creates the new run BEFORE blocking reasons are
        evaluated - so the lookup returned the run being recorded, whose
        `override_applied` is always False. The override could therefore never
        fire on the only path that matters: an operator force-allowed a blocked
        run and the same reason blocked the next one, with nothing to say why.

        `_row_shrink_already_accepted` had to work around this with its own
        previous-run lookup, and its docstring says so. This is the same
        exclusion, applied where it belonged.

        The old coverage called `_blocking_reasons` directly with no current
        run, so it exercised the branch that still worked and passed straight
        over the dead one.
        """
        previous = self._previous_override_run(validation_run)
        if previous is None:
            return False
        if policy is None or previous.policy_id != getattr(policy, "pk", None):
            return False
        return previous.snapshot_selector == context.get(
            "snapshot_selector"
        ) and previous.snapshot_id == context.get("snapshot_id")

    def _previous_override_run(self, validation_run=None):
        """The most recent force-allowed run that is not the one being recorded."""
        runs = self.sync.validation_runs.filter(override_applied=True)
        pk = getattr(validation_run, "pk", None)
        if pk is not None:
            runs = runs.exclude(pk=pk)
        return runs.order_by("-pk").first()

    def _snapshot_is_processed(self, context):
        info = context.get("snapshot_info") or {}
        metrics = context.get("snapshot_metrics") or {}
        state = (
            info.get("state")
            or info.get("snapshotState")
            or metrics.get("snapshotState")
            or ""
        )
        return str(state).upper() == "PROCESSED"

    def _baseline_snapshot_id(self, model_results, policy):
        if (
            policy is not None
            and policy.baseline_mode == ForwardDriftPolicyBaselineChoices.NONE
        ):
            return ""
        for result in model_results:
            baseline_snapshot_id = result.get("baseline_snapshot_id") or ""
            if baseline_snapshot_id:
                return baseline_snapshot_id
        baseline = self.sync.latest_baseline_ingestion()
        return baseline.snapshot_id if baseline else ""

    def _drift_summary(self, plan, model_results):
        by_model = {}
        for result in model_results:
            model_string = result.get("model") or ""
            model_summary = by_model.setdefault(
                model_string,
                {
                    "row_count": 0,
                    "delete_count": 0,
                    "failure_count": 0,
                    "runtime_ms": 0,
                },
            )
            model_summary["row_count"] += int(result.get("row_count") or 0)
            model_summary["delete_count"] += int(result.get("delete_count") or 0)
            model_summary["failure_count"] += int(result.get("failure_count") or 0)
            model_summary["runtime_ms"] += float(result.get("runtime_ms") or 0)

        return {
            "model_count": len([model for model in by_model if model]),
            "branch_count": len(plan or []),
            "total_rows": sum(item["row_count"] for item in by_model.values()),
            "total_deletes": sum(item["delete_count"] for item in by_model.values()),
            "total_failures": sum(item["failure_count"] for item in by_model.values()),
            "models": by_model,
        }


def force_allow_validation_run(validation_run, *, user, reason):
    reason = str(reason or "").strip()
    if not reason:
        raise SyncError("Provide a force-allow reason before overriding validation.")
    if not validation_run.blocking_reasons:
        raise SyncError("Only blocked validation runs can be force-allowed.")
    validation_run.override_applied = True
    validation_run.allowed = True
    validation_run.status = ForwardValidationStatusChoices.PASSED
    validation_run.override_user = user
    validation_run.override_reason = reason
    validation_run.override_blocking_reasons = list(
        validation_run.blocking_reasons or []
    )
    validation_run.override_at = timezone.now()
    validation_run.save(
        update_fields=[
            "override_applied",
            "allowed",
            "status",
            "override_user",
            "override_reason",
            "override_blocking_reasons",
            "override_at",
        ]
    )
    return validation_run
