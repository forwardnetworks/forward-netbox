# Validating a change against Forward: policies pick the rules, engines answer
# them from the change's workspace, and a score summarises the answers.
#
# Three engines, all Forward's: `nqe` runs a bound org query and applies an
# expectation, `check` reads Forward's own intent checks on a snapshot, and
# `predict` always skips: a prediction is advisory and never a result. A result is a
# shape and a count, never a row; a rule that could not be answered is an ERROR or
# a SKIP and is never counted as a pass. Everything that talks to Forward goes
# through an `api` object (the SDK underneath), so the rules are tested without a
# network.
from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from .actions import _journal
from .choices import ForwardChangeStateChoices as State
from .choices import ForwardCriterionExpectationChoices as Expect
from .choices import ForwardValidationEngineChoices as Engine
from .choices import ForwardValidationResultStatusChoices as Status
from .choices import ForwardValidationRunStatusChoices as RunStatus
from .choices import ForwardValidationSeverityChoices as Severity
from .choices import ForwardValidationTriggerChoices as Trigger
from .workspace import _open_api

# A run that has been "running" this long died with its worker; it must not block
# the next one forever.
RUN_STALE_AFTER = timedelta(minutes=30)

DEVICE_COLUMNS = ("device", "deviceName", "device_name")
NO_WORKSPACE = (
    "This change has no Forward workspace. Validation runs in the change's "
    "workspace, never against the whole network; create or adopt one first."
)


@dataclass(frozen=True)
class ValidationOutcome:
    ok: bool
    message: str = ""
    run_id: int | None = None
    reasons: tuple = field(default_factory=tuple)

    def __bool__(self):
        return self.ok


def _refuse(message):
    return ValidationOutcome(False, message)


# ---- which policies apply ---------------------------------------------------


def _region_ids(regions):
    ids = set()
    for region in regions:
        ids.update(
            region.get_descendants(include_self=True).values_list("pk", flat=True)
        )
    return ids


def _scope_matches(policy, devices):
    """Every kind the policy names must be matched by some device in the change."""
    kinds = (
        (
            list(policy.sites.all()),
            lambda d, ids: d.site_id in ids,
            lambda x: {s.pk for s in x},
        ),
        (
            list(policy.regions.all()),
            lambda d, ids: d.site.region_id in ids,
            _region_ids,
        ),
        (
            list(policy.site_groups.all()),
            lambda d, ids: d.site.group_id in ids,
            lambda x: {g.pk for g in x},
        ),
        (
            list(policy.device_roles.all()),
            lambda d, ids: d.role_id in ids,
            lambda x: {r.pk for r in x},
        ),
        (
            list(policy.platforms.all()),
            lambda d, ids: d.platform_id in ids,
            lambda x: {p.pk for p in x},
        ),
        (
            list(policy.scope_tags.all()),
            lambda d, ids: bool(ids & {t.pk for t in d.tags.all()}),
            lambda x: {t.pk for t in x},
        ),
    )
    for entries, test, to_ids in kinds:
        if not entries:
            continue
        ids = to_ids(entries)
        if not any(test(device, ids) for device in devices):
            return False
    return True


def policies_for(change):
    """Enabled validation policies that apply to the change, by its devices."""
    from ..models import ForwardChangeValidationPolicy

    devices = [
        row.device
        for row in change.devices.select_related(
            "device__site__region",
            "device__site__group",
            "device__role",
            "device__platform",
        )
    ]
    return [
        policy
        for policy in ForwardChangeValidationPolicy.objects.filter(
            enabled=True
        ).prefetch_related(
            "rules",
            "sites",
            "regions",
            "site_groups",
            "device_roles",
            "platforms",
            "scope_tags",
        )
        if _scope_matches(policy, devices)
    ]


# ---- engines ----------------------------------------------------------------


@dataclass
class _Result:
    check_name: str
    status: str
    message: str = ""
    device_name: str = ""
    row_count: int = 0


def _nqe(rule, ctx):
    params = rule.parameters or {}
    path = (params.get("query_path") or "").strip()
    expectation = params.get("expectation") or Expect.NO_ROWS
    if not path:
        return [_Result(rule.name, Status.ERROR, "The rule has no query_path.")]
    if expectation not in (Expect.NO_ROWS, Expect.SOME_ROWS):
        return [
            _Result(rule.name, Status.ERROR, f"Unknown expectation {expectation!r}.")
        ]
    query_id, commit_id = ctx.api.resolve_query(path, params.get("commit_id"))
    if not query_id or not commit_id:
        return [
            _Result(
                rule.name,
                Status.ERROR,
                "Forward resolved the path but returned no query id and commit pair.",
            )
        ]
    rows = ctx.api.run_query(query_id, commit_id, ctx.network_id, ctx.snapshot_id) or []
    if expectation == Expect.SOME_ROWS:
        if rows:
            return [
                _Result(
                    rule.name, Status.PASS, f"{len(rows)} row(s).", row_count=len(rows)
                )
            ]
        return [_Result(rule.name, Status.FAIL, "The query returned no rows.")]
    if not rows:
        return [_Result(rule.name, Status.PASS, "No rows returned.")]
    per_device = {}
    for row in rows:
        name = ""
        if isinstance(row, dict):
            for key in DEVICE_COLUMNS:
                if row.get(key):
                    name = str(row[key])
                    break
        per_device[name] = per_device.get(name, 0) + 1
    results = []
    for name, count in sorted(per_device.items()):
        results.append(
            _Result(
                rule.name,
                Status.FAIL,
                f"{count} row(s) violate the expectation.",
                device_name=name,
                row_count=count,
            )
        )
    return results


_CHECK_STATUS = {
    "PASS": Status.PASS,
    "FAIL": Status.FAIL,
    "ERROR": Status.ERROR,
    "TIMEOUT": Status.ERROR,
}


def _check(rule, ctx):
    params = rule.parameters or {}
    names = {str(n) for n in params.get("check_names") or []}
    types = {str(t).lower() for t in params.get("check_types") or []}
    checks = ctx.api.checks(ctx.snapshot_id) or []
    chosen = [
        c
        for c in checks
        if (not names or c["name"] in names)
        and (not types or str(c["type"]).lower() in types)
    ]
    results = []
    # A named check Forward does not have is an unanswered rule, not a smaller one.
    for missing in sorted(names - {c["name"] for c in checks}):
        results.append(
            _Result(
                missing,
                Status.ERROR,
                "Forward has no check with this name on the snapshot (checks are "
                "copied from the network when a workspace is created).",
            )
        )
    if not chosen:
        if results:
            return results
        return [
            _Result(
                rule.name,
                Status.SKIP,
                "No Forward checks matched on this snapshot (the workspace "
                f"lists {len(checks)} check(s)). Checks are copied from the "
                "network when a workspace is created.",
            )
        ]
    for check in chosen:
        status = _CHECK_STATUS.get(str(check["status"]).upper(), Status.SKIP)
        if status == Status.PASS:
            message = "Passed."
        elif status == Status.FAIL:
            message = f"{check['violations']} violation(s)."
        elif status == Status.ERROR:
            message = f"Forward could not evaluate this check ({check['status']})."
        else:
            message = f"Not evaluated yet ({check['status'] or 'no result'})."
        results.append(
            _Result(
                check["name"] or rule.name,
                status,
                message,
                row_count=check["violations"] if status == Status.FAIL else 0,
            )
        )
    return results


def _predict(rule, ctx):
    # Stays SKIP. A prediction is advisory (see predict.py): wiring it to a result
    # here would put it in the approval gate, which reads validation results.
    return [
        _Result(
            rule.name,
            Status.SKIP,
            "Forward Predict is advisory and is not evaluated as a validation "
            "result. Any prediction is shown on the change page.",
        )
    ]


ENGINES = {Engine.NQE: _nqe, Engine.CHECK: _check, Engine.PREDICT: _predict}


@dataclass
class _Context:
    api: object
    network_id: str
    snapshot_id: str


# ---- scoring ----------------------------------------------------------------


def score_results(results):
    """100 minus a penalty per failing or erroring result, by rule severity.

    Nothing measured gives no score at all (None): a run in which every rule was
    skipped must not read as a perfect one.
    """
    measured = [r for r in results if r.status != Status.SKIP]
    if not measured:
        return None
    penalty = 0
    for r in measured:
        if r.status in (Status.FAIL, Status.ERROR):
            penalty += Severity.PENALTY.get(r.severity, 0)
        elif r.status == Status.WARNING:
            penalty += Severity.PENALTY[Severity.WARNING]
    return max(0, 100 - penalty)


# ---- the run ----------------------------------------------------------------


def _pick_snapshot(change):
    """The snapshot to validate on: the after snapshot once there is one."""
    if change.after_snapshot_id and change.state in (
        State.APPLIED,
        State.COLLECTED,
        State.VERIFIED_PROCEED,
        State.VERIFIED_HOLD,
    ):
        return change.after_snapshot_id
    return change.before_snapshot_id


def start_run(change, user, trigger=Trigger.MANUAL, policy=None):
    """Create a pending run, or say why one cannot start.

    Returns ``(run, None)`` or ``(None, reason)``. The change is locked so two
    clicks cannot start two runs, and a run that is still live blocks another.
    """
    from ..models import ForwardChange
    from ..models import ForwardChangeValidationRun

    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().filter(pk=change.pk).first()
        if locked is None:
            return None, "The change no longer exists."
        if not locked.workspace_network_id:
            return None, NO_WORKSPACE
        snapshot = _pick_snapshot(locked)
        if not snapshot:
            return None, "The change has no snapshot yet; refresh its snapshots first."
        if policy is not None:
            # A run of one policy only counts for that policy, and only a policy
            # that is on and applies to this change may be run.
            if not policy.enabled or policy.pk not in {
                p.pk for p in policies_for(locked)
            }:
                return (
                    None,
                    "That policy is not enabled or does not apply to this change.",
                )
        policies = [policy] if policy is not None else policies_for(locked)
        rules = [rule for p in policies for rule in p.rules.filter(enabled=True)]
        if not rules:
            return None, "No enabled validation rules apply to this change."
        cutoff = timezone.now() - RUN_STALE_AFTER
        # A run whose job never ran (or died) is closed as failed, so the list does
        # not show it pending forever.
        locked.validation_runs.filter(
            status__in=(RunStatus.PENDING, RunStatus.RUNNING), created__lt=cutoff
        ).update(
            status=RunStatus.FAILED,
            error="The run never completed (its job did not finish in time).",
            completed_at=timezone.now(),
        )
        live = locked.validation_runs.filter(
            status__in=(RunStatus.PENDING, RunStatus.RUNNING)
        ).exists()
        if live:
            return None, "A validation run is already in progress for this change."
        run = ForwardChangeValidationRun.objects.create(
            change=locked,
            policy=policy,
            trigger=trigger,
            started_by=user,
            snapshot_id=snapshot,
            before_snapshot_id=locked.before_snapshot_id,
            after_snapshot_id=locked.after_snapshot_id,
        )
        _journal(locked, user, f"Validation run {run.pk} started ({trigger}).")
    return run, None


def execute_run(run, *, api=None):
    """Run every rule and record the results and the score. Never raises."""
    from ..models import ForwardChangeValidationResult

    change = run.change
    # Only a pending run runs: a job delivered twice must not add a second set of
    # results to the same run.
    claimed = (
        type(run)
        .objects.filter(pk=run.pk, status=RunStatus.PENDING)
        .update(status=RunStatus.RUNNING, started_at=timezone.now())
    )
    if not claimed:
        run.refresh_from_db()
        return run
    run.refresh_from_db()
    try:
        api, error = _open_api(change, api)
        if error:
            raise RuntimeError(error)
        ctx = _Context(api, change.workspace_network_id, run.snapshot_id)
        policies = [run.policy] if run.policy_id else policies_for(change)
        rules = [r for p in policies for r in p.rules.filter(enabled=True)]
        rows, scored = [], []
        for rule in rules:
            try:
                answered = ENGINES[rule.engine](rule, ctx)
            except Exception as exc:  # noqa: BLE001 - one rule never sinks the run
                answered = [
                    _Result(
                        rule.name,
                        Status.ERROR,
                        f"{type(exc).__name__}: {str(exc)[:300]}",
                    )
                ]
            for item in answered:
                rows.append(
                    ForwardChangeValidationResult(
                        run=run,
                        rule=rule,
                        engine=rule.engine,
                        category=rule.category,
                        check_name=item.check_name[:255],
                        device_name=item.device_name[:255],
                        status=item.status,
                        severity=rule.severity,
                        message=item.message,
                        row_count=item.row_count,
                    )
                )
                scored.append(_Scored(item.status, rule.severity))
        ForwardChangeValidationResult.objects.bulk_create(rows)
        counts = {}
        for r in rows:
            counts[r.status] = counts.get(r.status, 0) + 1
        run.score = score_results(scored)
        run.summary = {"counts": counts, "rules": len(rules)}
        run.status = RunStatus.COMPLETED
    except Exception as exc:  # noqa: BLE001 - recorded on the run
        run.status = RunStatus.FAILED
        run.error = f"{type(exc).__name__}: {str(exc)[:500]}"
    run.completed_at = timezone.now()
    run.save(update_fields=["status", "score", "summary", "error", "completed_at"])
    from .events import VALIDATION_COMPLETED
    from .events import announce

    announce(
        run.change,
        VALIDATION_COMPLETED,
        run.started_by,
        # Whoever started it in the background wants to hear it finished.
        notify_actor=True,
        run_id=run.pk,
        run_status=run.status,
        score=run.score,
    )
    return run


@dataclass
class _Scored:
    status: str
    severity: str


def run_validation(change, user, trigger=Trigger.MANUAL, policy=None, *, api=None):
    """Start and execute a run inline. The web and REST entry points enqueue a job
    that calls the same two steps; tests and the job call this."""
    run, reason = start_run(change, user, trigger, policy)
    if run is None:
        return _refuse(reason)
    execute_run(run, api=api)
    run.refresh_from_db()
    if run.status == RunStatus.FAILED:
        return ValidationOutcome(False, run.error, run.pk)
    return ValidationOutcome(
        True,
        f"Score {run.score}" if run.score is not None else "Nothing was measured.",
        run.pk,
    )


def run_is_current(run, change):
    """A run counts only against the snapshots the change is pinned to now."""
    return (
        run.status == RunStatus.COMPLETED
        and run.before_snapshot_id == change.before_snapshot_id
        and run.after_snapshot_id == change.after_snapshot_id
    )


def _results_for_policy(run, policy):
    """The run's results that came from this policy's rules, as scorable rows."""
    return list(
        run.results.filter(rule__policy=policy).values_list("status", "severity")
    )


class _Row:
    def __init__(self, status, severity):
        self.status = status
        self.severity = severity


def validation_blockers(change):
    """What the applicable policies still want before a gate may pass.

    Judged policy by policy: a policy that asks for a run needs a completed, current
    run that actually ran its rules (a run of another policy does not stand in for
    it), with nothing unanswered, that measured something and reached its score. A
    policy that asks for nothing adds nothing.
    """
    blockers = []
    runs = list(
        change.validation_runs.filter(status=RunStatus.COMPLETED).order_by("-created")
    )
    for policy in policies_for(change):
        if not (policy.require_passed_run or policy.min_score):
            continue
        covering = None
        for run in runs:
            rows = _results_for_policy(run, policy)
            if rows:
                covering = (run, rows)
                break
        name = policy.name
        if covering is None:
            blockers.append(
                f"Run validation: policy '{name}' requires a completed validation run."
            )
            continue
        run, rows = covering
        if not run_is_current(run, change):
            blockers.append(
                f"Validation is out of date for policy '{name}': the change's "
                "snapshots moved since the last run. Run it again."
            )
            continue
        unanswered = sum(1 for status, _sev in rows if status == Status.ERROR)
        if unanswered:
            blockers.append(
                f"Policy '{name}': {unanswered} rule result(s) could not be answered "
                "by Forward, so the run cannot pass."
            )
            continue
        score = score_results([_Row(status, severity) for status, severity in rows])
        if score is None:
            blockers.append(
                f"Policy '{name}': the last validation run measured nothing, so it "
                "cannot pass."
            )
        elif score < policy.min_score:
            blockers.append(
                f"Policy '{name}': validation scored {score}; at least "
                f"{policy.min_score} is required."
            )
    return blockers


def rule_parameter_problems(engine, parameters):
    """What is wrong with a rule's parameters for its engine, or [] if nothing."""
    params = parameters if isinstance(parameters, dict) else None
    if params is None:
        return ["Parameters must be a JSON object."]
    problems = []
    if engine == Engine.NQE:
        if not str(params.get("query_path") or "").strip():
            problems.append(
                "An nqe rule needs a `query_path` (the path of an org query)."
            )
        if params.get("expectation", Expect.NO_ROWS) not in (
            Expect.NO_ROWS,
            Expect.SOME_ROWS,
        ):
            problems.append("`expectation` must be `no-rows` or `some-rows`.")
    elif engine == Engine.CHECK:
        for key in ("check_names", "check_types"):
            value = params.get(key, [])
            if not isinstance(value, list) or not all(
                isinstance(v, str) for v in value
            ):
                problems.append(f"`{key}` must be a list of strings.")
    known = {
        Engine.NQE: {"query_path", "expectation", "commit_id"},
        Engine.CHECK: {"check_names", "check_types"},
        Engine.PREDICT: set(),
    }.get(engine, set())
    unknown = sorted(set(params) - known)
    if unknown:
        problems.append(f"Unknown parameter(s) for {engine}: {', '.join(unknown)}.")
    return problems
