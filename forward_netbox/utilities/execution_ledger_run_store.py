from core.choices import JobStatusChoices
from core.models import Job
from django.db import transaction
from django.utils import timezone

from ..choices import ForwardApplyEngineChoices
from ..choices import ForwardExecutionBackendChoices
from ..choices import ForwardExecutionRunStatusChoices
from ..choices import ForwardExecutionStepKindChoices
from ..choices import ForwardExecutionStepStatusChoices
from .branch_budget import shard_fetch_contract


def ensure_branch_execution_run(
    *,
    sync,
    context,
    plan,
    plan_preview,
    validation_run=None,
    job=None,
    max_changes_per_branch=1,
    auto_merge=False,
    model_change_density=None,
    next_plan_index=None,
):
    from ..models import ForwardExecutionRun
    from .sync_state import get_branch_run_display_state
    from .sync_state import prune_stale_branch_run_state

    terminal_statuses = {
        ForwardExecutionRunStatusChoices.COMPLETED,
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }
    with transaction.atomic():
        locked_sync = (
            sync.__class__.objects.select_for_update()
            .select_related("source")
            .get(pk=sync.pk)
        )
        run = None
        state = get_branch_run_display_state(locked_sync)
        run_id = state.get("execution_run_id")
        if run_id:
            candidate = (
                ForwardExecutionRun.objects.select_for_update()
                .filter(pk=run_id, sync=locked_sync)
                .first()
            )
            if candidate is not None and candidate.status not in terminal_statuses:
                run = candidate
        if run is None:
            run = (
                ForwardExecutionRun.objects.select_for_update()
                .filter(sync=locked_sync)
                .exclude(status__in=terminal_statuses)
                .order_by("-pk")
                .first()
            )
        if run is None:
            prune_stale_branch_run_state(locked_sync)
            state = get_branch_run_display_state(locked_sync)
        if run is None:
            run = ForwardExecutionRun.objects.create(
                sync=locked_sync,
                source=locked_sync.source,
                job=job if isinstance(job, Job) else None,
                validation_run=validation_run,
                backend=ForwardExecutionBackendChoices.BRANCHING,
                status=ForwardExecutionRunStatusChoices.RUNNING,
                phase="executing",
                phase_message="Applying planned shard changes.",
                snapshot_selector=context["snapshot_selector"],
                snapshot_id=context["snapshot_id"],
                max_changes_per_branch=max_changes_per_branch,
                auto_merge=bool(auto_merge),
                total_steps=len(plan),
                next_step_index=int(
                    next_plan_index or state.get("next_plan_index") or 1
                ),
                plan_preview=plan_preview or {},
                model_change_density=dict(model_change_density or {}),
                latest_heartbeat=timezone.now(),
            )
        else:
            phase = state.get("phase") or run.phase or "executing"
            phase_message = (
                state.get("phase_message")
                or run.phase_message
                or "Applying planned shard changes."
            )
            run.source = locked_sync.source
            if isinstance(job, Job):
                run.job = job
            run.validation_run = validation_run or run.validation_run
            run.backend = ForwardExecutionBackendChoices.BRANCHING
            run.status = ForwardExecutionRunStatusChoices.RUNNING
            run.phase = phase
            run.phase_message = phase_message
            run.snapshot_selector = context["snapshot_selector"]
            run.snapshot_id = context["snapshot_id"]
            run.max_changes_per_branch = max_changes_per_branch
            run.auto_merge = bool(auto_merge)
            run.total_steps = len(plan)
            run.next_step_index = int(
                next_plan_index or state.get("next_plan_index") or 1
            )
            run.plan_preview = plan_preview or {}
            run.model_change_density = dict(model_change_density or {})
            run.latest_heartbeat = timezone.now()
            run.save()

        sync_steps_from_plan(run, plan)
    return run


def sync_steps_from_plan(run, plan):
    from ..models import ForwardExecutionStep

    existing_steps = {
        step.index: step
        for step in ForwardExecutionStep.objects.filter(
            run=run,
            kind=ForwardExecutionStepKindChoices.STAGE,
        )
    }
    seen_indexes = set()
    for item in plan:
        seen_indexes.add(int(item.index))
        fetch_contract = shard_fetch_contract(item.model_string, item.shard_keys)
        defaults = {
            "model_string": item.model_string,
            "label": item.label,
            "query_name": item.query_name,
            "execution_mode": item.execution_mode,
            "execution_value": item.execution_value,
            "sync_mode": item.sync_mode,
            "operation": item.operation,
            "baseline_snapshot_id": item.baseline_snapshot_id,
            "estimated_changes": max(0, int(item.estimated_changes or 0)),
            "fetched_row_count": max(0, int(item.estimated_changes or 0)),
            "query_runtime_ms": item.query_runtime_ms,
            "shard_keys": list(item.shard_keys or ()),
            "apply_engine": item.apply_engine,
            "fetch_mode": item.fetch_mode or fetch_contract["fetch_mode"],
            "fetch_key_family": (
                item.fetch_key_family or fetch_contract["fetch_key_family"]
            ),
            "fetch_parameters": (
                item.fetch_parameters or fetch_contract["fetch_parameters"]
            ),
            "query_parameters": (
                item.query_parameters or fetch_contract.get("query_parameters") or {}
            ),
            "fetch_column_filters": (
                item.fetch_column_filters or fetch_contract["fetch_column_filters"]
            ),
        }
        step = existing_steps.get(int(item.index))
        if step is None:
            ForwardExecutionStep.objects.create(
                run=run,
                index=int(item.index),
                kind=ForwardExecutionStepKindChoices.STAGE,
                status=ForwardExecutionStepStatusChoices.PENDING,
                **defaults,
            )
            continue
        for field, value in defaults.items():
            setattr(step, field, value)
        step.save(
            update_fields=[
                *defaults.keys(),
                "updated",
            ]
        )

    stale_steps = [
        step.pk for index, step in existing_steps.items() if index not in seen_indexes
    ]
    if stale_steps:
        ForwardExecutionStep.objects.filter(pk__in=stale_steps).update(
            status=ForwardExecutionStepStatusChoices.SKIPPED,
            completed=timezone.now(),
        )


def latest_execution_run(sync, *, terminal_run_statuses=None):
    terminal_run_statuses = terminal_run_statuses or {
        ForwardExecutionRunStatusChoices.COMPLETED,
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }
    run = active_execution_run(sync, terminal_run_statuses=terminal_run_statuses)
    if run is not None:
        return run
    return sync.execution_runs.order_by("-pk").first()


def active_execution_run(sync, *, terminal_run_statuses=None):
    terminal_run_statuses = terminal_run_statuses or {
        ForwardExecutionRunStatusChoices.COMPLETED,
        ForwardExecutionRunStatusChoices.FAILED,
        ForwardExecutionRunStatusChoices.TIMEOUT,
        ForwardExecutionRunStatusChoices.CANCELLED,
    }
    return (
        sync.execution_runs.exclude(status__in=terminal_run_statuses)
        .order_by("-pk")
        .first()
    )


def upgrade_branch_run_state_to_execution_run(sync):
    from ..models import ForwardExecutionRun
    from ..models import ForwardExecutionStep

    state = sync.get_branch_run_state()
    plan_items = state.get("plan_items")
    if not state or state.get("execution_run_id") or not isinstance(plan_items, list):
        return None
    if not plan_items:
        return None
    with transaction.atomic():
        sync.__class__.objects.select_for_update().get(pk=sync.pk)
        sync.refresh_from_db()
        state = sync.get_branch_run_state()
        plan_items = state.get("plan_items")
        if state.get("execution_run_id"):
            return ForwardExecutionRun.objects.filter(
                pk=state.get("execution_run_id"),
                sync=sync,
            ).first()
        if not isinstance(plan_items, list) or not plan_items:
            return None

        run = ForwardExecutionRun.objects.create(
            sync=sync,
            source=sync.source,
            backend=ForwardExecutionBackendChoices.BRANCHING,
            status=_run_status_from_branch_state(sync, state),
            phase=state.get("phase") or "executing",
            phase_message=(
                state.get("phase_message") or "Recovered Branching run state."
            ),
            snapshot_selector=(
                state.get("snapshot_selector")
                or state.get("snapshot_id")
                or sync.get_snapshot_id()
                or ""
            ),
            snapshot_id=state.get("snapshot_id") or "",
            max_changes_per_branch=int(
                state.get("max_changes_per_branch")
                or sync.get_max_changes_per_branch()
                or 1
            ),
            auto_merge=bool(state.get("auto_merge", sync.auto_merge)),
            total_steps=int(state.get("total_plan_items") or len(plan_items)),
            next_step_index=int(state.get("next_plan_index") or 1),
            plan_preview=state.get("plan_preview") or {},
            model_change_density=state.get("model_change_density") or {},
            latest_heartbeat=timezone.now(),
            reconciliation_events=[
                {
                    "timestamp": timezone.now().isoformat(),
                    "type": "run",
                    "reason": "branch_run_state_upgraded",
                    "status": _run_status_from_branch_state(sync, state),
                    "phase": state.get("phase") or "executing",
                }
            ],
        )
        for item in plan_items:
            if isinstance(item, dict):
                ForwardExecutionStep.objects.create(
                    run=run,
                    **_execution_step_kwargs_from_plan_item(item),
                )
    return run


def legacy_execution_run_from_branch_state(sync, state):
    plan_items = state.get("plan_items")
    if not isinstance(plan_items, list) or not plan_items:
        return None

    snapshot_id = str(state.get("snapshot_id") or "")
    total_steps = int(state.get("total_plan_items") or len(plan_items))
    next_step_index = int(state.get("next_plan_index") or 1)
    auto_merge = bool(state.get("auto_merge", sync.auto_merge))

    for run in (
        sync.execution_runs.filter(
            snapshot_id=snapshot_id,
            total_steps=total_steps,
            next_step_index=next_step_index,
            auto_merge=auto_merge,
            backend=ForwardExecutionBackendChoices.BRANCHING,
        )
        .order_by("-pk")
        .prefetch_related("steps")
    ):
        if not any(
            isinstance(event, dict)
            and event.get("reason") == "branch_run_state_upgraded"
            for event in (run.reconciliation_events or [])
        ):
            continue
        if run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE).count() != len(
            plan_items
        ):
            continue
        return run
    return None


def execution_step_for_ingestion(ingestion):
    from ..models import ForwardExecutionStep

    if ingestion is None or not getattr(ingestion, "pk", None):
        return None
    return (
        ForwardExecutionStep.objects.select_related("run", "branch")
        .filter(
            ingestion=ingestion,
            kind=ForwardExecutionStepKindChoices.STAGE,
        )
        .order_by("-run_id", "-index", "-pk")
        .first()
    )


def ingestion_has_mergeable_execution_step(ingestion, *, mergeable_step_statuses):
    step = execution_step_for_ingestion(ingestion)
    if step is None:
        return False
    if step.status not in mergeable_step_statuses:
        return False
    branch = step.branch or getattr(ingestion, "branch", None)
    return bool(branch and getattr(branch, "status", "") != "merged")


def ingestion_has_requeueable_merge_timeout_step(ingestion):
    step = execution_step_for_ingestion(ingestion)
    if step is None:
        return False
    if step.status != ForwardExecutionStepStatusChoices.MERGE_TIMEOUT:
        return False
    branch = step.branch or getattr(ingestion, "branch", None)
    return bool(branch and getattr(branch, "status", "") != "merged")


def claim_ingestion_merge_step(ingestion, job):
    from ..models import ForwardExecutionStep

    step = execution_step_for_ingestion(ingestion)
    if step is None:
        return True
    now = timezone.now()
    with transaction.atomic():
        step = ForwardExecutionStep.objects.select_for_update().get(pk=step.pk)
        if step.status in {
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.SKIPPED,
            ForwardExecutionStepStatusChoices.CANCELLED,
        }:
            return False
        merge_job = step.merge_job
        requeueing_timed_out_merge = (
            step.status == ForwardExecutionStepStatusChoices.MERGE_TIMEOUT
        )
        if (
            merge_job is not None
            and merge_job.pk != getattr(job, "pk", None)
            and not merge_job.completed
            and not requeueing_timed_out_merge
        ):
            return False
        if isinstance(job, Job):
            step.merge_job = job
        step.status = ForwardExecutionStepStatusChoices.MERGE_QUEUED
        step.heartbeat = now
        step.completed = None
        if requeueing_timed_out_merge:
            step.last_error = ""
        step.save()
    return True


def mark_ingestion_step_merged(ingestion, *, baseline_ready=False, merge_job=None):
    from ..models import ForwardExecutionRun
    from ..models import ForwardExecutionStep

    step = execution_step_for_ingestion(ingestion)
    if step is None:
        return None
    now = timezone.now()
    with transaction.atomic():
        step = (
            ForwardExecutionStep.objects.select_for_update()
            .select_related("run")
            .get(pk=step.pk)
        )
        run = ForwardExecutionRun.objects.select_for_update().get(pk=step.run_id)
        if isinstance(merge_job, Job):
            step.merge_job = merge_job
        if step.branch_id and not step.branch_name:
            step.branch_name = step.branch.name
        elif getattr(ingestion, "branch", None) is not None and not step.branch_name:
            step.branch_name = ingestion.branch.name
        step.status = ForwardExecutionStepStatusChoices.MERGED
        step.completed = step.completed or now
        step.heartbeat = now
        step.save()

        next_incomplete_index = _next_incomplete_stage_index(run)
        if next_incomplete_index is not None:
            run.next_step_index = next_incomplete_index
        else:
            run.next_step_index = max(
                int(run.next_step_index or 1), int(step.index) + 1
            )
        run.latest_heartbeat = now
        if _step_is_final_success(step):
            run.status = ForwardExecutionRunStatusChoices.COMPLETED
            run.phase = "completed"
            run.phase_message = "Forward execution completed."
            run.baseline_ready = bool(baseline_ready)
            run.completed = run.completed or now
        else:
            run.status = ForwardExecutionRunStatusChoices.RUNNING
            run.phase = "merged"
            run.phase_message = "Merged shard; ready for next shard."
        run.save()
    return step


def update_run_from_branch_state(sync):
    run = active_execution_run(
        sync,
        terminal_run_statuses={
            ForwardExecutionRunStatusChoices.COMPLETED,
            ForwardExecutionRunStatusChoices.FAILED,
            ForwardExecutionRunStatusChoices.TIMEOUT,
            ForwardExecutionRunStatusChoices.CANCELLED,
        },
    )
    if run is None:
        return None
    from .sync_state import get_branch_run_display_state

    state = get_branch_run_display_state(sync)
    status = _run_status_from_steps(
        run,
        fallback_status=_run_status_from_sync(sync),
    )
    phase, phase_message = _run_phase_from_steps(
        run,
        status=status,
        fallback_phase=(state.get("phase") or run.phase),
        fallback_message=(state.get("phase_message") or run.phase_message),
    )
    run.status = status
    run.phase = phase
    run.phase_message = phase_message
    run.next_step_index = int(state.get("next_plan_index") or run.next_step_index or 1)
    run.total_steps = int(state.get("total_plan_items") or run.total_steps or 0)
    run.plan_preview = state.get("plan_preview") or run.plan_preview or {}
    run.model_change_density = (
        state.get("model_change_density") or run.model_change_density or {}
    )
    run.latest_heartbeat = timezone.now()
    run.save()
    return run


def _run_status_from_steps(run, *, fallback_status):
    stage_steps = run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
    if not stage_steps.exists():
        return fallback_status

    if stage_steps.filter(
        status__in=[
            ForwardExecutionStepStatusChoices.QUEUED,
            ForwardExecutionStepStatusChoices.RUNNING,
            ForwardExecutionStepStatusChoices.MERGE_QUEUED,
        ]
    ).exists():
        return ForwardExecutionRunStatusChoices.RUNNING

    if stage_steps.filter(status=ForwardExecutionStepStatusChoices.STAGED).exists():
        return ForwardExecutionRunStatusChoices.WAITING

    if stage_steps.filter(status=ForwardExecutionStepStatusChoices.PENDING).exists():
        return ForwardExecutionRunStatusChoices.RUNNING

    if stage_steps.filter(
        status__in=[
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        ]
    ).exists():
        return ForwardExecutionRunStatusChoices.TIMEOUT

    if stage_steps.filter(status=ForwardExecutionStepStatusChoices.FAILED).exists():
        return ForwardExecutionRunStatusChoices.FAILED

    if not stage_steps.exclude(
        status__in=[
            ForwardExecutionStepStatusChoices.MERGED,
            ForwardExecutionStepStatusChoices.SKIPPED,
            ForwardExecutionStepStatusChoices.CANCELLED,
        ]
    ).exists():
        return ForwardExecutionRunStatusChoices.COMPLETED

    return fallback_status


def _run_phase_from_steps(
    run,
    *,
    status,
    fallback_phase,
    fallback_message,
):
    stage_steps = run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
    total_steps = int(run.total_steps or 0)
    active_step = (
        stage_steps.filter(
            status__in=[
                ForwardExecutionStepStatusChoices.RUNNING,
                ForwardExecutionStepStatusChoices.QUEUED,
                ForwardExecutionStepStatusChoices.MERGE_QUEUED,
            ]
        )
        .order_by("index")
        .first()
    )
    if active_step is not None:
        shard_text = (
            f"{int(active_step.index)}/{total_steps}"
            if total_steps
            else str(int(active_step.index))
        )
        if active_step.status == ForwardExecutionStepStatusChoices.MERGE_QUEUED:
            return (
                "queued_merge",
                f"Queued merge for shard {shard_text}.",
            )
        if active_step.status == ForwardExecutionStepStatusChoices.RUNNING:
            return ("staging", f"Applying shard {shard_text}.")
        return ("queued", f"Queued shard {shard_text} for Branching execution.")

    if status == ForwardExecutionRunStatusChoices.WAITING:
        staged_step = (
            stage_steps.filter(status=ForwardExecutionStepStatusChoices.STAGED)
            .order_by("index")
            .first()
        )
        if staged_step is not None:
            shard_text = (
                f"{int(staged_step.index)}/{total_steps}"
                if total_steps
                else str(int(staged_step.index))
            )
            return ("waiting_merge", f"Waiting for merge of shard {shard_text}.")
    return fallback_phase, fallback_message


def update_step_from_plan_item(
    sync,
    index,
    *,
    terminal_stage_statuses,
    active_execution_run_fn,
    **updates,
):
    run = active_execution_run_fn(sync)
    if run is None:
        return False
    step = run.steps.filter(
        index=int(index),
        kind=ForwardExecutionStepKindChoices.STAGE,
    ).first()
    if step is None:
        return False
    mapped = _step_updates(updates)
    for field, value in mapped.items():
        setattr(step, field, value)
    if "status" in mapped:
        if mapped["status"] in terminal_stage_statuses or mapped["status"] in (
            ForwardExecutionStepStatusChoices.FAILED,
            ForwardExecutionStepStatusChoices.TIMEOUT,
            ForwardExecutionStepStatusChoices.MERGE_TIMEOUT,
        ):
            step.completed = timezone.now()
        elif mapped["status"] == ForwardExecutionStepStatusChoices.RUNNING:
            step.started = step.started or timezone.now()
    step.heartbeat = timezone.now()
    step.save()
    update_run_from_branch_state(sync)
    return True


def touch_execution_step_progress(
    sync,
    *,
    model_string,
    shard_index=None,
    row_count=None,
    row_total=None,
    active_execution_run_fn=None,
):
    from ..models import ForwardExecutionRun
    from ..models import ForwardExecutionStep

    run = active_execution_run_fn(sync)
    if run is None:
        return False
    queryset = ForwardExecutionStep.objects.select_for_update().filter(
        run=run,
        kind=ForwardExecutionStepKindChoices.STAGE,
    )
    if shard_index is not None:
        queryset = queryset.filter(index=int(shard_index))
    else:
        queryset = queryset.filter(
            status=ForwardExecutionStepStatusChoices.RUNNING,
            model_string=model_string,
        )
    with transaction.atomic():
        step = queryset.order_by("index").first()
        if step is None:
            return False
        update_fields = ["heartbeat", "updated"]
        if row_count is not None:
            step.attempted_row_count = max(
                int(step.attempted_row_count or 0),
                int(row_count),
            )
            update_fields.append("attempted_row_count")
        if row_total is not None:
            step.fetched_row_count = max(
                int(step.fetched_row_count or 0),
                int(row_total),
            )
            update_fields.append("fetched_row_count")
        step.heartbeat = timezone.now()
        step.save(update_fields=update_fields)
        ForwardExecutionRun.objects.filter(pk=run.pk).update(
            latest_heartbeat=step.heartbeat,
        )
    return True


def claim_stage_step(
    sync, index, job, *, claimable_stage_statuses, active_execution_run_fn
):
    from ..models import ForwardExecutionStep

    run = active_execution_run_fn(sync)
    if run is None:
        return None
    with transaction.atomic():
        other_running_step = (
            ForwardExecutionStep.objects.select_for_update()
            .filter(
                run=run,
                kind=ForwardExecutionStepKindChoices.STAGE,
                status=ForwardExecutionStepStatusChoices.RUNNING,
            )
            .exclude(index=int(index))
            .order_by("index")
            .first()
        )
        if other_running_step is not None:
            running_job = getattr(other_running_step, "job", None)
            # Permit claim only when the older running marker is clearly finished.
            if running_job is None or getattr(running_job, "completed", None) is None:
                return None
        step = (
            ForwardExecutionStep.objects.select_for_update()
            .filter(
                run=run,
                index=int(index),
                kind=ForwardExecutionStepKindChoices.STAGE,
            )
            .first()
        )
        if step is None:
            return None
        if step.status == ForwardExecutionStepStatusChoices.RUNNING:
            incoming_job_id = job.pk if isinstance(job, Job) else None
            if step.job_id and step.job_id != incoming_job_id:
                return None
        elif step.status not in claimable_stage_statuses:
            return None
        if isinstance(job, Job):
            step.job = job
        step.status = ForwardExecutionStepStatusChoices.RUNNING
        step.started = step.started or timezone.now()
        step.heartbeat = timezone.now()
        step.last_error = ""
        step.save()
        run.status = ForwardExecutionRunStatusChoices.RUNNING
        run.next_step_index = int(index)
        run.latest_heartbeat = timezone.now()
        run.save()
        return step


def mark_run_completed(
    sync, *, baseline_ready=False, active_execution_run_fn, latest_execution_run_fn
):
    candidate = active_execution_run_fn(sync)
    if candidate is None:
        candidate = latest_execution_run_fn(sync)
    if candidate is None:
        return None
    from ..models import ForwardExecutionRun

    with transaction.atomic():
        run = ForwardExecutionRun.objects.select_for_update().get(pk=candidate.pk)
        if run.status == ForwardExecutionRunStatusChoices.COMPLETED:
            return run
        if not _run_can_complete(run):
            return run
        run.status = ForwardExecutionRunStatusChoices.COMPLETED
        run.phase = "completed"
        run.phase_message = "Forward execution completed."
        run.baseline_ready = bool(baseline_ready)
        run.completed = timezone.now()
        run.latest_heartbeat = timezone.now()
        run.save()
        return run


def _step_is_final_success(step):
    run = step.run
    if run.total_steps and int(step.index) < int(run.total_steps):
        return False
    return _run_can_complete(run)


def _next_incomplete_stage_index(run):
    index = (
        run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
        .exclude(
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ]
        )
        .order_by("index")
        .values_list("index", flat=True)
        .first()
    )
    if index is not None:
        return int(index)
    total_steps = int(run.total_steps or 0)
    if not total_steps:
        return None
    existing_indexes = set(
        run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE).values_list(
            "index",
            flat=True,
        )
    )
    for candidate in range(1, total_steps + 1):
        if candidate not in existing_indexes:
            return candidate
    return None


def _stage_steps_successfully_terminal(run):
    return (
        not run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
        .exclude(
            status__in=[
                ForwardExecutionStepStatusChoices.MERGED,
                ForwardExecutionStepStatusChoices.SKIPPED,
                ForwardExecutionStepStatusChoices.CANCELLED,
            ]
        )
        .exists()
    )


def _run_can_complete(run):
    stage_steps = run.steps.filter(kind=ForwardExecutionStepKindChoices.STAGE)
    if stage_steps.exists():
        if not _stage_steps_successfully_terminal(run):
            return False
        if run.total_steps:
            return stage_steps.count() >= int(run.total_steps)
        return True
    # Keep backward compatibility for no-step runs (for example, pre-queued
    # completion calls that never materialized stage rows).
    return True


def _run_status_from_sync(sync):
    status = getattr(sync, "status", "")
    if status == "completed":
        return ForwardExecutionRunStatusChoices.COMPLETED
    if status == "failed":
        return ForwardExecutionRunStatusChoices.FAILED
    if status == "timeout":
        return ForwardExecutionRunStatusChoices.TIMEOUT
    if status == "ready_to_merge":
        return ForwardExecutionRunStatusChoices.WAITING
    if status in {"queued", "syncing", "merging"}:
        return ForwardExecutionRunStatusChoices.RUNNING
    return ForwardExecutionRunStatusChoices.QUEUED


def _run_status_from_branch_state(sync, state):
    phase = str((state or {}).get("phase") or "")
    if phase == "completed":
        return ForwardExecutionRunStatusChoices.COMPLETED
    if phase == "failed":
        return ForwardExecutionRunStatusChoices.FAILED
    if phase == "timeout":
        return ForwardExecutionRunStatusChoices.TIMEOUT
    if (state or {}).get("awaiting_merge"):
        return ForwardExecutionRunStatusChoices.WAITING
    return _run_status_from_sync(sync)


def _step_updates(updates):
    mapped = {}
    status = updates.get("status")
    if status:
        mapped["status"] = _normalize_step_status(status)
    if "ingestion_id" in updates:
        mapped["ingestion_id"] = updates.get("ingestion_id")
    if "branch_name" in updates:
        mapped["branch_name"] = updates.get("branch_name") or ""
    if "stage_job_id" in updates:
        mapped["job_id"] = updates.get("stage_job_id")
    if "merge_job_id" in updates:
        mapped["merge_job_id"] = updates.get("merge_job_id")
    if "retry_count" in updates:
        mapped["retry_count"] = int(updates.get("retry_count") or 0)
    if "last_error" in updates:
        mapped["last_error"] = updates.get("last_error") or ""
    if "actual_changes" in updates:
        mapped["actual_changes"] = max(0, int(updates.get("actual_changes") or 0))
    for source, target in (
        ("attempted_row_count", "attempted_row_count"),
        ("applied_row_count", "applied_row_count"),
        ("skipped_row_count", "skipped_row_count"),
        ("failed_row_count", "failed_row_count"),
    ):
        if source in updates:
            mapped[target] = max(0, int(updates.get(source) or 0))
    if "apply_engine" in updates:
        mapped["apply_engine"] = (
            updates.get("apply_engine") or ForwardApplyEngineChoices.ADAPTER
        )
    if "fetch_mode" in updates:
        mapped["fetch_mode"] = updates.get("fetch_mode") or "model"
    if "fetch_key_family" in updates:
        mapped["fetch_key_family"] = updates.get("fetch_key_family") or ""
    if "fetch_parameters" in updates:
        mapped["fetch_parameters"] = dict(updates.get("fetch_parameters") or {})
    if "query_parameters" in updates:
        mapped["query_parameters"] = dict(updates.get("query_parameters") or {})
    if "fetch_column_filters" in updates:
        mapped["fetch_column_filters"] = list(updates.get("fetch_column_filters") or [])
    return mapped


def _normalize_step_status(status):
    status = str(status)
    return {
        "staging": ForwardExecutionStepStatusChoices.RUNNING,
        "queued_merge": ForwardExecutionStepStatusChoices.MERGE_QUEUED,
    }.get(status, status)


def _execution_step_kwargs_from_plan_item(item):
    shard_keys = list(item.get("shard_keys") or [])
    model_string = item.get("model") or item.get("model_string") or ""
    fetch_contract = shard_fetch_contract(model_string, shard_keys)
    return {
        "index": int(item.get("index") or 1),
        "kind": ForwardExecutionStepKindChoices.STAGE,
        "status": _normalize_step_status(
            item.get("status") or ForwardExecutionStepStatusChoices.PENDING
        ),
        "model_string": model_string,
        "label": item.get("label") or model_string,
        "query_name": item.get("query_name") or "",
        "execution_mode": item.get("execution_mode") or "",
        "execution_value": item.get("execution_value") or "",
        "commit_id": item.get("commit_id") or "",
        "sync_mode": item.get("sync_mode") or "",
        "operation": item.get("operation") or "mixed",
        "baseline_snapshot_id": item.get("baseline_snapshot_id") or "",
        "estimated_changes": max(0, int(item.get("estimated_changes") or 0)),
        "actual_changes": max(0, int(item.get("actual_changes") or 0)),
        "fetched_row_count": max(0, int(item.get("fetched_row_count") or 0)),
        "query_runtime_ms": item.get("query_runtime_ms"),
        "attempted_row_count": max(0, int(item.get("attempted_row_count") or 0)),
        "applied_row_count": max(0, int(item.get("applied_row_count") or 0)),
        "skipped_row_count": max(0, int(item.get("skipped_row_count") or 0)),
        "failed_row_count": max(0, int(item.get("failed_row_count") or 0)),
        "shard_keys": shard_keys,
        "fetch_mode": item.get("fetch_mode") or fetch_contract["fetch_mode"],
        "fetch_key_family": (
            item.get("fetch_key_family") or fetch_contract["fetch_key_family"]
        ),
        "fetch_parameters": (
            item.get("fetch_parameters") or fetch_contract["fetch_parameters"]
        ),
        "query_parameters": (
            item.get("query_parameters") or fetch_contract.get("query_parameters") or {}
        ),
        "fetch_column_filters": (
            item.get("fetch_column_filters") or fetch_contract["fetch_column_filters"]
        ),
        "apply_engine": item.get("apply_engine") or ForwardApplyEngineChoices.ADAPTER,
        "branch_name": item.get("branch_name") or "",
        "retry_count": max(0, int(item.get("retry_count") or 0)),
        "last_error": item.get("last_error") or "",
    }


def ingestion_merge_job_state(ingestion):
    step = execution_step_for_ingestion(ingestion)
    if step is None or step.merge_job is None:
        return ""
    return step.merge_job.status


def ingestion_merge_job_timed_out(ingestion):
    return ingestion_merge_job_state(ingestion) == JobStatusChoices.STATUS_FAILED
