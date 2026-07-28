# One branch per sync. Dependency-ordered workloads stage in the same native
# Branching branch, which is reviewed and merged exactly once.
from core.exceptions import SyncError
from core.models import ObjectType
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from ..choices import ForwardSyncStatusChoices
from .branch_budget import build_branch_plan
from .branch_lifecycle import create_noop_ingestion
from .branch_lifecycle import run_item_in_branch
from .branching import build_branch_request
from .branching import missing_branch_table_report
from .executor_base import ForwardExecutorBase
from .primary_ip import apply_primary_ip_from_mgmt_tags
from .primary_ip import primary_ip_from_mgmt_tag_enabled
from .query_fetch import ForwardQueryFetcher
from .validation import ForwardValidationRunner
from .workload_state import stage_and_promote_noop_workload_states
from .workload_state import stage_workload_states


def failed_model_strings(model_results) -> list[str]:
    """Models whose fetch failed, so an empty workload set is not convergence.

    An empty workload set means one of two very different things: the source
    genuinely converged, or every fetch failed and produced nothing. They are
    indistinguishable from the workload alone, so the failure results decide it.
    """
    return sorted(
        {
            str(getattr(result, "model_string", "") or "")
            for result in model_results or []
            if getattr(result, "failure_count", 0)
        }
        - {""}
    )


class ForwardSingleBranchExecutor(ForwardExecutorBase):
    """Stage a whole sync into ONE provisioned branch, then bulk-merge once."""

    def run(self):
        self.logger.log_info("Starting single-branch readiness checks.", obj=self.sync)
        # Fail in seconds — before the expensive fetch and before provisioning
        # would die mid-CREATE TABLE on a table that was never migrated.
        missing_tables = missing_branch_table_report()
        if missing_tables:
            detail = "; ".join(
                f"{app}: {', '.join(tables)}" for app, tables in missing_tables.items()
            )
            raise SyncError(
                "Cannot provision a branch: database tables are missing for "
                f"installed apps ({detail}). Apply their migrations "
                "(python manage.py migrate) or remove the plugin from PLUGINS, "
                "then re-run the sync."
            )
        fetcher = ForwardQueryFetcher(self.sync, self.client, self.logger)
        context = fetcher.resolve_context()
        self.logger.log_info(
            "Resolving snapshot and building the validated single-branch workload.",
            obj=self.sync,
        )
        workloads = fetcher.fetch_workloads(context, include_diagnostics=False)
        self.last_model_results = [r.as_dict() for r in fetcher.model_results]
        self.logger.log_info(
            "Recording single-branch validation results.",
            obj=self.sync,
        )
        self.last_validation_run = ForwardValidationRunner(
            self.sync,
            self.client,
            self.logger,
            job=self.job,
        ).record_plan_validation(context.as_dict(), workloads, self.last_model_results)
        if not workloads:
            # Previously any empty workload recorded a completed, zero-change,
            # branchless ingestion, so a run whose every fetch failed was
            # indistinguishable from a converged one. Fail closed instead.
            failed_models = failed_model_strings(fetcher.model_results)
            if failed_models:
                raise SyncError(
                    "No Forward changes were returned because "
                    f"{len(failed_models)} model(s) failed to fetch: "
                    f"{', '.join(failed_models)}. This run applied nothing and is "
                    "not a converged sync. Resolve the reported query failures "
                    "and re-run; see the execution contract preflight warnings "
                    "on this job for the failing maps."
                )
            self.logger.log_info("No Forward changes were returned for this run.")
            ingestion = create_noop_ingestion(self, context.as_dict())
            staged_contributor_relations = fetcher.stage_pending_contributor_baseline(
                ingestion, context
            )
            if staged_contributor_relations:
                self.logger.log_info(
                    "Staged "
                    f"{staged_contributor_relations} contributor relation(s) "
                    "for merge-gated no-op promotion.",
                    obj=ingestion,
                )
            promoted = stage_and_promote_noop_workload_states(
                ingestion,
                fetcher.pending_workload_states,
            )
            if promoted:
                self.logger.log_info(
                    f"Promoted {promoted} durable workload state(s) without branch provisioning.",
                    obj=ingestion,
                )
            return [ingestion]

        from .fast_baseline import fast_baseline_static_decision, run_fast_baseline_load

        fast_baseline_decision = fast_baseline_static_decision(
            sync=self.sync,
            workloads=workloads,
            model_results=self.last_model_results,
        )
        fast_baseline_enabled = bool(
            (self.sync.parameters or {}).get("enable_fast_baseline_load")
        )
        fast_baseline_required = bool(
            (self.sync.parameters or {}).get("require_fast_baseline_eligibility")
        )
        if fast_baseline_required and not fast_baseline_enabled:
            raise SyncError(
                "Fast baseline eligibility cannot be required while the fast "
                "baseline load is disabled."
            )
        if fast_baseline_enabled:
            if fast_baseline_decision.enabled:
                self.logger.log_info(
                    "Fast baseline static checks passed; acquiring locked empty-"
                    "database eligibility checks.",
                    obj=self.sync,
                )
                ingestion, locked_decision = run_fast_baseline_load(
                    self,
                    context=context,
                    workloads=workloads,
                    fetcher=fetcher,
                )
                if ingestion is not None:
                    return [ingestion]
                if fast_baseline_required:
                    raise SyncError(
                        "Fast baseline eligibility is required; locked proof "
                        f"failed ({locked_decision.reason_code})."
                    )
                self.logger.log_info(
                    "Fast baseline failed closed "
                    f"({locked_decision.reason_code}); using the ordinary branch path.",
                    obj=self.sync,
                )
            else:
                if fast_baseline_required:
                    raise SyncError(
                        "Fast baseline eligibility is required; static proof "
                        f"failed ({fast_baseline_decision.reason_code})."
                    )
                self.logger.log_info(
                    "Fast baseline failed closed "
                    f"({fast_baseline_decision.reason_code}); using the ordinary branch path.",
                    obj=self.sync,
                )

        request = build_branch_request(self.user)
        ingestion = self._create_ingestion(
            context.as_dict(), change_request_id=request.id
        )
        staged_contributor_relations = fetcher.stage_pending_contributor_baseline(
            ingestion,
            context,
        )
        if staged_contributor_relations:
            self.logger.log_info(
                f"Staged {staged_contributor_relations} contributor relation(s) "
                "for merge-gated promotion.",
                obj=ingestion,
            )
        staged_states = stage_workload_states(
            ingestion,
            fetcher.pending_workload_states,
        )
        if staged_states:
            self.logger.log_info(
                f"Staged {staged_states} durable workload state(s) for merge-gated promotion.",
                obj=ingestion,
            )

        # Provision exactly ONE branch for the whole sync.
        self.logger.log_info("Provisioning single sync branch.", obj=self.sync)
        branch = Branch(name=f"Forward Sync {self.sync.pk} - ingestion {ingestion.pk}")
        branch.save(provision=False)
        branch.provision(user=self.user)
        branch.refresh_from_db()
        if branch.status == BranchStatusChoices.FAILED:
            self.logger.log_failure(f"Branch failed: `{branch}`", obj=branch)
            raise SyncError("Branch provisioning failed for single-branch sync.")
        ingestion.branch = branch
        ingestion.save(update_fields=["branch"])
        if self.job is not None:
            self.job.object_type = ObjectType.objects.get_for_model(ingestion)
            self.job.object_id = ingestion.pk
            self.job.save(update_fields=["object_type", "object_id"])

        # Stage every workload into the single branch. Bulk staging emits native
        # branch ObjectChanges for its parity-tested model set; an explicit
        # enable_bulk_orm=False keeps the adapter path. The workload budget
        # partitions oversized workloads into bounded staging/progress units,
        # all targeting this same branch. An indivisible identity bucket may
        # exceed the budget and is reported below without breaking colocation.
        max_changes_per_staging_item = self.sync.get_max_changes_per_staging_item()
        warned_models = set()
        for workload in workloads:
            if workload.estimated_changes > max_changes_per_staging_item:
                self.logger.log_warning(
                    f"{workload.model_string} workload has "
                    f"{workload.estimated_changes} estimated changes, exceeding "
                    f"the staging-item budget of {max_changes_per_staging_item}."
                )
                warned_models.add(workload.model_string)
        plan = build_branch_plan(
            workloads,
            max_changes_per_staging_item=max_changes_per_staging_item,
            oversized_bucket_policy="warn",
        )
        for item in plan:
            if (
                item.estimated_changes > max_changes_per_staging_item
                and item.model_string not in warned_models
            ):
                self.logger.log_warning(
                    f"{item.model_string} plan item has {item.estimated_changes} "
                    f"estimated changes, exceeding the staging-item budget of "
                    f"{max_changes_per_staging_item}."
                )
                warned_models.add(item.model_string)
        total = len(plan)
        context_dict = context.as_dict()
        for item in plan:
            run_item_in_branch(
                self,
                item,
                context_dict,
                ingestion,
                branch,
                total_plan_items=total,
            )

        ingestion.sync_mode = self._sync_mode()
        ingestion.model_results = self.last_model_results
        ingestion.save(update_fields=["sync_mode", "model_results"])

        # Optional: set device primary_ip4/6 from Forward Mgmt_<iface> tags. Runs
        # in the branch after every workload is staged (interfaces + IPs exist),
        # before merge, so the primary-IP updates merge with the rest of the sync.
        if primary_ip_from_mgmt_tag_enabled(self.sync):
            self.logger.log_info(
                "Resolving device primary IPs from Mgmt_ tags.",
                obj=self.sync,
            )
            apply_primary_ip_from_mgmt_tags(
                self, branch, snapshot_id=context_dict["snapshot_id"]
            )

        if not self.sync.auto_merge:
            # Leave the single branch staged for operator review.
            self.sync.status = ForwardSyncStatusChoices.READY_TO_MERGE
            self.sync.__class__.objects.filter(pk=self.sync.pk).update(
                status=self.sync.status
            )
            self.logger.log_success(
                "Forward single-branch sync staged for review.", obj=ingestion
            )
            return [ingestion]

        # Auto-merge: bulk-merge the one branch into main (bulk_merge.py).
        self.logger.log_info("Bulk-merging the single sync branch.", obj=self.sync)
        ingestion.sync_merge(remove_branch=True)
        self.logger.log_info(
            "Forward single-branch ingestion completed.", obj=ingestion
        )
        return [ingestion]
