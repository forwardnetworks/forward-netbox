# One branch per sync (large-dataset ingest redesign, phase 2).
#
# Collapses the per-shard model (Partner: 163 branches = 163 schema copies + 163
# merges) to a SINGLE branch per sync: provision one branch, stage ALL
# dependency-phased workloads into it (per-object, because netbox_branching
# tracks branch changes via core ObjectChange post_save signals — bulk_create
# would record nothing to merge), then bulk-merge the one branch into main
# exactly once (the merge replay was eliminated by bulk_merge.py). Branching
# stays the engine — there is still a real, reviewable branch; just one of it.
from core.exceptions import SyncError
from core.models import ObjectType
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from ..choices import ForwardSyncStatusChoices
from .branch_budget import build_branch_plan
from .branching import build_branch_request
from .branching import missing_branch_table_report
from .fast_bootstrap_executor import ForwardFastBootstrapExecutor
from .multi_branch_lifecycle import create_noop_ingestion
from .multi_branch_lifecycle import run_item_in_branch
from .primary_ip import apply_primary_ip_from_mgmt_tags
from .primary_ip import primary_ip_from_mgmt_tag_enabled
from .query_fetch import ForwardQueryFetcher
from .validation import ForwardValidationRunner


class ForwardSingleBranchExecutor(ForwardFastBootstrapExecutor):
    """Stage a whole sync into ONE provisioned branch, then bulk-merge once."""

    def run(self):
        self._set_runtime_phase("initializing", "Starting single-branch preflight.")
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
        self._set_runtime_phase(
            "planning",
            "Resolving snapshot, running query preflight, and building the "
            "single-branch workload.",
            next_plan_index=1,
        )
        if self._query_preflight_enabled():
            fetcher.run_preflight(context)
        workloads = fetcher.fetch_workloads(context)
        self.last_model_results = [r.as_dict() for r in fetcher.model_results]
        self._set_runtime_phase(
            "validating",
            "Recording single-branch validation results.",
            total_plan_items=len(workloads),
        )
        self.last_validation_run = ForwardValidationRunner(
            self.sync,
            self.client,
            self.logger,
            job=self.job,
        ).record_plan_validation(context.as_dict(), workloads, self.last_model_results)
        if not workloads:
            self.sync.clear_branch_run_state()
            self.logger.log_info("No Forward changes were returned for this run.")
            return [create_noop_ingestion(self, context.as_dict())]

        request = build_branch_request(self.user)
        ingestion = self._create_ingestion(
            context.as_dict(), change_request_id=request.id
        )

        # Provision exactly ONE branch for the whole sync.
        self._set_runtime_phase("provisioning", "Provisioning single sync branch.")
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

        # Branch change tracking is signal-based: a per-object save fires NetBox's
        # core ObjectChange post_save, which netbox_branching records as the
        # branch diff. bulk_create fires no signal, so historically it recorded
        # ZERO ObjectChanges and the merge silently dropped every bulk-staged row
        # — which forced slow per-object staging. Phase 4 fixes that at the
        # source: the bulk engines synthesize the branch ObjectChanges after each
        # bulk write (apply_engine_bulk.emit_branch_object_changes), so bulk
        # staging is now merge-safe. Enable bulk so staging runs in batches.
        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "enable_bulk_orm": "true",
        }

        # Stage every workload into the single branch. Reuse the proven per-shard
        # staging path (active_branch + current_request, per-object apply ->
        # branch ObjectChanges); all plan items land in ONE branch. There is no
        # every plan item targets the same branch. Optional budget splitting
        # partitions oversized workloads for bounded staging/progress units.
        max_changes_per_branch = self.sync.get_max_changes_per_branch()
        warned_models = set()
        for workload in workloads:
            if workload.estimated_changes > max_changes_per_branch:
                self.logger.log_warning(
                    f"{workload.model_string} workload has "
                    f"{workload.estimated_changes} estimated changes, exceeding "
                    f"the branch budget of {max_changes_per_branch}."
                )
                warned_models.add(workload.model_string)
        if self.sync.parameters.get("enable_branch_budget_split"):
            oversized_bucket_policy = (
                "fail"
                if self.sync.parameters.get("branch_budget_enforcement") == "strict"
                else "warn"
            )
            plan = build_branch_plan(
                workloads,
                max_changes_per_branch=max_changes_per_branch,
                oversized_bucket_policy=oversized_bucket_policy,
            )
        else:
            plan = build_branch_plan(workloads)
        for item in plan:
            if (
                item.estimated_changes > max_changes_per_branch
                and item.model_string not in warned_models
            ):
                self.logger.log_warning(
                    f"{item.model_string} plan item has {item.estimated_changes} "
                    f"estimated changes, exceeding the branch budget of "
                    f"{max_changes_per_branch}."
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
            self._set_runtime_phase(
                "primary-ip",
                "Resolving device primary IPs from Mgmt_ tags.",
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
        self._set_runtime_phase("merging", "Bulk-merging the single sync branch.")
        ingestion.sync_merge(remove_branch=True)
        self.sync.clear_branch_run_state()
        self.logger.log_info(
            "Forward single-branch ingestion completed.", obj=ingestion
        )
        return [ingestion]
