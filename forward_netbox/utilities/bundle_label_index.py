# Where each row a customer reads off the sync's Health, Scope Reconciliation and
# Drift report pages lives in the support bundle, by the page's own label.
#
# A customer quotes a figure by its label ("Carry no include tag", "Deleted
# objects"); the bundle stores it under the field name the code uses. This index
# is the lookup, and it ships inside the bundle (`ui_label_index`) so a reader can
# find any figure from the file alone. `test_bundle_label_index` fails when a row
# is added to a page without an entry here, or an entry outlives its row.
#
# A path is dotted from the bundle's top level. `<integration>` stands for one key
# of `health.optional_plugin_capabilities_ui`: those rows repeat per integration.
# A label that appears on several cards of one page ("Status" on Health) maps to a
# list of paths, in page order. The per-model "In sync" column of the Drift report
# is in `latest_dependency_preview.drift_table`, under the same words.
#
# The export filter renames some keys, so a path is the name the bundle holds, not
# the one the page reads: tag names become `include_tag_count`, and a key ending
# `_parameters` is renamed `*_names` and dropped.
#
# "Query parameters" (Health) reads `step_query_parameters`, which no code fills
# in, so the page always says none were recorded and the export filter would drop
# the key anyway; it points at the section so the label still leads somewhere.

PAGE_LABEL_PATHS = {
    "Health": {
        "Source status": "health.source.status",
        "Source URL": "health.source.url",
        "Branching available": "health.runtime.branching_available",
        "Max changes per staging item": "health.runtime.max_changes_per_staging_item",
        "Forward API timeout": "health.runtime.source_timeout_seconds",
        "Worker timeout": "health.runtime.rq_default_timeout_seconds",
        "Pushdown alert thresholds": "health.runtime.pushdown_alert_thresholds.fallback_warn_rate",
        "Snapshot selection": "health.runtime.snapshot_selector",
        "Next run": "health.next_run.message",
        "Budget status": "health.api_usage.budget.status",
        "Configured rate": "health.api_usage.budget.metrics.configured_requests_per_minute",
        "Observed rate": "health.api_usage.budget.metrics.observed_http_attempts_per_minute",
        "HTTP attempts": "health.api_usage.counters.http_attempts",
        "HTTP 429 failures": "health.api_usage.counters.http_429_failures",
        "NQE calls": "health.api_usage.counters.nqe_query_calls",
        "Throttle sleep": "health.api_usage.budget.metrics.throttle_sleep_seconds",
        "Query parameters": "health.api_usage",
        "Warnings": "health.api_usage.budget.warnings",
        "Failure reasons": "health.api_usage.budget.failure_reasons",
        "Models primed": "health.dependency_lookup_cache.model_count",
        "Rows processed": "health.dependency_lookup_cache.row_count",
        "Primed targets": "health.dependency_lookup_cache.primed_target_count",
        "Raw query maps": "health.query_modes.query",
        "Direct query ID maps": "health.query_modes.query_id",
        "Repository path maps": "health.query_modes.query_path",
        "Diff-capable maps": "health.query_modes.diff_capable",
        "Data-file-dependent maps": "health.query_modes.data_file_maps",
        "Query path specs": "health.query_path_resolution.total_query_path_specs",
        "Resolved specs": "health.query_path_resolution.resolved_spec_count",
        "Maps assessed": "health.query_drift_summary.total_maps",
        "Warn / info / pass": "health.query_drift_summary.warn_count",
        "Selected engines": "health.apply_engines.selected",
        "Fallback reasons": "health.apply_engines.fallback_reasons",
        "Fetch modes": "health.fetch_contracts.modes",
        "Shard-safe models": "health.fetch_contracts.shard_safe_count",
        "Model-fetch fallbacks": "health.fetch_contracts.model_fallback_count",
        "Backfilled devices": "health.collection_gap.backfilled_count",
        "Status": [
            "health.collection_gap.message",
            "health.out_of_scope.message",
            "health.uncovered.message",
            "health.quarantine_cadence.message",
        ],
        "Out-of-scope devices": "health.out_of_scope.out_of_scope_count",
        "Uncovered devices": "health.uncovered.uncovered_count",
        "Tracked models": "health.density_learning.model_count",
        "High confidence models": "health.density_learning.high_confidence_count",
        "Medium confidence models": "health.density_learning.medium_confidence_count",
        "Low confidence models": "health.density_learning.low_confidence_count",
        "Latest validation": "health.latest_validation.status",
        "Latest ingestion": "health.latest_ingestion.name",
        "Latest branch": "health.latest_ingestion.branch",
        "Ownership finalization": "health.ownership_finalization.failed_domains",
        "Phase": "health.delete_wave.phase",
        "Summary": "health.delete_wave.message",
        "Deleted objects": "health.delete_wave.latest_ingestion.deleted_change_count",
        "Dependency skip issues": "health.delete_wave.latest_ingestion.dependency_skip_issues.count",
        "Branch": "health.latest_ingestion.branch",
        "Branch status": "health.latest_ingestion.branch_status",
        "Stage job": "health.latest_ingestion.stage_job_status",
        "Merge job": "health.latest_ingestion.merge_job_status",
        "Applied / failed / skipped changes": "health.latest_ingestion.applied_change_count",
        "Ingestion issues": "health.latest_ingestion.issue_count",
        "Last run": "health.config_backup.last_run_status",
        "Managed devices": "health.config_backup.last_result.scoped_devices",
        "Managed devices with no config from Forward": "health.config_backup.last_result.scoped_without_config",
        "Rows fetched": "health.config_backup.last_result.rows",
        "Written / unchanged / unmapped": "health.config_backup.last_result.written",
        "Unmanaged written / unchanged": "health.config_backup.last_result.unmanaged_written",
        "Files in the backup folder": "health.config_backup.last_result.files_in_folder",
        "Snapshot": "health.config_backup.last_result.snapshot_id",
        "Commit / pushed": "health.config_backup.last_result.commit",
        "Duration (seconds)": "health.config_backup.last_result.duration_seconds",
        "Validity device_config_path": "health.config_backup.expected_device_config_path",
        "Source": "health.source.name",
        "Integration": "health.optional_plugin_capabilities_ui.<integration>.app_label",
        "Installed": "health.optional_plugin_capabilities_ui.<integration>.installed",
        "Installed version": "health.optional_plugin_capabilities_ui.<integration>.version",
        "Required version": "health.optional_plugin_capabilities_ui.<integration>.required_version",
        "Package": "health.optional_plugin_capabilities_ui.<integration>.package_name",
        "Available": "health.optional_plugin_capabilities_ui.<integration>.available",
        "Availability status": "health.optional_plugin_capabilities_ui.<integration>.availability_status",
        "Availability reason": "health.optional_plugin_capabilities_ui.<integration>.availability_reason",
        "Version matches": "health.optional_plugin_capabilities_ui.<integration>.version_matches",
        "Required models missing": "health.optional_plugin_capabilities_ui.<integration>.missing_required",
        "Command inventory entries": "health.optional_plugin_capabilities_ui.<integration>.command_inventory_count",
        "Remediation actions": "health.query_drift_summary.remediation_actions",
        "Thresholds": "health.quarantine_cadence.required_runs",
        "Effective protection": "health.quarantine_cadence.effective_hours",
        "Note": "health.config_backup.last_result",
    },
    "Scope Reconciliation": {
        "Include tags": "scope_reconciliation.report.include_tag_count",
        "NetBox devices": "scope_reconciliation.report.netbox_device_count",
        "In scope (collected)": "scope_reconciliation.report.forward_in_scope_completed",
        "In scope (SNMP endpoints)": "scope_reconciliation.report.forward_in_scope_endpoints",
        "Tagged but backfilled": "scope_reconciliation.report.forward_tagged_backfilled",
        "Out of scope (orphans)": "scope_reconciliation.report.netbox_out_of_scope",
        "Empty orphan sites": "scope_reconciliation.report.netbox_empty_orphan_site_count",
        "In scope, missing from NetBox": "scope_reconciliation.report.forward_missing_in_netbox",
        "Carry no include tag": "scope_reconciliation.report.unmanaged.untagged_total",
        "Platforms with manufacturer": "upgrade_reconciliation.platforms.with_manufacturer",
        "Platforms without manufacturer": "upgrade_reconciliation.platforms.without_manufacturer",
        "Stale endpoint DeviceTypes": "upgrade_reconciliation.stale_endpoint_device_types.candidate_count",
        "DLM status": "upgrade_reconciliation.dlm.status",
        "Software versions": "upgrade_reconciliation.dlm.software_versions.total",
        "Versions without devices": "upgrade_reconciliation.dlm.software_versions.without_devices",
        "Protected versions without devices": "upgrade_reconciliation.dlm.software_versions.protected_without_devices",
        "Unreferenced version candidates": "upgrade_reconciliation.dlm.software_versions.unreferenced_without_devices",
        "CVEs": "upgrade_reconciliation.dlm.cves.total",
        "CVEs with vulnerabilities": "upgrade_reconciliation.dlm.cves.with_vulnerabilities",
        "CVEs with affected software": "upgrade_reconciliation.dlm.cves.with_affected_software",
        "Unlinked CVEs": "upgrade_reconciliation.dlm.cves.unlinked",
        "Mode": "upgrade_reconciliation",
    },
    "Drift report": {
        "Models compared": "latest_dependency_preview.drift_report.measured_model_count",
        "Comparison cost": "latest_dependency_preview.drift_report.comparison_runtime_ms",
        "Drifted models": "latest_dependency_preview.drift_report.drifted_model_count",
        "Total drift": "latest_dependency_preview.drift_report.total_drift",
        "Estimated apply work": "latest_dependency_preview.drift_report.total_apply_work",
        "From preview": "latest_dependency_preview.drift_report.generated_at",
        "Sync snapshot": "latest_dependency_preview.latest_sync_evidence.snapshot_id",
        "Applied": "latest_dependency_preview.latest_sync_evidence.applied",
        "Created": "latest_dependency_preview.latest_sync_evidence.created",
        "Updated": "latest_dependency_preview.latest_sync_evidence.updated",
        "Deleted": "latest_dependency_preview.latest_sync_evidence.deleted",
        "Failed": "latest_dependency_preview.latest_sync_evidence.failed",
        "Skipped (rule rejections)": "latest_dependency_preview.latest_sync_evidence.skipped",
        "Completed": "latest_dependency_preview.latest_sync_evidence.completed_at",
        "In sync": "latest_dependency_preview.drift_report.in_sync",
        "Not drift": "latest_dependency_preview.drift_report.total_already_removed",
        "Convergence": "latest_dependency_preview.latest_sync_evidence.convergence_confirmed",
        "Ownership": "latest_dependency_preview.latest_sync_evidence.ownership",
        "Same as preview": "latest_dependency_preview.latest_sync_evidence.same_snapshot",
    },
}


def ui_label_index():
    """The index as the bundle carries it: page -> label -> bundle path(s)."""
    return {page: dict(rows) for page, rows in PAGE_LABEL_PATHS.items()}
