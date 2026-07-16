import json
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import override_settings
from django.test import SimpleTestCase
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from forward_netbox.choices import forward_configured_models
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardNQEMap
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.branch_budget import BranchPlanItem
from forward_netbox.utilities.health import live_data_file_health_check
from forward_netbox.utilities.health import live_source_health_check
from forward_netbox.utilities.health import sync_health_summary
from forward_netbox.utilities.health_checks import ingestion_check_message
from forward_netbox.utilities.health_checks import ingestion_check_status
from forward_netbox.utilities.health_checks import query_drift_check_message
from forward_netbox.utilities.health_summary_blocks import large_run_tuning_summary
from forward_netbox.utilities.query_registry import read_compiled_builtin_query_source


BGP_PLUGIN_CONFIG = {
    **settings.PLUGINS_CONFIG,
    "forward_netbox": {
        **settings.PLUGINS_CONFIG.get("forward_netbox", {}),
        "enable_bgp_sync": True,
    },
}


class HealthCheckMessageTest(SimpleTestCase):
    def test_direct_query_id_guidance_uses_publish_workflow(self):
        message = query_drift_check_message(
            [{"severity": "info"}],
            query_drift_summary={"remediation_actions": []},
        )

        self.assertIn("Publish Bundled Queries", message)
        self.assertIn("live repository paths", message)
        self.assertNotIn("Refresh Query IDs", message)


class ForwardSyncHealthTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()
        cls.user = User.objects.create_superuser(
            username="health-admin",
            password="TestPassword123!",
            email="health-admin@example.com",
        )
        cls.source = ForwardSource.objects.create(
            name="health-source",
            type="saas",
            url="https://fwd.app",
            status=ForwardSourceStatusChoices.READY,
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
                "timeout": 1200,
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="health-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        cls.sync.set_model_change_density({"dcim.device": 1.6, "dcim.cable": 2.4})
        cls.sync.set_model_change_density_profile(
            {
                "dcim.device": {
                    "density": 1.6,
                    "sample_count": 6,
                    "accepted_observations": 6,
                    "rejected_observations": 1,
                    "mean": 1.5,
                    "m2": 0.12,
                    "variance": 0.024,
                    "stddev": 0.154919,
                    "last_updated_at": timezone.now().isoformat(),
                },
                "dcim.cable": {
                    "density": 2.4,
                    "sample_count": 2,
                    "accepted_observations": 2,
                    "rejected_observations": 0,
                    "mean": 2.4,
                    "m2": 0.0,
                    "variance": 0.0,
                    "stddev": 0.0,
                    "last_updated_at": timezone.now().isoformat(),
                },
            }
        )
        ForwardNQEMap.objects.update(enabled=False)
        site_type = ContentType.objects.get(app_label="dcim", model="site")
        device_type = ContentType.objects.get(app_label="dcim", model="device")
        ForwardNQEMap.objects.create(
            name="Health Sites",
            netbox_model=site_type,
            query_id="query-sites",
            enabled=True,
            weight=10,
        )
        ForwardNQEMap.objects.create(
            name="Health Devices with NetBox Device Type Aliases",
            netbox_model=device_type,
            query_path="/forward_netbox_validation/forward_devices",
            query_repository="org",
            enabled=True,
            weight=20,
        )
        ForwardValidationRun.objects.create(
            sync=cls.sync,
            status="passed",
            allowed=True,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )
        ForwardIngestion.objects.create(
            sync=cls.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
            baseline_ready=True,
            applied_change_count=2,
            model_results=[
                {
                    "model": "dcim.device",
                    "query_name": "Health Devices with NetBox Device Type Aliases",
                    "row_count": 3,
                    "delete_count": 1,
                    "execution_mode": "query_path",
                    "fetch_mode": "nqe_parameters",
                    "query_path_resolution": {
                        "available": True,
                        "query_path_spec_count": 1,
                        "artifact_hit_count": 1,
                        "client_resolve_count": 0,
                        "repository_index_count": 1,
                        "cache_hit_rate": 1.0,
                    },
                }
            ],
        )
        now = timezone.now()
        cls.execution_job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=cls.sync.pk,
            name="health execution job",
            user=None,
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174099",
            created=now,
            started=now,
            completed=now,
            data={
                "forward_api_usage": {
                    "api_requests_per_minute": 1800,
                    "http_attempts": 21,
                    "http_429_failures": 0,
                    "nqe_query_calls": 4,
                    "nqe_diff_calls": 3,
                    "nqe_pages": 9,
                    "throttle_sleep_seconds": 1.5,
                    "usage_window_seconds": 60.0,
                    "observed_http_attempts_per_minute": 20.0,
                },
                "statistics": {
                    "dcim.cable": {
                        "current": 4,
                        "total": 4,
                        "applied": 1,
                        "failed": 0,
                        "skipped": 0,
                        "unchanged": 2,
                    }
                },
                "dependency_lookup_cache": {
                    "available": True,
                    "row_count": 4,
                    "primed_target_count": 7,
                    "model_count": 1,
                    "models": [
                        {
                            "model": "dcim.device",
                            "row_count": 4,
                            "primed_target_count": 7,
                            "device_name_count": 4,
                            "tag_row_count": 0,
                            "interface_pair_count": 2,
                            "module_bay_pair_count": 0,
                            "fhrp_group_count": 1,
                            "ipam_identity_row_count": 0,
                            "ipam_global_host_row_count": 0,
                        }
                    ],
                },
            },
        )
        cls.sync.last_ingestion.job = cls.execution_job
        cls.sync.last_ingestion.save(update_fields=["job"])
        cls.sync.refresh_from_db()

    def test_collection_gap_summary_reflects_backfilled_tag(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from extras.models import Tag

        summary = sync_health_summary(self.sync)
        self.assertEqual(summary["collection_gap"]["status"], "info")
        self.assertEqual(summary["collection_gap"]["backfilled_count"], 0)

        mfr = Manufacturer.objects.create(name="MfrCG", slug="mfr-cg")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-cg", slug="dt-cg")
        role = DeviceRole.objects.create(name="RoleCG", slug="role-cg")
        site = Site.objects.create(name="SiteCG", slug="site-cg")
        dev = Device.objects.create(name="dev-cg", device_type=dt, role=role, site=site)
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        dev.tags.add(tag)

        summary2 = sync_health_summary(self.sync)
        self.assertEqual(summary2["collection_gap"]["status"], "warn")
        self.assertEqual(summary2["collection_gap"]["backfilled_count"], 1)
        self.assertIsNone(summary2["collection_gap"]["trend_delta"])

    def test_out_of_scope_summary_reflects_tag(self):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from extras.models import Tag

        summary = sync_health_summary(self.sync)
        self.assertEqual(summary["out_of_scope"]["status"], "info")
        self.assertEqual(summary["out_of_scope"]["out_of_scope_count"], 0)

        mfr = Manufacturer.objects.create(name="MfrOS", slug="mfr-os")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-os", slug="dt-os")
        role = DeviceRole.objects.create(name="RoleOS", slug="role-os")
        site = Site.objects.create(name="SiteOS", slug="site-os")
        dev = Device.objects.create(name="dev-os", device_type=dt, role=role, site=site)
        tag = Tag.objects.create(
            name="Forward Out Of Scope", slug="forward-out-of-scope"
        )
        dev.tags.add(tag)

        summary2 = sync_health_summary(self.sync)
        self.assertEqual(summary2["out_of_scope"]["status"], "warn")
        self.assertEqual(summary2["out_of_scope"]["out_of_scope_count"], 1)

    def test_collection_gap_escalates_when_growing(self):
        from core.choices import JobStatusChoices
        from core.models import Job
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag

        mfr = Manufacturer.objects.create(name="MfrCG2", slug="mfr-cg2")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-cg2", slug="dt-cg2")
        role = DeviceRole.objects.create(name="RoleCG2", slug="role-cg2")
        site = Site.objects.create(name="SiteCG2", slug="site-cg2")
        dev = Device.objects.create(
            name="dev-cg2", device_type=dt, role=role, site=site
        )
        tag = Tag.objects.create(name="Forward Backfilled", slug="forward-backfilled")
        dev.tags.add(tag)

        # Two prior tag reconciliations recorded a growing gap (18 -> 72).
        from uuid import uuid4

        content_type = ContentType.objects.get_for_model(self.sync.__class__)
        for total in (18, 72):
            Job.objects.create(
                object_type=content_type,
                object_id=self.sync.pk,
                name="tag backfilled devices",
                status=JobStatusChoices.STATUS_COMPLETED,
                data={"total_backfilled": total},
                job_id=uuid4(),
            )

        summary = sync_health_summary(self.sync)["collection_gap"]
        self.assertEqual(summary["status"], "danger")
        self.assertEqual(summary["trend_delta"], 54)
        self.assertIn("Up 54", summary["message"])

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_dependency_preflight_warns_for_interface_without_bgp_models(self):
        sync = self._sync_with_enabled_models(
            "health-sync-interface-no-bgp",
            ["dcim.interface", "ipam.ipaddress"],
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-dependency",
            baseline_ready=True,
        )

        summary = sync_health_summary(sync)
        preflight = summary["dependency_preflight"]

        self.assertEqual(preflight["status"], "warn")
        self.assertTrue(preflight["delete_or_prune_possible"])
        self.assertIn(
            "baseline_ready_for_diff_deletes",
            preflight["delete_or_prune_evidence"],
        )
        interface_warning = next(
            item
            for item in preflight["warnings"]
            if item["selected_model"] == "dcim.interface"
        )
        self.assertEqual(
            interface_warning["omitted_models"],
            [
                "netbox_routing.bgppeer",
                "netbox_routing.bgppeeraddressfamily",
                "netbox_peering_manager.peeringsession",
            ],
        )
        self.assertIn(
            "netbox_routing.bgppeer",
            interface_warning["suggested_models"],
        )
        self.assertIsNotNone(interface_warning["delete_dependency_rank"])
        dependency_check = next(
            item
            for item in summary["checks"]
            if item["name"] == "Scoped dependency preflight"
        )
        self.assertEqual(dependency_check["status"], "warn")
        self.assertIn("netbox_routing.bgppeer", dependency_check["message"])

    def test_dependency_preflight_warns_for_interface_without_device_model(self):
        sync = self._sync_with_enabled_models(
            "health-sync-interface-no-device",
            ["dcim.interface"],
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-dependency-device",
            baseline_ready=True,
        )

        summary = sync_health_summary(sync)
        preflight = summary["dependency_preflight"]

        self.assertEqual(preflight["status"], "warn")
        self.assertEqual(preflight["apply_dry_run"]["status"], "warn")
        self.assertIn(
            {
                "model": "dcim.interface",
                "parent_model": "dcim.device",
                "model_apply_rank": preflight["apply_dry_run"]["missing_dependencies"][
                    0
                ]["model_apply_rank"],
                "parent_apply_rank": preflight["apply_dry_run"]["missing_dependencies"][
                    0
                ]["parent_apply_rank"],
                "message": preflight["apply_dry_run"]["missing_dependencies"][0][
                    "message"
                ],
            },
            preflight["apply_dry_run"]["missing_dependencies"],
        )
        interface_warning = next(
            item
            for item in preflight["warnings"]
            if item["selected_model"] == "dcim.interface"
            and item["code"] == "parent_device_model_omitted"
        )
        self.assertEqual(interface_warning["omitted_models"], ["dcim.device"])
        self.assertEqual(interface_warning["suggested_models"], ["dcim.device"])
        self.assertIn("dcim.device", interface_warning["message"])
        dependency_check = next(
            item
            for item in summary["checks"]
            if item["name"] == "Scoped dependency preflight"
        )
        self.assertEqual(dependency_check["status"], "warn")
        self.assertIn("dcim.device", dependency_check["message"])

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_dependency_preflight_passes_when_routing_models_are_enabled(self):
        sync = self._sync_with_enabled_models(
            "health-sync-interface-with-bgp",
            [
                "dcim.site",
                "dcim.manufacturer",
                "dcim.devicerole",
                "dcim.platform",
                "dcim.devicetype",
                "dcim.device",
                "dcim.interface",
                "ipam.vrf",
                "ipam.ipaddress",
                "netbox_routing.bgppeer",
                "netbox_routing.bgppeeraddressfamily",
                "netbox_peering_manager.peeringsession",
            ],
        )

        summary = sync_health_summary(sync)

        self.assertEqual(summary["dependency_preflight"]["status"], "pass")
        self.assertEqual(
            summary["dependency_preflight"]["apply_dry_run"]["status"],
            "pass",
        )
        self.assertEqual(
            summary["dependency_preflight"]["apply_dry_run"][
                "missing_dependency_count"
            ],
            0,
        )
        self.assertEqual(summary["dependency_preflight"]["warnings"], [])
        dependency_check = next(
            item
            for item in summary["checks"]
            if item["name"] == "Scoped dependency preflight"
        )
        self.assertEqual(dependency_check["status"], "pass")

    @override_settings(PLUGINS_CONFIG=BGP_PLUGIN_CONFIG)
    def test_sync_health_view_renders_dependency_preflight_warning(self):
        sync = self._sync_with_enabled_models(
            "health-sync-dependency-view",
            ["dcim.interface"],
        )
        ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-dependency-view",
            baseline_ready=True,
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": sync.pk},
            )
        )

        self.assertContains(response, "Scoped dependency preflight")
        self.assertContains(response, "netbox_routing.bgppeer")

    def test_sync_health_summary_reports_optional_plugin_capabilities(self):
        summary = sync_health_summary(self.sync)
        self.assertIn("optional_plugin_capabilities", summary)
        self.assertIn("optional_plugin_capabilities_ui", summary)
        self.assertIn(
            "aci.netbox_cisco_aci",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn(
            "routing.netbox_routing",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn(
            "peering.netbox_peering_manager",
            summary["optional_plugin_capabilities"],
        )
        self.assertIn("aci", summary["optional_plugin_capabilities_ui"])
        self.assertIn("routing", summary["optional_plugin_capabilities_ui"])
        self.assertIn("peering", summary["optional_plugin_capabilities_ui"])
        self.assertIn(
            "availability_status",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "availability_reason",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "version",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "minimum_version",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "package_names",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "installed_package_name",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertIn(
            "command_inventory",
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"],
        )
        self.assertEqual(
            summary["optional_plugin_capabilities"]["aci.netbox_cisco_aci"][
                "command_inventory_count"
            ],
            17,
        )

    @override_settings(RQ_DEFAULT_TIMEOUT=100)
    def test_large_run_tuning_advises_fast_bootstrap_on_timeout_risk(self):
        summary = large_run_tuning_summary(
            self.sync,
            capacity={
                "available": True,
                "total_steps": 50,
                "remaining_steps": 49,
                "projected_remaining_seconds": 120,
            },
            query_pushdown={
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {"diff_actual_ratio": None},
                "tuning_guidance": [],
            },
        )

        self.assertEqual(summary["status"], "warn")
        self.assertEqual(
            summary["execution_backend_advice"]["code"],
            "branching_timeout_risk_consider_bootstrap",
        )
        self.assertEqual(
            summary["execution_backend_advice"]["recommended_backend"],
            "fast_bootstrap",
        )
        self.assertEqual(
            summary["first_order_actions"][0]["code"],
            "consider_fast_bootstrap_for_trusted_baseline",
        )

    def test_large_run_tuning_recommends_safe_speed_options(self):
        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "execution_backend": "branching",
            "auto_merge": True,
            "enable_bulk_orm": False,
            "scheduler_overlap": False,
        }
        self.sync.auto_merge = True

        summary = large_run_tuning_summary(
            self.sync,
            capacity={
                "available": True,
                "total_steps": 50,
                "remaining_steps": 40,
                "projected_remaining_seconds": 60,
            },
            query_pushdown={
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {"diff_actual_ratio": None},
                "tuning_guidance": [],
            },
        )

        action_codes = {item["code"] for item in summary["first_order_actions"]}
        self.assertIn("enable_safe_bulk_orm", action_codes)
        self.assertIn("consider_scheduler_overlap", action_codes)

    def test_large_run_tuning_advises_switch_back_after_fast_bootstrap(self):
        self.sync.parameters["execution_backend"] = "fast_bootstrap"

        summary = large_run_tuning_summary(
            self.sync,
            capacity={"available": False},
            query_pushdown={
                "efficiency": {"fallback_steps": 0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
        )

        self.assertEqual(
            summary["execution_backend_advice"]["code"],
            "fast_bootstrap_baseline_active",
        )
        self.assertEqual(
            summary["execution_backend_advice"]["next_backend"],
            "branching",
        )
        self.assertEqual(
            summary["first_order_actions"][0]["code"],
            "complete_fast_bootstrap_then_branching",
        )

    def test_adaptive_capacity_recommends_one_tuning_batch(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-recommend",
            {
                "timeout": 1200,
                "query_fetch_concurrency": 8,
                "nqe_page_size": 8000,
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                    "worker_headroom": "available",
                    "queue_backlog_depth": 3,
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 80, "remaining_steps": 70},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 0.5,
                "bottleneck_phase": "fetch",
            },
        )

        adaptive = summary["adaptive_capacity"]
        batch = adaptive["next_tuning_batch"]

        self.assertEqual(adaptive["status"], "warn")
        self.assertEqual(adaptive["decision"], "recommend_tuning_batch")
        self.assertEqual(batch["worker_count"]["recommended"], 18)
        self.assertEqual(batch["query_fetch_concurrency"]["recommended"], 10)
        self.assertEqual(batch["nqe_page_size"]["recommended"], 9600)
        self.assertEqual(batch["restart_scope"], "restart_workers_only")
        self.assertEqual(batch["hold_minutes"], 60)

    def test_adaptive_capacity_holds_when_throughput_is_healthy(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-hold",
            {
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 20, "remaining_steps": 10},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 6.0,
                "shards_per_hour_6h": 5.5,
                "issue_rate_per_hour": 0.0,
                "bottleneck_phase": "apply",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "pass")
        self.assertEqual(adaptive["decision"], "hold_current_settings")

    def test_adaptive_capacity_rolls_back_on_issue_spike(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-rollback",
            {
                "runtime_capacity_evidence": {
                    "active_worker_count": 12,
                    "database_headroom": "available",
                },
            },
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 20, "remaining_steps": 10},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 3.0,
                "bottleneck_phase": "apply",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "warn")
        self.assertEqual(adaptive["decision"], "rollback_latest_tuning_batch")

    def test_adaptive_capacity_requires_worker_and_database_evidence(self):
        sync = self._sync_with_source_parameters(
            "health-sync-adaptive-insufficient",
            {"query_fetch_concurrency": 8, "nqe_page_size": 8000},
        )

        summary = large_run_tuning_summary(
            sync,
            capacity={"available": True, "total_steps": 80, "remaining_steps": 70},
            query_pushdown={
                "available": True,
                "efficiency": {"fallback_steps": 0, "fallback_rate": 0.0},
                "runtime_share": {},
                "diff_utilization": {},
                "tuning_guidance": [],
            },
            throughput={
                "available": True,
                "shards_per_hour_1h": 2.0,
                "shards_per_hour_6h": 2.0,
                "issue_rate_per_hour": 0.5,
                "bottleneck_phase": "fetch",
            },
        )

        adaptive = summary["adaptive_capacity"]

        self.assertEqual(adaptive["status"], "info")
        self.assertEqual(adaptive["decision"], "insufficient_evidence")
        self.assertEqual(adaptive["capacity_evidence"]["status"], "unknown")

    def test_promoted_bulk_orm_models_run_without_allowlist(self):
        # ipam.ipaddress, dcim.interface, and ipam.prefix are all in the default
        # safe set now, so with bulk enabled they run bulk without an explicit
        # allowlist and are not held back as `bulk_orm_model_not_allowlisted`.
        # There are no remaining experimental models, so the not-allowlisted gate
        # no longer applies to any built-in model.
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.save(update_fields=["parameters"])

        summary = sync_health_summary(self.sync)
        self.assertNotIn(
            "bulk_orm_model_not_allowlisted",
            summary["apply_engines"]["global_fallback_reasons"],
        )

    def test_allowlisted_experimental_bulk_orm_model_has_no_gap(self):
        # Once explicitly allowlisted, the experimental model is no longer held
        # back as not-allowlisted.
        self.sync.parameters["enable_bulk_orm"] = True
        self.sync.parameters["bulk_orm_models"] = ["ipam.ipaddress", "dcim.interface"]
        self.sync.save(update_fields=["parameters"])

        summary = sync_health_summary(self.sync)
        self.assertNotIn(
            "bulk_orm_model_not_allowlisted",
            summary["apply_engines"]["global_fallback_reasons"],
        )

    def test_sync_health_summary_reports_next_run_blockers(self):
        platform_type = ContentType.objects.get(app_label="dcim", model="platform")
        raw_map = ForwardNQEMap.objects.create(
            name="Health Raw Platforms",
            netbox_model=platform_type,
            query="select {}",
            enabled=True,
            weight=30,
        )
        sync = ForwardSync.objects.create(
            name="health-sync-blocked",
            source=self.source,
            parameters={
                "snapshot_id": "fixed-snapshot",
                "dcim.platform": True,
            },
        )

        summary = sync_health_summary(sync)

        self.assertEqual(summary["next_run"]["mode"], "full_or_reconciliation")
        self.assertIn("snapshot_selector_is_fixed", summary["next_run"]["reasons"])
        self.assertIn("no_baseline_ready_ingestion", summary["next_run"]["reasons"])
        self.assertIn(
            "raw_query_maps_cannot_use_forward_diffs",
            summary["next_run"]["reasons"],
        )
        blockers = summary["next_run"]["blockers"]
        self.assertEqual(
            {blocker["reason"] for blocker in blockers},
            {
                "snapshot_selector_is_fixed",
                "no_baseline_ready_ingestion",
                "raw_query_maps_cannot_use_forward_diffs",
            },
        )
        map_blocker = next(blocker for blocker in blockers if blocker["scope"] == "map")
        self.assertEqual(map_blocker["map"], raw_map.name)
        self.assertEqual(map_blocker["model"], "dcim.platform")

        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": sync.pk},
            )
        )
        self.assertContains(
            response, "Raw query text maps cannot use Forward nqe-diffs"
        )

    def test_live_source_health_check_reports_reachability_without_ids(self):
        client = Mock()
        client.get_networks.return_value = [
            {"id": "test-network", "name": "Visible Network"},
        ]
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_source_health_check(self.sync)

        self.assertTrue(result["reachable"])
        self.assertTrue(result["configured_network_id_present"])
        self.assertTrue(result["configured_network_visible"])
        self.assertTrue(result["latest_processed_snapshot_available"])
        self.assertNotIn("test-network", json.dumps(result))
        self.assertNotIn("snapshot-1", json.dumps(result))

    def test_sync_live_source_health_downloads_reachability_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_networks.return_value = [
            {"id": "test-network", "name": "Visible Network"},
        ]
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_source_health",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertTrue(data["source_health"]["reachable"])
        self.assertTrue(data["source_health"]["configured_network_visible"])
        self.assertTrue(data["source_health"]["latest_processed_snapshot_available"])

    def test_live_data_file_health_check_reports_snapshot_captured_rows(self):
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": True,
                "row_count": 42,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_data_file_health_check(self.sync)

        self.assertEqual(
            result["required_data_files"],
            ["netbox_device_type_aliases"],
        )
        self.assertEqual(result["results"][0]["status"], "present")
        self.assertEqual(result["results"][0]["row_count"], 42)
        self.assertNotIn("test-network", json.dumps(result))
        self.assertNotIn("snapshot-1", json.dumps(result))
        client.run_nqe_query.assert_called_once()
        probe_query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn("network.extensions.netbox_device_type_aliases", probe_query)

    def test_live_data_file_health_check_reports_missing_snapshot_value(self):
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": False,
                "row_count": 0,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            result = live_data_file_health_check(self.sync)

        self.assertEqual(result["results"][0]["status"], "not_captured")
        self.assertEqual(result["checks"][0]["status"], "warn")

    def test_sync_live_data_file_health_downloads_freshness_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_latest_processed_snapshot_id.return_value = "snapshot-1"
        client.run_nqe_query.return_value = [
            {
                "data_file": "netbox_device_type_aliases",
                "value_present": True,
                "row_count": 42,
            }
        ]

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_data_file_health",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertEqual(
            data["data_file_health"]["required_data_files"],
            ["netbox_device_type_aliases"],
        )
        self.assertEqual(data["data_file_health"]["results"][0]["status"], "present")

    def test_sync_live_query_drift_downloads_forward_checked_diagnostics(self):
        self.client.force_login(self.user)
        client = Mock()
        client.get_nqe_repository_queries.return_value = []
        client.get_nqe_repository_query_index.return_value = {"by_query_id": {}}
        client.get_committed_nqe_query.return_value = {
            "queryId": "Q_devices",
            "lastCommitId": "commit-1",
            "path": "/forward_netbox_validation/forward_devices",
            "sourceCode": read_compiled_builtin_query_source("forward_devices.nqe"),
        }

        with patch.object(ForwardSource, "get_client", return_value=client):
            response = self.client.get(
                reverse(
                    "plugins:forward_netbox:forwardsync_query_drift",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response["Content-Disposition"])
        data = json.loads(response.content)
        self.assertEqual(data["sync"]["pk"], self.sync.pk)
        self.assertEqual(data["query_drift_summary"]["total_maps"], 2)
        self.assertEqual(
            data["query_drift_summary"]["status_counts"],
            {
                "direct_query_id_unverified": 1,
                "repository_path_matches_bundled_filename": 1,
            },
        )
        self.assertEqual(data["query_drift_summary"]["warn_count"], 0)
        self.assertEqual(data["query_drift_summary"]["info_count"], 1)
        self.assertEqual(data["query_drift_summary"]["pass_count"], 1)
        self.assertEqual(len(data["results"]), 2)
        path_result = next(
            result for result in data["results"] if result["mode"] == "query_path"
        )
        self.assertEqual(path_result["status"], "live_repository_source_match")
        self.assertEqual(path_result["live_query_id"], "Q_devices")
        self.assertEqual(path_result["requested_commit_id"], "head")
        self.assertEqual(path_result["commit_binding"], "latest_commit")

    def test_sync_publish_bundled_queries_posts_to_repair_action(self):
        self.client.force_login(self.user)
        result = Mock(matched=True)
        client = Mock()
        with patch.object(ForwardSource, "get_client", return_value=client), patch(
            "forward_netbox.views.publish_builtin_nqe_map_queries",
            return_value=[result],
        ) as publish:
            response = self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_publish_bundled_queries",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response["Location"],
            reverse(
                "plugins:forward_netbox:forwardsync_health",
                kwargs={"pk": self.sync.pk},
            ),
        )
        publish.assert_called_once()
        self.assertEqual(publish.call_args.kwargs["client"], client)
        self.assertEqual(
            publish.call_args.kwargs["directory"], "/forward_netbox_validation/"
        )
        self.assertTrue(publish.call_args.kwargs["overwrite"])

    def test_sync_publish_bundled_queries_surfaces_write_permission_error(self):
        self.client.force_login(self.user)
        with patch.object(ForwardSource, "get_client", return_value=Mock()), patch(
            "forward_netbox.views.publish_builtin_nqe_map_queries",
            side_effect=Exception("403 Forbidden"),
        ):
            response = self.client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_publish_bundled_queries",
                    kwargs={"pk": self.sync.pk},
                ),
                follow=True,
            )

        self.assertContains(response, "NQE-library write permission")

    def test_sync_detail_surfaces_query_drift_and_dependency_preview_actions(self):
        self.client.force_login(self.user)

        response = self.client.get(self.sync.get_absolute_url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Query Drift")
        self.assertContains(response, "Publish Bundled Queries")
        self.assertNotContains(response, "Refresh Query IDs")
        self.assertContains(response, "Preview Dependencies")
        self.assertContains(
            response,
            reverse(
                "plugins:forward_netbox:forwardsync_dependency_preview",
                kwargs={"pk": self.sync.pk},
            ),
        )

    def _mock_dependency_preview_planner(self):
        planner = Mock()
        planner.build_plan.return_value = (
            {
                "network_id": "test-network",
                "snapshot_id": "snapshot-1",
                "snapshot_selector": "latestProcessed",
            },
            [
                BranchPlanItem(
                    index=1,
                    model_string="dcim.device",
                    label="Devices",
                    estimated_changes=3,
                    upsert_rows=[{"name": "device-a"}, {"name": "device-b"}],
                    delete_rows=[{"name": "device-c"}],
                    sync_mode="diff",
                    query_name="forward_devices",
                    execution_mode="query_id",
                    fetch_mode="diff",
                )
            ],
        )
        planner.model_results = [
            {
                "model": "dcim.device",
                "query_name": "forward_devices",
                "execution_mode": "query_id",
                "fetch_mode": "diff",
                "row_count": 2,
                "delete_count": 1,
                "estimated_changes": 3,
                "runtime_ms": 42.0,
            }
        ]
        return planner

    def test_compute_drift_report_summarizes_model_results(self):
        from forward_netbox.utilities.drift_report import compute_drift_report

        payload = {
            "generated_at": "t",
            "model_results": [
                {
                    "model": "dcim.device",
                    "row_count": 10,
                    "estimated_changes": 3,
                    "delete_count": 1,
                },
                {
                    "model": "ipam.prefix",
                    "row_count": 5,
                    "estimated_changes": 0,
                    "delete_count": 0,
                },
            ],
        }
        report = compute_drift_report(payload)
        self.assertFalse(report["in_sync"])
        self.assertEqual(report["total_drift"], 4)
        self.assertEqual(report["drifted_model_count"], 1)
        self.assertEqual(report["model_count"], 2)
        self.assertEqual(report["models"][0]["model"], "dcim.device")
        self.assertTrue(report["models"][1]["in_sync"])

    def test_compute_drift_report_does_not_label_workload_as_drift(self):
        from forward_netbox.utilities.drift_report import compute_drift_report

        report = compute_drift_report(
            {
                "model_results": [
                    {
                        "model": "netbox_dlm.devicesoftware",
                        "row_count": 3343,
                        "estimated_changes": 4963,
                        "delete_count": 1620,
                        "change_estimate_kind": "workload_upper_bound",
                    },
                    {
                        "model": "dcim.device",
                        "row_count": 4029,
                        "estimated_changes": 4029,
                        "delete_count": 0,
                        "change_estimate_kind": "workload_upper_bound",
                    },
                ]
            }
        )

        self.assertFalse(report["comparison_available"])
        self.assertIsNone(report["in_sync"])
        self.assertIsNone(report["total_drift"])
        self.assertIsNone(report["drifted_model_count"])
        self.assertEqual(report["total_upsert_candidates"], 7372)
        self.assertEqual(report["total_removes"], 1620)
        self.assertEqual(report["total_apply_work"], 8992)
        dlm = next(
            row
            for row in report["models"]
            if row["model"] == "netbox_dlm.devicesoftware"
        )
        self.assertEqual(dlm["pending_changes"], 3343)
        self.assertEqual(dlm["estimated_apply_work"], 4963)
        self.assertIsNone(dlm["drift"])

    def _create_dependency_preview_job(self, created_at):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174050",
            created=created_at,
            data={
                "generated_at": "t",
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 3,
                        "estimated_changes": 3,
                        "delete_count": 0,
                    }
                ],
            },
        )

    def test_drift_report_flags_stale_when_sync_ran_after_preview(self):
        # Regression (2.3.0 field report): the drift report is built from the cached
        # preview payload, so a sync run AFTER the preview leaves stale
        # "everything to create" numbers. Flag it.
        preview_at = timezone.now() - timezone.timedelta(hours=2)
        self._create_dependency_preview_job(preview_at)
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="s1",
            created=timezone.now(),
        )
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["drift_stale"])
        self.assertContains(response, "stale")

    def test_drift_report_not_stale_when_preview_is_newest(self):
        ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="s1",
            created=timezone.now() - timezone.timedelta(hours=2),
        )
        self._create_dependency_preview_job(timezone.now())
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["drift_stale"])

    def test_compute_drift_report_flags_full_create_empty_baseline(self):
        # Every model fully pending with no removals across >=3 models is the
        # empty/unmerged-baseline fingerprint (field report: 18/19 models all
        # showing pending == forward rows).
        from forward_netbox.utilities.drift_report import compute_drift_report

        report = compute_drift_report(
            {
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 10,
                        "estimated_changes": 10,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                    {
                        "model": "dcim.interface",
                        "row_count": 50,
                        "estimated_changes": 50,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                    {
                        "model": "ipam.prefix",
                        "row_count": 5,
                        "estimated_changes": 5,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                ]
            }
        )
        self.assertTrue(report["looks_like_full_create"])
        self.assertEqual(report["full_create_model_count"], 3)

    def test_compute_drift_report_not_full_create_on_real_delta(self):
        from forward_netbox.utilities.drift_report import compute_drift_report

        # A partial change or any removal is a genuine delta, not the fingerprint.
        report = compute_drift_report(
            {
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 10,
                        "estimated_changes": 2,
                        "delete_count": 0,
                    },
                    {
                        "model": "dcim.interface",
                        "row_count": 50,
                        "estimated_changes": 50,
                        "delete_count": 1,
                    },
                    {
                        "model": "ipam.prefix",
                        "row_count": 5,
                        "estimated_changes": 5,
                        "delete_count": 0,
                    },
                ]
            }
        )
        self.assertFalse(report["looks_like_full_create"])

    def test_compute_drift_report_not_full_create_below_model_floor(self):
        from forward_netbox.utilities.drift_report import compute_drift_report

        # A single tiny scope is not the multi-model empty-baseline signature.
        report = compute_drift_report(
            {
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 3,
                        "estimated_changes": 3,
                        "delete_count": 0,
                    },
                ]
            }
        )
        self.assertFalse(report["looks_like_full_create"])

    def test_drift_report_flags_stale_when_preview_is_old(self):
        # A preview older than a day is stale even when no sync ran after it —
        # the field case where a 4-day-old "everything to create" preview showed
        # no warning at all. Job.created is auto_now_add, so force the age with a
        # queryset update (a plain create() timestamp is ignored).
        job = self._create_dependency_preview_job(timezone.now())
        Job.objects.filter(pk=job.pk).update(
            created=timezone.now() - timezone.timedelta(days=2)
        )
        # No ingestion after the preview — isolate the absolute-age path from the
        # relative "newer sync ran" path.
        ForwardIngestion.objects.filter(sync=self.sync).delete()
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["drift_stale"])
        self.assertTrue(response.context["drift_stale_old_preview"])
        self.assertFalse(response.context["drift_stale_newer_sync"])
        self.assertContains(response, "over a day old")

    def test_drift_report_surfaces_full_create_hint(self):
        # The empty-baseline hint explains a 100%-pending preview so operators do
        # not read it as real drift. Fresh preview so the stale banner is absent.
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174051",
            created=timezone.now(),
            data={
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 10,
                        "estimated_changes": 10,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                    {
                        "model": "dcim.interface",
                        "row_count": 50,
                        "estimated_changes": 50,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                    {
                        "model": "ipam.prefix",
                        "row_count": 5,
                        "estimated_changes": 5,
                        "delete_count": 0,
                        "change_estimate_kind": "exact_comparison",
                    },
                ]
            },
        )
        self.client.force_login(self.user)
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["report"]["looks_like_full_create"])
        self.assertContains(response, "everything Forward has")

    def test_drift_report_surfaces_workload_as_not_measured(self):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174052",
            created=timezone.now(),
            data={
                "model_results": [
                    {
                        "model": "netbox_dlm.devicesoftware",
                        "row_count": 3343,
                        "estimated_changes": 4963,
                        "delete_count": 1620,
                        "change_estimate_kind": "workload_upper_bound",
                    }
                ]
            },
        )
        self.client.force_login(self.user)

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["report"]["comparison_available"])
        self.assertContains(response, "Not measured")
        self.assertContains(response, "estimated apply workload")

    def test_ingestion_health_check_marks_non_blocking_issue_baseline_as_pass(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
            baseline_ready=True,
            applied_change_count=5,
            failed_change_count=0,
        )
        ingestion.issues.create(
            model="ipam.ipaddress",
            message="Skipping delete for `ipam.ipaddress` due to protected dependencies.",
            exception="ForwardDependencySkipError",
        )

        self.assertEqual(ingestion_check_status(ingestion), "pass")
        self.assertIn("non-blocking", ingestion_check_message(ingestion))

    def test_ingestion_health_check_marks_blocking_issue_as_warn(self):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
            baseline_ready=True,
            applied_change_count=5,
            failed_change_count=0,
        )
        ingestion.issues.create(
            model="dcim.device",
            message="Unable to apply device row.",
            exception="ForwardSyncDataError",
        )

        self.assertEqual(ingestion_check_status(ingestion), "warn")
        self.assertIn("including blocking rows", ingestion_check_message(ingestion))

    def _sync_with_enabled_models(self, name, enabled_models):
        enabled_models = set(enabled_models)
        return ForwardSync.objects.create(
            name=name,
            source=self.source,
            parameters={
                "snapshot_id": "latestProcessed",
                **{
                    model_string: model_string in enabled_models
                    for model_string in forward_configured_models()
                },
            },
        )

    def _sync_with_source_parameters(self, name, source_parameters):
        source = ForwardSource.objects.create(
            name=f"{name}-source",
            type="saas",
            url="https://fwd.app",
            status=ForwardSourceStatusChoices.READY,
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
                **dict(source_parameters or {}),
            },
        )
        return ForwardSync.objects.create(
            name=name,
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )


class DLMHardwareNoticeAliasCheckTest(TestCase):
    """#1: config-time base/alias hardware-notice mismatch warning."""

    def _map(self, name, model_string="netbox_dlm.hardwarenotice", enabled=True):
        return Mock(name=name, model_string=model_string, enabled=enabled)

    def _named(self, name, **kw):
        m = self._map(name, **kw)
        m.name = name
        return m

    def test_no_notice_map_returns_none(self):
        from forward_netbox.utilities.health_checks import (
            dlm_hardware_notice_alias_check,
        )

        self.assertIsNone(dlm_hardware_notice_alias_check([]))

    def test_alias_query_with_base_notice_warns(self):
        from forward_netbox.utilities.health_checks import (
            dlm_hardware_notice_alias_check,
        )

        maps = [
            self._named(
                "Forward Devices with NetBox Device Type Aliases",
                model_string="dcim.devicetype",
            ),
            self._named("Forward DLM Hardware Notices"),
        ]
        result = dlm_hardware_notice_alias_check(maps)
        self.assertEqual(result["status"], "warn")
        self.assertIn("alias-aware device query", result["message"])

    def test_matched_variants_pass(self):
        from forward_netbox.utilities.health_checks import (
            dlm_hardware_notice_alias_check,
        )

        maps = [
            self._named(
                "Forward Devices with NetBox Device Type Aliases",
                model_string="dcim.devicetype",
            ),
            self._named("Forward DLM Hardware Notices with NetBox Aliases"),
        ]
        self.assertEqual(dlm_hardware_notice_alias_check(maps)["status"], "pass")

    def test_base_query_with_alias_notice_warns(self):
        from forward_netbox.utilities.health_checks import (
            dlm_hardware_notice_alias_check,
        )

        maps = [
            self._named("Forward Devices", model_string="dcim.device"),
            self._named("Forward DLM Hardware Notices with NetBox Aliases"),
        ]
        self.assertEqual(dlm_hardware_notice_alias_check(maps)["status"], "warn")


class DLMDependencyReadinessCheckTest(TestCase):
    """#3: readiness signal from the last ingestion's DLM dependency skips."""

    @classmethod
    def setUpTestData(cls):
        cls.source = ForwardSource.objects.create(
            name="dlm-ready-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "x",
                "verify": True,
                "network_id": "n",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="dlm-ready-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _named(self, name, model_string):
        m = Mock(model_string=model_string, enabled=True)
        m.name = name
        return m

    def test_warns_when_last_run_skipped_dlm_rows(self):
        from forward_netbox.choices import ForwardIngestionPhaseChoices
        from forward_netbox.models import ForwardIngestionIssue
        from forward_netbox.utilities.health_checks import (
            dlm_dependency_readiness_check,
        )

        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        for i in range(3):
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                model="netbox_dlm.hardwarenotice",
                message=f"skip {i}",
                exception="ForwardDependencySkipError",
            )
        maps = [
            self._named("Forward DLM Hardware Notices", "netbox_dlm.hardwarenotice")
        ]
        result = dlm_dependency_readiness_check(maps, ingestion)
        self.assertEqual(result["status"], "warn")
        self.assertIn("3 DLM row", result["message"])

    def test_uses_rollup_count_instead_of_persisted_issue_count(self):
        from forward_netbox.choices import ForwardIngestionPhaseChoices
        from forward_netbox.models import ForwardIngestionIssue
        from forward_netbox.utilities.health_checks import (
            dlm_dependency_readiness_check,
        )

        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        for i in range(10):
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                model="netbox_dlm.hardwarenotice",
                message=f"skip {i}",
                exception="ForwardDependencySkipError",
            )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            phase=ForwardIngestionPhaseChoices.SYNC,
            model="netbox_dlm.hardwarenotice",
            message="15 rows skipped",
            exception="ForwardDependencySkipError",
            coalesce_fields={
                "dependency_skip_summary": True,
                "dependency_skip_count": 15,
            },
        )
        maps = [
            self._named("Forward DLM Hardware Notices", "netbox_dlm.hardwarenotice")
        ]

        result = dlm_dependency_readiness_check(maps, ingestion)

        self.assertEqual(result["status"], "warn")
        self.assertIn("15 DLM row", result["message"])

    def test_pass_when_no_skips(self):
        from forward_netbox.utilities.health_checks import (
            dlm_dependency_readiness_check,
        )

        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        maps = [
            self._named("Forward DLM Hardware Notices", "netbox_dlm.hardwarenotice")
        ]
        self.assertEqual(
            dlm_dependency_readiness_check(maps, ingestion)["status"], "pass"
        )

    def test_none_when_no_dlm_maps_enabled(self):
        from forward_netbox.utilities.health_checks import (
            dlm_dependency_readiness_check,
        )

        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        self.assertIsNone(dlm_dependency_readiness_check([], ingestion))


class EnabledMapModelNotSelectedTest(TestCase):
    """Correction (Blake 2.5.8): an optional-model map enabled in the NQE Maps
    list but whose model is NOT selected in the sync runs silently — surface it."""

    @classmethod
    def setUpTestData(cls):
        cls.source = ForwardSource.objects.create(
            name="darkmap-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "x",
                "verify": True,
                "network_id": "n",
            },
        )

    def _sync(self, **model_selection):
        return ForwardSync.objects.create(
            name=f"darkmap-{len(model_selection)}-{id(model_selection)}",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed", **model_selection},
        )

    def _map(self, name, model_string):
        m = Mock(model_string=model_string, enabled=True)
        m.name = name
        return m

    def test_optional_map_enabled_without_model_selected_flagged(self):
        from forward_netbox.utilities.health_apply_fetch import model_summary

        # CVE map enabled globally, but netbox_dlm.cve NOT selected on the sync.
        sync = self._sync()
        maps = [self._map("Forward DLM CVEs", "netbox_dlm.cve")]
        summary = model_summary(sync, maps)
        self.assertEqual(
            summary["enabled_optional_maps_without_model"], ["Forward DLM CVEs"]
        )

    def test_selected_optional_model_not_flagged(self):
        from forward_netbox.utilities.health_apply_fetch import model_summary

        sync = self._sync(**{"netbox_dlm.cve": True})
        maps = [self._map("Forward DLM CVEs", "netbox_dlm.cve")]
        summary = model_summary(sync, maps)
        self.assertEqual(summary["enabled_optional_maps_without_model"], [])

    def test_health_check_warns(self):
        from forward_netbox.utilities.health_checks import health_checks

        checks = health_checks(
            sync=self._sync(),
            maps=[],
            model_summary={
                "enabled_models_without_map": [],
                "enabled_optional_maps_without_model": ["Forward DLM Vulnerabilities"],
            },
            query_drift={},
            query_drift_summary={},
            raw_maps=[],
            data_file_maps=[],
            validation_run=None,
            latest_ingestion=None,
            execution_run=None,
            capacity_summary=None,
            query_pushdown=None,
            large_run_tuning=None,
            dependency_preflight=None,
            delete_wave=None,
            throughput=None,
            compatibility_cache=None,
            next_run={"mode": "diff_eligible", "message": ""},
            branching_available_fn=lambda: True,
        )
        entry = next(
            c for c in checks if c["name"] == "Enabled map, model not selected"
        )
        self.assertEqual(entry["status"], "warn")
        self.assertIn("Forward DLM Vulnerabilities", entry["message"])

    def test_sync_health_summary_keeps_unselected_optional_maps_for_check(self):
        content_type = ContentType.objects.get(app_label="dcim", model="module")
        ForwardNQEMap.objects.create(
            name="Test Optional Modules",
            netbox_model=content_type,
            query="@query\n",
            enabled=True,
        )
        sync = self._sync()

        summary = sync_health_summary(sync)

        self.assertIn(
            "Test Optional Modules",
            summary["models"]["enabled_optional_maps_without_model"],
        )
        entry = next(
            check
            for check in summary["checks"]
            if check["name"] == "Enabled map, model not selected"
        )
        self.assertEqual(entry["status"], "warn")
