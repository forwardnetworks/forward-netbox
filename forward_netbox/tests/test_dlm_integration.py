# Optional netbox-dlm (Device Lifecycle Management) integration wiring.
#
# The plugin is NOT installed in CI; these tests cover the registry surface,
# bundled queries, and adapter contract so the integration is exercised without
# it. Field origin: the design partner authoring netbox-dlm runs this plugin.
import importlib

from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.choices import FORWARD_DLM_MODELS
from forward_netbox.choices import FORWARD_OPTIONAL_MODELS
from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.utilities.plugin_integrations.registry import DLM_INTEGRATION
from forward_netbox.utilities.plugin_integrations.registry import (
    optional_integration_for_model,
)
from forward_netbox.utilities.query_registry import _default_query_parameters
from forward_netbox.utilities.query_registry import _read_query
from forward_netbox.utilities.query_registry import builtin_nqe_map_rows

DLM_MODEL_STRINGS = (
    "netbox_dlm.softwareversion",
    "netbox_dlm.hardwarenotice",
    "netbox_dlm.devicesoftware",
    "netbox_dlm.cve",
    "netbox_dlm.vulnerability",
)


class DlmRegistryWiringTest(SimpleTestCase):
    def test_models_registered_as_supported_and_optional(self):
        for model_string in DLM_MODEL_STRINGS:
            self.assertIn(model_string, FORWARD_DLM_MODELS)
            self.assertIn(model_string, FORWARD_SUPPORTED_MODELS)
            self.assertIn(model_string, FORWARD_OPTIONAL_MODELS)

    def test_integration_matches_models(self):
        for model_string in DLM_MODEL_STRINGS:
            integration = optional_integration_for_model(model_string)
            self.assertIsNotNone(integration, model_string)
            self.assertEqual(integration.app_label, "netbox_dlm")

    def test_builtin_maps_seeded_disabled_by_default(self):
        rows = {row["name"]: row for row in builtin_nqe_map_rows()}
        for name in DLM_INTEGRATION.query_maps:
            self.assertIn(name, rows)
            self.assertFalse(
                rows[name].get("enabled", True),
                f"{name} must ship disabled (opt-in plugin surface)",
            )

    def test_adapter_module_exposes_apply_and_delete_per_model(self):
        adapter = importlib.import_module(DLM_INTEGRATION.adapter_module)
        for model_string in DLM_INTEGRATION.supported_models:
            slug = model_string.replace(".", "_")
            self.assertTrue(callable(getattr(adapter, f"apply_{slug}", None)), slug)
            self.assertTrue(callable(getattr(adapter, f"delete_{slug}", None)), slug)


class DlmQueryStructureTest(SimpleTestCase):
    def test_software_versions_query_shape(self):
        src = _read_query("forward_dlm_software_versions.nqe")
        self.assertIn("device.platform.osSupport", src)
        self.assertIn("lastSupportDate", src)
        # dlm SoftwareVersion.version is Char(50); announcement URL is URLField.
        self.assertIn("substring(device.platform.osVersion, 0, 50)", src)
        self.assertIn("substring(support.announcementUrl, 0, 200)", src)
        self.assertIn("select distinct", src)

    def test_hardware_notices_query_shape(self):
        src = _read_query("forward_dlm_hardware_notices.nqe")
        self.assertIn("DevicePartType.CHASSIS", src)
        for field in (
            "end_of_support:",
            "end_of_security_patches:",
            "end_of_sw_releases:",
            "device_type_slug:",
        ):
            self.assertIn(field, src)

    def test_hardware_notices_aliases_query_shape(self):
        # Alias-aware variant: same chassis-part support fields, but the
        # device_type is mapped through the netbox_device_type_aliases data file
        # (as the alias-aware device query does) so the notice's DeviceType
        # lookup matches the aliased name instead of skipping on the raw model.
        src = _read_query("forward_dlm_hardware_notices_with_netbox_aliases.nqe")
        self.assertIn("DevicePartType.CHASSIS", src)
        self.assertIn("network.extensions.netbox_device_type_aliases", src)
        self.assertIn("device_type_alias", src)
        self.assertIn("device_type: device_type_model", src)
        for field in (
            "end_of_support:",
            "end_of_security_patches:",
            "end_of_sw_releases:",
            "device_type_slug:",
        ):
            self.assertIn(field, src)

    def test_device_software_query_shape(self):
        src = _read_query("forward_dlm_device_software.nqe")
        self.assertIn("device.platform.osVersion", src)
        for field in ("name:", "platform_slug:", "version:"):
            self.assertIn(field, src)

    def test_cve_query_shape(self):
        src = _read_query("forward_dlm_cves.nqe")
        # Global catalog off the CVE database, keyed on the unique cve_id.
        self.assertIn("network.cveDatabase.cves", src)
        self.assertIn("@primaryKey(cve_id)", src)
        for field in ("cve_id:", "name:", "description:", "severity:"):
            self.assertIn(field, src)
        # Severity maps to the 5 netbox-dlm CVESeverityChoices via enum-direct
        # comparison (the deprecated Cve.severity/description fields are avoided
        # in favour of the per-vendor infos).
        self.assertIn("cve.vendorInfos", src)
        for choice in ('"critical"', '"high"', '"medium"', '"low"', '"none"'):
            self.assertIn(choice, src)
        for sev in (
            "Severity.CRITICAL",
            "Severity.HIGH",
            "Severity.MEDIUM",
            "Severity.LOW",
            "Severity.NONE",
        ):
            self.assertIn(sev, src)
        # cve_id clamped to the 20-char column.
        self.assertIn("substring(cve.cveId, 0, 20)", src)

    def test_vulnerability_query_shape(self):
        src = _read_query("forward_dlm_vulnerabilities.nqe")
        # Device-scoped: only vulnerable findings, only devices that can build a
        # SoftwareVersion FK (isPresent(osVersion)); dual-basis collapsed.
        self.assertIn("device.cveFindings", src)
        self.assertIn("isVulnerable", src)
        self.assertIn("isPresent(device.platform.osVersion)", src)
        self.assertIn("distinct(", src)
        self.assertIn("@primaryKey(name, cve_id)", src)
        # Emits the same (platform, version) natural key as device software so
        # the software_version FK resolves to the same row.
        for field in ("name:", "cve_id:", "platform:", "platform_slug:", "version:"):
            self.assertIn(field, src)
        self.assertIn("substring(device.platform.osVersion, 0, 50)", src)

    def test_queries_seed_shard_parameter(self):
        for filename in (
            "forward_dlm_software_versions.nqe",
            "forward_dlm_hardware_notices.nqe",
            "forward_dlm_hardware_notices_with_netbox_aliases.nqe",
            "forward_dlm_device_software.nqe",
            "forward_dlm_cves.nqe",
            "forward_dlm_vulnerabilities.nqe",
        ):
            params = _default_query_parameters(filename)
            self.assertIn("forward_netbox_shard_keys", params, filename)


class DlmRunnerDispatchTest(SimpleTestCase):
    def test_runner_has_apply_and_delete_methods(self):
        from forward_netbox.utilities.sync import ForwardSyncRunner

        for model_string in DLM_MODEL_STRINGS:
            slug = model_string.replace(".", "_")
            self.assertTrue(
                callable(getattr(ForwardSyncRunner, f"_apply_{slug}", None)), slug
            )
            self.assertTrue(
                callable(getattr(ForwardSyncRunner, f"_delete_{slug}", None)), slug
            )


class DependencySkipIssueRollupTest(TestCase):
    """#2: a flood of distinct 'device type not in NetBox yet' skips is capped
    to N detail rows + one actionable summary issue."""

    def _runner(self, ingestion):
        from types import SimpleNamespace

        logger = SimpleNamespace(
            log_info=lambda *a, **k: None,
            log_warning=lambda *a, **k: None,
            log_failure=lambda *a, **k: None,
        )
        return SimpleNamespace(
            ingestion=ingestion,
            logger=logger,
            _recorded_issue_ids=set(),
            _dependency_skip_issue_counts={},
            _dependency_skip_issue_samples={},
            DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT=10,
        )

    def test_caps_rows_and_emits_summary(self):
        from forward_netbox.exceptions import ForwardDependencySkipError
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardIngestionIssue
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync
        from forward_netbox.utilities.sync_reporting import (
            emit_dependency_skip_issue_summary,
        )
        from forward_netbox.utilities.sync_reporting import record_issue

        source = ForwardSource.objects.create(
            name="rollup-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@e.c",
                "password": "x",
                "verify": True,
                "network_id": "n",
            },
        )
        sync = ForwardSync.objects.create(
            name="rollup-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ingestion = ForwardIngestion.objects.create(sync=sync)
        runner = self._runner(ingestion)
        model = "netbox_dlm.hardwarenotice"
        for i in range(15):
            record_issue(
                runner,
                model,
                f"Skipping DLM hardware notice because device type `DT-{i}` is not in NetBox yet.",
                {"device_type": f"DT-{i}"},
                exception=ForwardDependencySkipError(
                    "skip", context={"device_type": f"DT-{i}"}
                ),
                context={"device_type": f"DT-{i}"},
                log_level="info",
            )
        # Only the first 10 distinct rows persist as detail (5 suppressed).
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=ingestion, model=model
            ).count(),
            10,
        )
        # Emit the summary: one rolled-up issue covering all 15.
        emit_dependency_skip_issue_summary(runner, model)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(
                ingestion=ingestion, model=model
            ).count(),
            11,
        )
        summary = ForwardIngestionIssue.objects.filter(
            ingestion=ingestion,
            model=model,
            coalesce_fields__dependency_skip_summary=True,
        )
        self.assertEqual(summary.count(), 1)
        self.assertIn(
            "15 netbox_dlm.hardwarenotice row(s) skipped", summary.first().message
        )

    def test_no_summary_below_cap(self):
        from forward_netbox.exceptions import ForwardDependencySkipError
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardIngestionIssue
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync
        from forward_netbox.utilities.sync_reporting import (
            emit_dependency_skip_issue_summary,
        )
        from forward_netbox.utilities.sync_reporting import record_issue

        source = ForwardSource.objects.create(
            name="rollup-src2",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@e.c",
                "password": "x",
                "verify": True,
                "network_id": "n",
            },
        )
        sync = ForwardSync.objects.create(
            name="rollup-sync2",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        ingestion = ForwardIngestion.objects.create(sync=sync)
        runner = self._runner(ingestion)
        model = "netbox_dlm.hardwarenotice"
        for i in range(4):
            record_issue(
                runner,
                model,
                f"skip DT-{i}",
                {"device_type": f"DT-{i}"},
                exception=ForwardDependencySkipError("skip"),
                context={"device_type": f"DT-{i}"},
                log_level="info",
            )
        emit_dependency_skip_issue_summary(runner, model)
        self.assertEqual(
            ForwardIngestionIssue.objects.filter(ingestion=ingestion).count(), 4
        )
