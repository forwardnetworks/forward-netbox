# Optional netbox-dlm (Device Lifecycle Management) integration wiring.
#
# The plugin is NOT installed in CI; these tests cover the registry surface,
# bundled queries, and adapter contract so the integration is exercised without
# it. Field origin: the design partner authoring netbox-dlm runs this plugin.
import importlib
from datetime import date
from unittest import skipUnless
from unittest.mock import Mock
from unittest.mock import patch

from django.apps import apps
from django.test import SimpleTestCase
from django.test import TestCase

from forward_netbox.choices import FORWARD_DLM_MODELS
from forward_netbox.choices import FORWARD_OPTIONAL_MODELS
from forward_netbox.choices import FORWARD_SUPPORTED_MODELS
from forward_netbox.utilities.branch_budget import APPLY_DEPENDENCY_MODEL_ORDER
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

    def test_device_software_branch_applies_before_catalog_enrichment(self):
        self.assertLess(
            APPLY_DEPENDENCY_MODEL_ORDER.index("netbox_dlm.devicesoftware"),
            APPLY_DEPENDENCY_MODEL_ORDER.index("netbox_dlm.softwareversion"),
        )


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
        self.assertIn("device.platform.osSupport?.lastSupportDate", src)
        self.assertIn(
            "substring(device.platform.osSupport.announcementUrl, 0, 200)", src
        )
        for field in (
            "name:",
            "platform_slug:",
            "version:",
            "end_of_support:",
            "documentation_url:",
        ):
            self.assertIn(field, src)

    def test_cve_query_shape(self):
        src = _read_query("forward_dlm_cves.nqe")
        # Global catalog off the CVE database, keyed on the unique cve_id.
        self.assertIn("network.cveDatabase.cves", src)
        # This Forward runtime rejects @primaryKey stacked with a parameterized
        # @query before query execution. NetBox coalesce_fields enforce identity.
        self.assertNotIn("@primaryKey", src)
        self.assertIn("@query\nf(forward_netbox_shard_keys", src)
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
        self.assertNotIn("@primaryKey", src)
        self.assertIn("@query\nf(forward_netbox_shard_keys", src)
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


class DlmAssociationContractTest(SimpleTestCase):
    def _runner(self):
        runner = Mock()
        runner._model_field_values.side_effect = lambda model, values: values
        return runner

    @patch("forward_netbox.utilities.sync_dlm._lookup_platform")
    @patch("forward_netbox.utilities.sync_dlm._dlm_model")
    def test_catalog_map_does_not_create_unassociated_version(
        self, dlm_model, lookup_platform
    ):
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_softwareversion

        runner = self._runner()
        dlm_model.return_value = object()
        lookup_platform.return_value = object()
        runner._get_unique_or_raise.return_value = None

        result = apply_netbox_dlm_softwareversion(
            runner,
            {
                "platform": "IOS_XE",
                "platform_slug": "ios-xe",
                "version": "17.12.07b",
                "end_of_support": "2028-03-31",
            },
        )

        self.assertFalse(result)
        runner._upsert_values_from_defaults.assert_not_called()

    @patch("forward_netbox.utilities.sync_dlm._lookup_device")
    @patch("forward_netbox.utilities.sync_dlm._lookup_platform")
    @patch("forward_netbox.utilities.sync_dlm._dlm_model")
    def test_device_software_creates_link_with_lifecycle_dates(
        self, dlm_model, lookup_platform, lookup_device
    ):
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_devicesoftware

        runner = self._runner()
        software_model = object()
        device_software_model = object()
        dlm_model.side_effect = lambda runner_, name, model_string: {
            "SoftwareVersion": software_model,
            "DeviceSoftware": device_software_model,
        }[name]
        platform = object()
        device = object()
        software_version = object()
        device_software = object()
        lookup_platform.return_value = platform
        lookup_device.return_value = device
        runner._upsert_values_from_defaults.side_effect = [
            (software_version, True),
            (device_software, True),
        ]

        result = apply_netbox_dlm_devicesoftware(
            runner,
            {
                "name": "device-1",
                "platform": "IOS_XE",
                "platform_slug": "ios-xe",
                "version": "17.12.07b",
                "end_of_support": "2028-03-31",
                "documentation_url": "https://example.test/eol",
            },
        )

        self.assertIs(result, device_software)
        version_values = runner._upsert_values_from_defaults.call_args_list[0].kwargs[
            "values"
        ]
        self.assertEqual(version_values["end_of_support"], date(2028, 3, 31))
        self.assertEqual(
            version_values["documentation_url"], "https://example.test/eol"
        )
        link_values = runner._upsert_values_from_defaults.call_args_list[1].kwargs[
            "values"
        ]
        self.assertIs(link_values["device"], device)
        self.assertIs(link_values["software_version"], software_version)

    @patch("forward_netbox.utilities.sync_dlm.ensure_dlm_cve")
    @patch("forward_netbox.utilities.sync_dlm.ensure_dlm_device_software")
    @patch("forward_netbox.utilities.sync_dlm._dlm_model")
    def test_vulnerability_ensures_device_software_association(
        self, dlm_model, ensure_device_software, ensure_cve
    ):
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_vulnerability

        runner = self._runner()
        dlm_model.return_value = object()
        device = object()
        software_version = object()
        ensure_device_software.return_value = (device, software_version, object())
        cve = object()
        ensure_cve.return_value = cve
        vulnerability = object()
        runner._upsert_values_from_defaults.return_value = (vulnerability, True)

        result = apply_netbox_dlm_vulnerability(runner, {"name": "device-1"})

        self.assertIs(result, vulnerability)
        ensure_device_software.assert_called_once_with(runner, {"name": "device-1"})
        values = runner._upsert_values_from_defaults.call_args.kwargs["values"]
        self.assertEqual(
            values,
            {"cve": cve, "software_version": software_version, "device": device},
        )


@skipUnless(apps.is_installed("netbox_dlm"), "netbox-dlm is not installed")
class DlmInstalledPluginAssociationTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Platform
        from dcim.models import Site

        site = Site.objects.create(name="DLM Test", slug="dlm-test")
        manufacturer = Manufacturer.objects.create(name="DLM Vendor", slug="dlm-vendor")
        role = DeviceRole.objects.create(
            name="DLM Role", slug="dlm-role", color="9e9e9e"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="DLM Model",
            slug="dlm-model",
        )
        cls.platform = Platform.objects.create(name="IOS_XE", slug="ios-xe")
        cls.device = Device.objects.create(
            name="dlm-device-1",
            site=site,
            role=role,
            device_type=device_type,
            platform=cls.platform,
            status="active",
        )
        cls.vulnerability_device = Device.objects.create(
            name="dlm-device-2",
            site=site,
            role=role,
            device_type=device_type,
            platform=cls.platform,
            status="active",
        )

    def _runner(self):
        from forward_netbox.utilities.sync import ForwardSyncRunner

        return ForwardSyncRunner(
            sync=None,
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

    def test_real_models_never_create_unassociated_software_version(self):
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_devicesoftware
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_softwareversion
        from forward_netbox.utilities.sync_dlm import apply_netbox_dlm_vulnerability

        CVE = apps.get_model("netbox_dlm", "CVE")
        DeviceSoftware = apps.get_model("netbox_dlm", "DeviceSoftware")
        SoftwareVersion = apps.get_model("netbox_dlm", "SoftwareVersion")
        Vulnerability = apps.get_model("netbox_dlm", "Vulnerability")
        runner = self._runner()
        catalog_row = {
            "platform": "IOS_XE",
            "platform_slug": "ios-xe",
            "version": "17.12.07b",
            "end_of_support": "2028-03-31",
            "documentation_url": "https://example.test/eol",
        }

        self.assertFalse(apply_netbox_dlm_softwareversion(runner, catalog_row))
        self.assertFalse(SoftwareVersion.objects.exists())

        apply_netbox_dlm_devicesoftware(
            runner,
            {**catalog_row, "name": self.device.name},
        )
        software_version = SoftwareVersion.objects.get()
        self.assertEqual(software_version.end_of_support, date(2028, 3, 31))
        self.assertTrue(
            DeviceSoftware.objects.filter(
                device=self.device,
                software_version=software_version,
            ).exists()
        )

        apply_netbox_dlm_vulnerability(
            runner,
            {
                "name": self.vulnerability_device.name,
                "cve_id": "CVE-2026-12345",
                "platform": "IOS_XE",
                "platform_slug": "ios-xe",
                "version": "17.12.07b",
            },
        )
        self.assertTrue(
            DeviceSoftware.objects.filter(
                device=self.vulnerability_device,
                software_version=software_version,
            ).exists()
        )
        self.assertTrue(CVE.objects.filter(cve_id="CVE-2026-12345").exists())
        self.assertTrue(
            Vulnerability.objects.filter(
                device=self.vulnerability_device,
                software_version=software_version,
            ).exists()
        )
        self.assertEqual(
            SoftwareVersion.objects.exclude(devices_running__isnull=False).count(),
            0,
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

        for i in range(15, 20):
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
        emit_dependency_skip_issue_summary(runner, model)

        summary = ForwardIngestionIssue.objects.filter(
            ingestion=ingestion,
            model=model,
            coalesce_fields__dependency_skip_summary=True,
        )
        self.assertEqual(summary.count(), 1)
        self.assertIn(
            "20 netbox_dlm.hardwarenotice row(s) skipped", summary.first().message
        )
        self.assertEqual(summary.first().coalesce_fields["dependency_skip_count"], 20)

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
