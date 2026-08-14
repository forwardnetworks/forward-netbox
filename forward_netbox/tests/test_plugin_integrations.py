from importlib import metadata
from unittest.mock import Mock
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.utilities.plugin_integrations import integration_capability_summary
from forward_netbox.utilities.plugin_integrations import integration_summary
from forward_netbox.utilities.plugin_integrations import iter_integrations
from forward_netbox.utilities.plugin_integrations import optional_integration_for_model
from forward_netbox.utilities.plugin_integrations.registry import DLM_INTEGRATION
from forward_netbox.utilities.plugin_integrations.registry import integration_capability
from forward_netbox.utilities.plugin_integrations.registry import ROUTING_INTEGRATION
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher


class OptionalPluginIntegrationRegistryTest(TestCase):
    def test_registry_exposes_generic_optional_integrations(self):
        integrations = list(iter_integrations())

        self.assertEqual(
            [integration.key for integration in integrations],
            [
                "routing.netbox_routing",
                "peering.netbox_peering_manager",
                "aci.netbox_cisco_aci",
                "lifecycle.netbox_dlm",
            ],
        )
        self.assertEqual(
            optional_integration_for_model("netbox_routing.bgppeer").key,
            "routing.netbox_routing",
        )
        self.assertEqual(
            optional_integration_for_model("netbox_peering_manager.peeringsession").key,
            "peering.netbox_peering_manager",
        )
        self.assertEqual(
            optional_integration_for_model("netbox_cisco_aci.acinode").key,
            "aci.netbox_cisco_aci",
        )
        self.assertIsNone(optional_integration_for_model("dcim.device"))

        summary = integration_summary()
        self.assertEqual(
            set(summary),
            {
                "routing.netbox_routing",
                "peering.netbox_peering_manager",
                "aci.netbox_cisco_aci",
                "lifecycle.netbox_dlm",
            },
        )
        self.assertEqual(
            summary["routing.netbox_routing"]["display_name"],
            "NetBox Routing",
        )
        self.assertEqual(
            summary["peering.netbox_peering_manager"]["display_name"],
            "NetBox Peering Manager",
        )
        self.assertEqual(summary["aci.netbox_cisco_aci"]["display_name"], "Cisco ACI")

    def test_registry_reports_capabilities_for_each_optional_surface(self):
        def fake_version(package_name):
            versions = {
                "netbox-routing": "0.4.3",
                "netbox-peering-manager": "0.3.0",
                "netbox-cisco-aci": "0.3.9",
                "netbox-dlm": "0.9.1",
            }
            return versions[package_name]

        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            side_effect=lambda app_label: True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            side_effect=fake_version,
        ):
            summary = integration_capability_summary()

        self.assertEqual(
            summary["routing.netbox_routing"]["availability_status"], "available"
        )
        self.assertEqual(
            summary["peering.netbox_peering_manager"]["availability_status"],
            "available",
        )
        self.assertEqual(
            summary["aci.netbox_cisco_aci"]["availability_status"],
            "unsupported_version",
        )
        self.assertEqual(
            summary["routing.netbox_routing"]["package_name"],
            "netbox-routing",
        )
        self.assertEqual(
            summary["peering.netbox_peering_manager"]["package_name"],
            "netbox-peering-manager",
        )
        self.assertEqual(
            summary["aci.netbox_cisco_aci"]["package_name"],
            "netbox-cisco-aci",
        )
        self.assertEqual(summary["aci.netbox_cisco_aci"]["required_version"], "0.4.0")
        self.assertEqual(summary["lifecycle.netbox_dlm"]["required_version"], "0.9.1")
        self.assertEqual(
            summary["lifecycle.netbox_dlm"]["supported_versions"],
            ["0.4.1", "0.5.0", "0.6.0", "0.7.0", "0.8.0", "0.9.1"],
        )

    def test_dlm_supported_versions_are_available_and_not_dropped(self):
        model_strings = list(DLM_INTEGRATION.supported_models)
        for version in ("0.4.1", "0.5.0", "0.6.0", "0.7.0", "0.8.0"):
            with self.subTest(version=version), patch(
                "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
                return_value=True,
            ), patch(
                "forward_netbox.utilities.plugin_integrations.registry._present_models",
                side_effect=lambda models: sorted(models),
            ), patch(
                "forward_netbox.utilities.plugin_integrations.registry._missing_models",
                return_value=[],
            ), patch(
                "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
                return_value=version,
            ):
                self.assertTrue(integration_capability(DLM_INTEGRATION)["available"])
                fetcher = ForwardQueryFetcher(
                    sync=Mock(), client=Mock(), logger_=Mock()
                )
                self.assertEqual(
                    fetcher._drop_unavailable_integration_models(model_strings),
                    model_strings,
                )

    def test_unsupported_dlm_version_is_dropped(self):
        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            return_value="0.3.0",
        ):
            capability = integration_capability(DLM_INTEGRATION)
            self.assertFalse(capability["available"])
            self.assertEqual(capability["availability_status"], "unsupported_version")
            self.assertEqual(
                capability["availability_reason"],
                "Installed plugin version must equal one of: 0.4.1, 0.5.0, 0.6.0, 0.7.0, "
                "0.8.0, 0.9.1.",
            )
            fetcher = ForwardQueryFetcher(sync=Mock(), client=Mock(), logger_=Mock())
            self.assertEqual(
                fetcher._drop_unavailable_integration_models(
                    list(DLM_INTEGRATION.supported_models)
                ),
                [],
            )

    def test_single_version_integration_keeps_exact_match_behavior(self):
        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            return_value="0.4.3",
        ):
            capability = integration_capability(ROUTING_INTEGRATION)

        self.assertTrue(capability["available"])
        self.assertEqual(capability["supported_versions"], ["0.4.3"])

        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            return_value="0.4.4",
        ):
            capability = integration_capability(ROUTING_INTEGRATION)

        self.assertFalse(capability["available"])
        self.assertEqual(
            capability["availability_reason"],
            "Installed plugin version must equal 0.4.3.",
        )

    def test_registry_reports_no_plugin_state_cleanly(self):
        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=False,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            side_effect=metadata.PackageNotFoundError,
        ):
            summary = integration_capability_summary()

        for integration in summary.values():
            self.assertEqual(integration["availability_status"], "not_installed")
            self.assertEqual(
                integration["availability_reason"],
                "Target plugin app is not installed.",
            )
            self.assertFalse(integration["available"])
            self.assertFalse(integration["version_matches"])
            self.assertIsNone(integration["version"])

    def test_enabled_plugin_without_canonical_package_metadata_is_unavailable(self):
        with patch(
            "forward_netbox.utilities.plugin_integrations.registry.apps.is_installed",
            return_value=True,
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._present_models",
            side_effect=lambda models: sorted(models),
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry._missing_models",
            return_value=[],
        ), patch(
            "forward_netbox.utilities.plugin_integrations.registry.metadata.version",
            side_effect=metadata.PackageNotFoundError,
        ):
            summary = integration_capability_summary()

        for integration in summary.values():
            self.assertEqual(
                integration["availability_status"],
                "package_metadata_unavailable",
            )
            self.assertFalse(integration["available"])
