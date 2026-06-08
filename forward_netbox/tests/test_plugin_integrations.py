from importlib import metadata
from unittest.mock import patch

from django.test import TestCase

from forward_netbox.utilities.plugin_integrations import integration_capability_summary
from forward_netbox.utilities.plugin_integrations import integration_summary
from forward_netbox.utilities.plugin_integrations import iter_integrations
from forward_netbox.utilities.plugin_integrations import optional_integration_for_model


class OptionalPluginIntegrationRegistryTest(TestCase):
    def test_registry_exposes_generic_optional_integrations(self):
        integrations = list(iter_integrations())

        self.assertEqual(
            [integration.key for integration in integrations],
            [
                "routing.netbox_routing",
                "peering.netbox_peering_manager",
                "aci.netbox_cisco_aci",
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
                "netbox-routing": "1.0.0",
                "netbox-peering-manager": "1.0.0",
                "netbox-cisco-aci": "0.2.1",
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
            summary["routing.netbox_routing"]["installed_package_name"],
            "netbox-routing",
        )
        self.assertEqual(
            summary["peering.netbox_peering_manager"]["installed_package_name"],
            "netbox-peering-manager",
        )
        self.assertEqual(
            summary["aci.netbox_cisco_aci"]["installed_package_name"],
            "netbox-cisco-aci",
        )
        self.assertEqual(summary["aci.netbox_cisco_aci"]["minimum_version"], "0.2.2")

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
            self.assertFalse(integration["unsupported_version"])
            self.assertIsNone(integration["installed_package_name"])
            self.assertIsNone(integration["version"])
