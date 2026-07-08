# Opt-in features must work in the query VARIANTS, not just the base queries.
#
# A design partner ran the "with aliases" device query and the "with rules" tag
# query; those variants originally lacked the endpoint-import / device-tag-sync
# branches, so the features silently did nothing. These lock the ports in.
import types
from unittest import mock

from django.test import SimpleTestCase

from forward_netbox.utilities import health
from forward_netbox.utilities.query_registry import _default_query_parameters
from forward_netbox.utilities.query_registry import _read_query


class VariantQueryFeatureParityTest(SimpleTestCase):
    def test_aliases_device_query_has_endpoint_branch(self):
        src = _read_query("forward_devices_with_netbox_aliases.nqe")
        self.assertIn("sync_endpoints: Bool", src)
        self.assertIn("network.endpoints", src)
        self.assertIn("1.3.6.1.2.1.1.2", src)  # sysObjectId
        self.assertIn("10418", src)  # Avocent overlay
        self.assertIn("endpoint.tagNames", src)

    def test_aliases_device_query_declares_endpoint_and_tag_params(self):
        params = _default_query_parameters("forward_devices_with_netbox_aliases.nqe")
        self.assertIs(params.get("sync_endpoints"), False)
        for key in (
            "device_tag_include_tags",
            "device_tag_include_match",
            "device_tag_exclude_tags",
        ):
            self.assertIn(key, params)

    def test_with_rules_tag_query_has_sync_device_tags_branch(self):
        src = _read_query("forward_device_feature_tags_with_rules.nqe")
        self.assertIn("sync_device_tags", src)
        self.assertIn("device.tagNames", src)

    def test_with_rules_declares_sync_device_tags_param(self):
        params = _default_query_parameters("forward_device_feature_tags_with_rules.nqe")
        self.assertIn("sync_device_tags", params)

    def test_with_rules_keeps_single_network_devices_reference(self):
        # Two network.devices references disable device-parallel execution; the
        # port must stay a single device loop with a per-device inner union.
        src = _read_query("forward_device_feature_tags_with_rules.nqe")
        self.assertEqual(src.count("network.devices"), 1)


class OptInFeatureMapStateCheckTest(SimpleTestCase):
    """Warn when a feature is enabled but no enabled map provides it."""

    def _check(self, source_params, enabled_map_files):
        maps = []
        for filename in enabled_map_files:
            m = types.SimpleNamespace(
                enabled=True, model_string="dcim.device", _file=filename
            )
            maps.append(m)
        sync = types.SimpleNamespace(
            source=types.SimpleNamespace(parameters=source_params),
            get_maps=lambda: maps,
            is_model_enabled=lambda model_string: True,
        )

        def fake_default(query_map):
            return ({"filename": query_map._file}, None)

        with mock.patch.object(
            health, "builtin_query_default_for_map", side_effect=fake_default
        ):
            return health._optin_feature_map_state_check(sync)

    def test_no_feature_enabled_returns_none(self):
        self.assertIsNone(self._check({}, ["forward_devices.nqe"]))

    def test_endpoints_on_with_aliases_map_passes(self):
        result = self._check(
            {"sync_endpoints": True}, ["forward_devices_with_netbox_aliases.nqe"]
        )
        self.assertEqual(result["status"], "pass")

    def test_endpoints_on_without_supporting_map_warns(self):
        result = self._check({"sync_endpoints": True}, ["forward_interfaces.nqe"])
        self.assertEqual(result["status"], "warn")
        self.assertIn("Import SNMP Endpoints", result["message"])

    def test_device_tags_on_with_rules_map_passes(self):
        result = self._check(
            {"sync_device_tags": ["Mgmt_Lo0"]},
            ["forward_device_feature_tags_with_rules.nqe"],
        )
        self.assertEqual(result["status"], "pass")

    def test_device_tags_on_without_supporting_map_warns(self):
        result = self._check(
            {"sync_device_tags": ["Mgmt_Lo0"]}, ["forward_interfaces.nqe"]
        )
        self.assertEqual(result["status"], "warn")
        self.assertIn("Sync Device Tags", result["message"])


class BaseVariantConflictCheckTest(SimpleTestCase):
    """Warn when a base query and its opt-in variant are both enabled."""

    def _check(self, enabled_map_files):
        maps = [
            types.SimpleNamespace(
                enabled=True, model_string="dcim.device", _file=filename
            )
            for filename in enabled_map_files
        ]
        sync = types.SimpleNamespace(
            get_maps=lambda: maps,
            is_model_enabled=lambda model_string: True,
        )

        def fake_default(query_map):
            return ({"filename": query_map._file}, None)

        with mock.patch.object(
            health, "builtin_query_default_for_map", side_effect=fake_default
        ):
            return health._base_variant_conflict_check(sync)

    def test_base_and_alias_variant_both_enabled_warns(self):
        result = self._check(
            ["forward_devices.nqe", "forward_devices_with_netbox_aliases.nqe"]
        )
        self.assertEqual(result["status"], "warn")
        self.assertIn("Forward device", result["message"])

    def test_only_base_enabled_returns_none(self):
        self.assertIsNone(self._check(["forward_devices.nqe"]))

    def test_only_variant_enabled_returns_none(self):
        self.assertIsNone(self._check(["forward_devices_with_netbox_aliases.nqe"]))

    def test_ipv4_ipv6_split_does_not_warn(self):
        # Same model, different queries by design — not a base/variant pair.
        self.assertIsNone(
            self._check(
                ["forward_ip_addresses_ipv4.nqe", "forward_ip_addresses_ipv6.nqe"]
            )
        )
