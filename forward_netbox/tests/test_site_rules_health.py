# Site rules set on the source do nothing unless every enabled map that names a site
# runs a query that takes them. A map that cannot (a custom query, an older bundled
# one) would put its devices on the wrong sites, so Health says so before a sync does.
import types
from unittest import mock

from django.test import SimpleTestCase

from forward_netbox.utilities import health

RULES = [{"kind": "location", "pattern": "Hall*", "site": "campus"}]


class SiteRulesMapStateCheckTest(SimpleTestCase):
    def _check(self, source_params, maps):
        sync = types.SimpleNamespace(
            source=types.SimpleNamespace(parameters=source_params),
            get_maps=lambda: maps,
            is_model_enabled=lambda model_string: True,
        )

        def default_for(query_map):
            filename = getattr(query_map, "_file", None)
            return ({"filename": filename}, None) if filename else (None, "custom")

        with mock.patch.object(
            health, "builtin_query_default_for_map", side_effect=default_for
        ):
            return health._site_rules_map_state_check(sync)

    def _map(self, model_string, filename=None, enabled=True):
        return types.SimpleNamespace(
            enabled=enabled, model_string=model_string, _file=filename
        )

    def test_no_rules_adds_no_check(self):
        self.assertIsNone(
            self._check({}, [self._map("dcim.site", "forward_device_types.nqe")])
        )

    def test_every_map_that_names_a_site_takes_the_rules(self):
        result = self._check(
            {"site_rules": RULES},
            [
                self._map("dcim.site", "forward_locations.nqe"),
                self._map("dcim.device", "forward_devices.nqe"),
                self._map("ipam.vlan", "forward_vlans.nqe"),
            ],
        )

        self.assertEqual(result["status"], "pass")

    def test_a_bundled_map_that_names_no_site_is_ignored(self):
        # The ACI inventory is a device map that names no site: the rules have
        # nothing to do there, so it is not a problem.
        result = self._check(
            {"site_rules": RULES},
            [
                self._map("dcim.site", "forward_locations.nqe"),
                self._map("dcim.device", "forward_device_types.nqe"),
            ],
        )

        self.assertEqual(result["status"], "pass")

    def test_a_site_naming_bundled_query_that_lost_the_parameter_is_named(self):
        with mock.patch.object(
            health, "_default_query_parameters", return_value={"scope": []}
        ):
            result = self._check(
                {"site_rules": RULES},
                [self._map("dcim.device", "forward_devices.nqe")],
            )

        self.assertEqual(result["status"], "warn")
        self.assertIn("forward_devices.nqe", result["message"])

    def test_a_custom_query_cannot_take_the_rules(self):
        result = self._check({"site_rules": RULES}, [self._map("dcim.site")])

        self.assertEqual(result["status"], "warn")
        self.assertIn("custom", result["message"].lower())

    def test_a_disabled_map_is_ignored(self):
        result = self._check(
            {"site_rules": RULES},
            [self._map("dcim.device", "forward_device_types.nqe", enabled=False)],
        )

        self.assertEqual(result["status"], "pass")

    def test_maps_that_do_not_name_a_site_are_ignored(self):
        result = self._check(
            {"site_rules": RULES},
            [self._map("dcim.manufacturer", "forward_device_vendors.nqe")],
        )

        self.assertEqual(result["status"], "pass")
