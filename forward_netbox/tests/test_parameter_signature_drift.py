"""A published query with the wrong parameter list is a guaranteed HTTP 400.

A deployment upgraded the plugin and two models failed on every sync. The maps
resolved through queries published in the customer's Forward org library, and
the release had taken two bundled queries from one parameter to seven. The
plugin derives what it SENDS from the bundled file, so it sent seven arguments
to a published query declaring one, and Forward refused each execution with
`Provided argument, 'sync_endpoints' is not a parameter to the given query`.

The drift check of the day compared source TEXT and reported "source differs" -
true of any query republished late, and indistinguishable from routine. These
tests pin the distinction that matters.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.query_binding_resolution import (
    parameter_signature_drift,
)

# The shape the customer's published copy had: the pre-release signature.
PUBLISHED_ONE_PARAMETER = """
@query
f(forward_netbox_shard_keys: List<String>) =
foreach device in network.devices
select { name: device.name };
"""

# The shape the release shipped, and therefore what the plugin sends for.
BUNDLED_SEVEN_PARAMETERS = """
@query
f(forward_netbox_shard_keys: List<String>, sync_endpoints: Bool,
  sync_generic_endpoints: Bool, scope_endpoints_by_include_tags: Bool,
  device_tag_include_tags: List<String>, device_tag_include_match: String,
  device_tag_exclude_tags: List<String>) =
foreach device in network.devices
select { name: device.name };
"""


class ParameterSignatureDriftTest(SimpleTestCase):
    def _drift(self, bundled, published, monkeypatch_target=None):
        import forward_netbox.utilities.query_binding_resolution as module

        original = module.read_compiled_builtin_query_source
        module.read_compiled_builtin_query_source = lambda _filename: bundled
        try:
            return parameter_signature_drift("forward_interfaces.nqe", published)
        finally:
            module.read_compiled_builtin_query_source = original

    def test_the_outage_shape_is_reported_as_a_mismatch(self):
        drift = self._drift(BUNDLED_SEVEN_PARAMETERS, PUBLISHED_ONE_PARAMETER)

        self.assertFalse(drift["parameter_signature_matches"])
        self.assertEqual(
            drift["declared_parameters_live"], ["forward_netbox_shard_keys"]
        )
        self.assertEqual(len(drift["declared_parameters_local"]), 7)
        # Named individually, because these are the arguments Forward will
        # refuse by name on the next sync.
        self.assertIn("sync_endpoints", drift["missing_parameters"])
        self.assertIn("device_tag_include_tags", drift["missing_parameters"])

    def test_a_republished_query_matches(self):
        drift = self._drift(BUNDLED_SEVEN_PARAMETERS, BUNDLED_SEVEN_PARAMETERS)

        self.assertTrue(drift["parameter_signature_matches"])
        self.assertEqual(drift["missing_parameters"], [])
        self.assertEqual(drift["unexpected_parameters"], [])

    def test_a_body_change_that_keeps_the_signature_is_not_a_mismatch(self):
        edited = BUNDLED_SEVEN_PARAMETERS.replace(
            "select { name: device.name };",
            "select { name: device.name, extra: 1 };",
        )

        drift = self._drift(BUNDLED_SEVEN_PARAMETERS, edited)

        # Source text differs, which the existing check already reports. This
        # one must stay quiet, or the signal it exists to give is lost in it.
        self.assertTrue(drift["parameter_signature_matches"])

    def test_an_unparseable_side_reports_nothing_rather_than_guessing(self):
        self.assertEqual(self._drift(BUNDLED_SEVEN_PARAMETERS, ""), {})
