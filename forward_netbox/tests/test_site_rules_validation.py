# When a published query rejects the site rules at runtime, only that model's fetch
# fails: the other models would still apply, putting devices on sites that were never
# created or at the wrong place. A site-rules failure is therefore a reason to block
# the run, whichever model it hit, not a per-model note.
from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.utilities.validation import ForwardValidationRunner


def _runner(models):
    runner = ForwardValidationRunner.__new__(ForwardValidationRunner)
    runner.sync = SimpleNamespace(get_model_strings=lambda: models, parameters={})
    return runner


def _result(model, reason, failures=1):
    return {"model": model, "failure_count": failures, "failure_reason": reason}


class SiteRulesFailureBlocksTest(SimpleTestCase):
    MODELS = ["dcim.site", "dcim.device", "ipam.vlan", "dcim.interface"]

    def test_a_site_rules_failure_on_a_site_model_blocks_the_run(self):
        reasons = _runner(self.MODELS)._required_query_failure_reasons(
            [_result("dcim.site", "site-rules-query-rejected")]
        )

        self.assertEqual(len(reasons), 1)
        self.assertIn("dcim.site", reasons[0])
        self.assertIn("Publish Bundled Queries", reasons[0])

    def test_every_model_it_hit_is_named(self):
        reasons = _runner(self.MODELS)._required_query_failure_reasons(
            [
                _result("dcim.site", "site-rules-query-rejected"),
                _result("ipam.vlan", "site-rules-query-rejected"),
            ]
        )

        self.assertIn("dcim.site", reasons[0])
        self.assertIn("ipam.vlan", reasons[0])

    def test_the_other_site_rules_reasons_block_too(self):
        for reason in ("site-rules-custom-map", "site-rules-query-not-declared"):
            with self.subTest(reason=reason):
                reasons = _runner(self.MODELS)._required_query_failure_reasons(
                    [_result("dcim.device", reason)]
                )

                self.assertTrue(any("dcim.device" in text for text in reasons))

    def test_an_ordinary_failure_on_a_site_model_is_not_a_site_rules_reason(self):
        reasons = _runner(
            ["dcim.site", "dcim.interface"]
        )._required_query_failure_reasons([_result("dcim.site", "timeout")])

        self.assertFalse([text for text in reasons if "ite mapping rules" in text])

    def test_a_result_with_no_failure_is_ignored(self):
        reasons = _runner(self.MODELS)._required_query_failure_reasons(
            [_result("dcim.site", "site-rules-query-rejected", failures=0)]
        )

        self.assertEqual(reasons, [])


class AdviceMatchesTheCauseTest(SimpleTestCase):
    """The same sentence for every cause sent an operator with a custom query to
    Publish Bundled Queries, which does nothing for a custom query: they ran again
    and hit the same block."""

    MODELS = ["dcim.site", "dcim.device", "ipam.vlan"]

    def _reasons(self, *results):
        return _runner(self.MODELS)._required_query_failure_reasons(list(results))

    def test_a_custom_map_is_told_to_use_the_bundled_map_or_declare_the_parameter(self):
        text = self._reasons(_result("dcim.device", "site-rules-custom-map"))[0]

        self.assertIn("dcim.device", text)
        self.assertIn("custom", text)
        self.assertIn("Enable the bundled map", text)
        self.assertIn("map's parameters", text)
        self.assertNotIn("Publish Bundled Queries", text)

    def test_a_published_query_that_predates_the_rules_is_told_to_publish(self):
        for reason in ("site-rules-query-rejected", "site-rules-query-not-declared"):
            with self.subTest(reason=reason):
                text = self._reasons(_result("dcim.site", reason))[0]

                self.assertIn("Publish Bundled Queries", text)
                self.assertNotIn("Enable the bundled map", text)

    def test_both_causes_at_once_each_get_their_own_advice(self):
        reasons = self._reasons(
            _result("dcim.device", "site-rules-custom-map"),
            _result("dcim.site", "site-rules-query-rejected"),
        )
        text = " ".join(reasons)

        self.assertIn("Enable the bundled map", text)
        self.assertIn("Publish Bundled Queries", text)
        self.assertIn("dcim.device", text)
        self.assertIn("dcim.site", text)
