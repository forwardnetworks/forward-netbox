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
