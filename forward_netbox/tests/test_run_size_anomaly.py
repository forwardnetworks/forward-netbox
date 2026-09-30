"""When a run's size is out of proportion for a model, and when it is not.

The rule feeds a merge hold, so the negative space matters as much as the
positive: a first load into an empty table, a small model's ordinary churn and a
model with too little history must all be left alone.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.run_size_anomaly import assess_run_size
from forward_netbox.utilities.run_size_anomaly import BASIS_HISTORY
from forward_netbox.utilities.run_size_anomaly import BASIS_TABLE
from forward_netbox.utilities.run_size_anomaly import describe_finding
from forward_netbox.utilities.run_size_anomaly import GROWTH_MULTIPLE
from forward_netbox.utilities.run_size_anomaly import HISTORY_MIN_RUNS
from forward_netbox.utilities.run_size_anomaly import HISTORY_MULTIPLE
from forward_netbox.utilities.run_size_anomaly import MIN_CHANGES
from forward_netbox.utilities.run_size_anomaly import MIN_ESTABLISHED_ROWS

MODEL = "netbox_routing.bgppeer"


def _one(changes, existing):
    return {MODEL: {"changes": changes, "existing_rows": existing}}


class TableNormTest(SimpleTestCase):
    def test_a_run_many_times_the_table_is_flagged(self):
        # The case this exists for: 16,605 rows held, ~127k staged.
        findings = assess_run_size(_one(127_422, 16_605))

        self.assertEqual(len(findings), 1)
        finding = findings[0]
        self.assertEqual(finding["model"], MODEL)
        self.assertEqual(finding["basis"], BASIS_TABLE)
        self.assertEqual(finding["norm"], 16_605)
        self.assertEqual(finding["multiple"], 7.7)

    def test_a_first_load_into_an_empty_table_is_never_flagged(self):
        self.assertEqual(assess_run_size(_one(500_000, 0)), [])
        self.assertEqual(assess_run_size(_one(500_000, None)), [])

    def test_a_table_below_the_established_size_is_never_flagged(self):
        self.assertEqual(assess_run_size(_one(500_000, MIN_ESTABLISHED_ROWS - 1)), [])

    def test_growth_below_the_multiple_is_not_flagged(self):
        existing = 50_000
        changes = int(existing * GROWTH_MULTIPLE) - 1
        self.assertEqual(assess_run_size(_one(changes, existing)), [])

    def test_growth_at_the_multiple_is_flagged(self):
        existing = 50_000
        changes = int(existing * GROWTH_MULTIPLE)
        self.assertEqual(len(assess_run_size(_one(changes, existing))), 1)

    def test_below_the_absolute_floor_is_never_flagged(self):
        # 6x an established table, but only a few thousand changes.
        self.assertEqual(assess_run_size(_one(MIN_CHANGES - 1, 1_500)), [])


class HistoryNormTest(SimpleTestCase):
    def test_a_run_many_times_the_typical_run_is_flagged(self):
        findings = assess_run_size(
            _one(120_000, None), history={MODEL: [200, 180, 220, 190]}
        )

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["basis"], BASIS_HISTORY)
        self.assertEqual(findings[0]["runs"], 4)
        self.assertGreaterEqual(findings[0]["multiple"], HISTORY_MULTIPLE)

    def test_too_few_prior_runs_is_no_norm(self):
        history = {MODEL: [200] * (HISTORY_MIN_RUNS - 1)}
        self.assertEqual(assess_run_size(_one(120_000, None), history=history), [])

    def test_runs_that_applied_nothing_do_not_count_as_history(self):
        history = {MODEL: [0, 0, 0, 0, 200]}
        self.assertEqual(assess_run_size(_one(120_000, None), history=history), [])

    def test_a_run_within_the_multiple_of_typical_is_not_flagged(self):
        history = {MODEL: [10_000, 12_000, 11_000]}
        self.assertEqual(assess_run_size(_one(60_000, None), history=history), [])


class ShapeTest(SimpleTestCase):
    def test_both_norms_report_the_larger_multiple(self):
        findings = assess_run_size(
            _one(100_000, 20_000), history={MODEL: [1_000, 1_000, 1_000]}
        )

        self.assertEqual(findings[0]["basis"], BASIS_HISTORY)
        self.assertEqual(findings[0]["multiple"], 100.0)

    def test_findings_are_largest_first_and_only_the_flagged_models(self):
        models = {
            "a.small": {"changes": 30_000, "existing_rows": 2_000},
            "b.big": {"changes": 90_000, "existing_rows": 2_000},
            "c.fine": {"changes": 30_000, "existing_rows": 100_000},
        }

        findings = assess_run_size(models)

        self.assertEqual([item["model"] for item in findings], ["b.big", "a.small"])

    def test_junk_input_is_harmless(self):
        self.assertEqual(assess_run_size(None), [])
        self.assertEqual(assess_run_size({MODEL: {"changes": "x"}}), [])
        self.assertEqual(
            assess_run_size(_one(50_000, 2_000), history={MODEL: ["a", None, -3]}),
            assess_run_size(_one(50_000, 2_000)),
        )

    def test_the_sentence_names_the_model_and_the_multiple(self):
        table = assess_run_size(_one(127_422, 16_605))[0]
        history = assess_run_size(
            _one(120_000, None), history={MODEL: [200, 180, 220]}
        )[0]

        self.assertIn(MODEL, describe_finding(table))
        self.assertIn("7.7x", describe_finding(table))
        self.assertIn("16,605", describe_finding(table))
        self.assertIn("last 3", describe_finding(history))
