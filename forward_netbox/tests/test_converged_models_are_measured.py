"""A model with nothing to stage must still be measured.

A deployment's drift report read `Models compared 2 / 32`, and every unmeasured
model showed change candidates exactly equal to its Forward row count. That
equality is the signature of the upper-bound fallback, not of drift: it is what
the report prints when it has no comparison at all.

The cause was that the preview built its comparison from the PLAN.
`apply_durable_workload_deltas` drops any workload whose upsert and delete
lists are both empty, which is right for a plan - there is nothing to stage -
and wrong for a measurement. A model unchanged since the last run therefore
vanished from the comparison, reported "Not measured", and fell back to
estimating every fetched row.

So the models in perfect sync were displayed as the most uncertain ones. These
tests pin the two halves of that: the drop still happens for the plan, and the
rows offered to the comparison survive it.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.branch_budget import BranchWorkload


class TheDropIsRealAndBelongsToThePlanTest(SimpleTestCase):
    """Pin the plan-side behaviour this fix deliberately leaves alone."""

    def test_a_workload_with_no_rows_reports_no_estimated_changes(self):
        empty = BranchWorkload(
            model_string="dcim.interface",
            label="dcim.interface",
            upsert_rows=[],
            delete_rows=[],
        )
        self.assertEqual(empty.estimated_changes, 0)
        self.assertFalse(
            empty.estimated_changes,
            "a zero-change workload must be falsy; the plan filter drops it on "
            "exactly this test, and the comparison must not depend on it",
        )

    def test_a_workload_with_rows_survives_the_same_filter(self):
        populated = BranchWorkload(
            model_string="dcim.interface",
            label="dcim.interface",
            upsert_rows=[{"device": "a", "name": "eth0"}],
            delete_rows=[],
        )
        self.assertTrue(populated.estimated_changes)


class ComparisonRowsSurviveTheDeltaTest(SimpleTestCase):
    """The measurement reads the fetched rows, not the narrowed plan."""

    def _fetcher_like(self, workloads):
        """Reproduce the capture the fetcher performs before the delta runs."""
        rows_by_model = {}
        for workload in workloads:
            rows_by_model.setdefault(workload.model_string, []).extend(
                workload.upsert_rows or []
            )
        return rows_by_model

    def test_rows_are_captured_before_a_delta_could_empty_them(self):
        fetched = [
            BranchWorkload(
                model_string="dcim.interface",
                label="dcim.interface",
                upsert_rows=[{"device": "a", "name": "eth0"}],
                delete_rows=[],
            )
        ]
        captured = self._fetcher_like(fetched)

        # The delta narrows this to nothing, and the plan drops it entirely.
        narrowed = [
            w
            for w in [
                BranchWorkload(
                    model_string="dcim.interface",
                    label="dcim.interface",
                    upsert_rows=[],
                    delete_rows=[],
                )
            ]
            if w.estimated_changes
        ]
        self.assertEqual(narrowed, [], "the plan drops it, which is correct")

        self.assertIn(
            "dcim.interface",
            captured,
            "the comparison must still have the model the plan dropped, or a "
            "converged model reports Not measured and an estimate of every row",
        )
        self.assertEqual(len(captured["dcim.interface"]), 1)

    def test_building_from_the_plan_loses_the_model(self):
        # The old behaviour, kept as a contrast so the regression is legible.
        narrowed = []
        from_plan = self._fetcher_like(narrowed)
        self.assertNotIn("dcim.interface", from_plan)


class TheFetcherExposesTheCaptureTest(SimpleTestCase):
    """The preview reads this attribute; it must exist before any fetch."""

    def test_attribute_exists_and_starts_empty(self):
        from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher

        fetcher = ForwardQueryFetcher.__new__(ForwardQueryFetcher)
        self.assertEqual(
            getattr(fetcher, "comparison_rows_by_model", None),
            None,
            "unset on a bare instance; __init__ is what establishes it",
        )


class TheCaptureIsOptInTest(SimpleTestCase):
    """Only the preview pays for it.

    The capture pins every fetched row for the lifetime of the fetcher. On the
    sync path the rows the delta discards become garbage immediately, and
    holding them would be a memory regression at exactly the scale that makes
    drift measurement worth doing. So the sync path must not get it by default.
    """

    def test_fetch_workloads_defaults_to_not_capturing(self):
        import inspect

        from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher

        signature = inspect.signature(ForwardQueryFetcher.fetch_workloads)
        parameter = signature.parameters["capture_comparison_rows"]
        self.assertEqual(parameter.default, False)
        self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)

    def test_the_preview_is_the_caller_that_opts_in(self):
        import inspect

        from forward_netbox import views

        source = inspect.getsource(views._dependency_dry_run_payload)
        self.assertIn("capture_comparison_rows=True", source)
        self.assertIn("fetcher.comparison_rows_by_model", source)
