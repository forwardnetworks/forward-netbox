"""Measured density must replace the hardcoded table, and only when earned.

`set_model_change_density` had no production caller at all: the learning
machinery was fully built — confidence scoring, variance, safety factors, budget
widening — and permanently fed an empty map. Three independent customer bundles
confirmed it, each reporting `model_count=0` and all 29 models on
`default_density`, so every deployment sized row budgets from the hardcoded
`DEFAULT_MODEL_CHANGE_DENSITY` table forever.

Density is *changes per source row of that workload*, which is what
`effective_row_budget_for_model` divides the budget by. The attribution is the
whole game: grouping branch changes by the changed object's content type credits
a cable's terminations to `dcim.cabletermination`, measuring ~1.0 for a model
whose real density is 3.0 — an under-estimate that sizes row budgets too
generously in exactly the cases where density matters.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.density_learning import density_confidence_score
from forward_netbox.utilities.density_learning import (
    learned_density_map_from_profile,
)
from forward_netbox.utilities.density_learning import record_density_observation

WHEN = "2026-07-29T12:00:00+00:00"


class DensityObservationTest(SimpleTestCase):
    def _observe(self, profile, *, rows, changes, model="dcim.cable"):
        return record_density_observation(
            profile, model, rows=rows, changes=changes, observed_at=WHEN
        )

    def test_a_single_observation_records_the_measured_ratio(self):
        # 300 changes from 100 cable rows is a density of 3.0 — the value the
        # hardcoded table asserts for dcim.cable without evidence.
        profile = self._observe({}, rows=100, changes=300)
        self.assertAlmostEqual(profile["dcim.cable"]["density"], 3.0)
        self.assertEqual(profile["dcim.cable"]["sample_count"], 1)
        self.assertEqual(profile["dcim.cable"]["accepted_observations"], 1)

    def test_repeated_observations_converge_on_the_mean(self):
        profile = {}
        for changes in (300, 200, 400):
            profile = self._observe(profile, rows=100, changes=changes)
        self.assertAlmostEqual(profile["dcim.cable"]["density"], 3.0)
        self.assertEqual(profile["dcim.cable"]["sample_count"], 3)

    def test_variance_is_retained_without_keeping_samples(self):
        profile = {}
        for changes in (100, 300, 500):
            profile = self._observe(profile, rows=100, changes=changes)
        # densities 1.0, 3.0, 5.0 -> sample variance 4.0
        self.assertAlmostEqual(profile["dcim.cable"]["variance"], 4.0, places=6)

    def test_agreeing_observations_earn_more_confidence_than_scattered_ones(self):
        steady, scattered = {}, {}
        for _ in range(8):
            steady = self._observe(steady, rows=100, changes=300)
        for changes in (10, 900, 20, 800, 30, 700, 40, 600):
            scattered = self._observe(scattered, rows=100, changes=changes)

        def score(profile):
            entry = profile["dcim.cable"]
            return density_confidence_score(
                sample_count=entry["sample_count"],
                variance=entry["variance"],
                last_updated_at=WHEN,
            )

        self.assertGreater(score(steady), score(scattered))

    def test_an_observation_with_no_rows_is_rejected_not_recorded(self):
        # A zero-row workload carries no information about density.
        profile = self._observe({}, rows=0, changes=500)
        self.assertNotIn("dcim.cable", profile)

    def test_a_rejected_observation_leaves_a_learned_density_untouched(self):
        profile = self._observe({}, rows=100, changes=300)
        profile = self._observe(profile, rows=0, changes=999999)
        self.assertAlmostEqual(profile["dcim.cable"]["density"], 3.0)
        self.assertEqual(profile["dcim.cable"]["rejected_observations"], 1)
        self.assertEqual(profile["dcim.cable"]["sample_count"], 1)

    def test_an_absurd_ratio_is_clamped_rather_than_trusted(self):
        # MAX_MODEL_DENSITY is 1000; a runaway measurement must not size budgets.
        profile = self._observe({}, rows=1, changes=10_000_000)
        self.assertLessEqual(profile["dcim.cable"]["density"], 1000.0)

    def test_zero_changes_is_rejected_rather_than_learned_as_zero(self):
        # Learning 0 would widen the row budget without bound, letting a shard
        # exceed the change budget the budget exists to enforce. clamp_density
        # already refuses anything below MIN_MODEL_DENSITY, and that is the safe
        # direction: a density we cannot trust must not move the budget.
        profile = self._observe({}, rows=100, changes=0)
        self.assertNotIn("dcim.cable", profile)

    def test_models_are_tracked_independently(self):
        profile = self._observe({}, rows=100, changes=300, model="dcim.cable")
        profile = self._observe(profile, rows=100, changes=1600, model="x.vuln")
        self.assertAlmostEqual(profile["dcim.cable"]["density"], 3.0)
        self.assertAlmostEqual(profile["x.vuln"]["density"], 16.0)

    def test_the_learned_map_is_what_the_budget_consumes(self):
        profile = self._observe({}, rows=100, changes=300)
        self.assertEqual(learned_density_map_from_profile(profile), {"dcim.cable": 3.0})

    def test_an_empty_profile_yields_an_empty_map(self):
        self.assertEqual(learned_density_map_from_profile({}), {})

    def test_a_blank_model_string_is_ignored(self):
        self.assertEqual(
            record_density_observation(
                {}, "  ", rows=10, changes=10, observed_at=WHEN
            ),
            {},
        )
