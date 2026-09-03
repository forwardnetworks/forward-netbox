# The verify gate's ordering, and the absence problem it exists to catch.
#
# A device that failed collection is ABSENT from the Forward model rather than
# flagged in it, so "no violations found" and "the broken device was not in the
# snapshot" look identical from the rows alone. That is the same shape as a
# customer's 552 uncovered devices, and it is why completeness is step one
# rather than a footnote on the report.
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.change_control.choices import ForwardChangeVerdictChoices
from forward_netbox.change_control.gates import collection_postdates_apply
from forward_netbox.change_control.gates import device_set_complete
from forward_netbox.change_control.gates import verify

APPLIED = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def _change(*, keys=("dev-a", "dev-b"), applied_at=APPLIED):
    devices = [
        SimpleNamespace(forward_device_key=k, device=f"device-{k}") for k in keys
    ]
    return SimpleNamespace(
        applied_at=applied_at,
        devices=SimpleNamespace(all=lambda: devices),
        criteria=SimpleNamespace(all=lambda: []),
    )


class AnAbsentDeviceRefusesTheVerdictTest(SimpleTestCase):
    def test_a_complete_device_set_passes(self):
        result = device_set_complete(_change(), ["dev-a", "dev-b"])

        self.assertTrue(result)

    def test_a_missing_device_refuses_rather_than_warns(self):
        result = device_set_complete(_change(), ["dev-a"])

        self.assertFalse(result)
        self.assertIn("absent from this snapshot", result.reasons[0])

    def test_the_reason_explains_why_absence_is_not_a_pass(self):
        # The sentence matters: an operator who reads "1 device missing" may
        # accept it, and one who reads why absence is indistinguishable from
        # health will not.
        result = device_set_complete(_change(), [])

        self.assertIn("failed collection", result.reasons[0])
        self.assertIn("rather than marked failed", result.reasons[0])


class CollectionMustPostdateTheApplyTest(SimpleTestCase):
    def test_collection_after_the_apply_passes(self):
        times = {
            "dev-a": APPLIED + timedelta(minutes=5),
            "dev-b": APPLIED + timedelta(minutes=6),
        }

        self.assertTrue(collection_postdates_apply(_change(), times))

    def test_collection_before_the_apply_holds(self):
        times = {
            "dev-a": APPLIED - timedelta(minutes=1),
            "dev-b": APPLIED + timedelta(minutes=5),
        }
        result = collection_postdates_apply(_change(), times)

        self.assertFalse(result)
        self.assertIn("collected before the change was applied", result.reasons[0])

    def test_an_unknown_collection_time_is_held_not_assumed(self):
        # The weakest joint in the workflow. Assuming the snapshot is new
        # enough is how a verdict gets computed against the wrong network.
        result = collection_postdates_apply(
            _change(), {"dev-a": APPLIED + timedelta(minutes=5)}
        )

        self.assertFalse(result)
        self.assertIn("Unknown is held, not assumed", result.reasons[-1])

    def test_no_attested_apply_time_holds(self):
        result = collection_postdates_apply(_change(applied_at=None), {})

        self.assertFalse(result)


class TheGateRunsInOrderTest(SimpleTestCase):
    def test_an_incomplete_device_set_short_circuits_the_criteria(self):
        # The criteria are not evaluated at all, so no evidence row is written
        # that would later read as a real measurement of this snapshot.
        verdict, reasons = verify(
            _change(),
            observed_device_keys=["dev-a"],
            device_collection_times={"dev-a": APPLIED + timedelta(minutes=5)},
        )

        self.assertEqual(verdict, ForwardChangeVerdictChoices.HOLD)
        self.assertTrue(any("absent from this snapshot" in r for r in reasons))

    def test_a_complete_and_timely_snapshot_reaches_the_criteria(self):
        # With no criteria defined, the verdict falls through to the criteria
        # stage and holds for a different, honest reason.
        verdict, reasons = verify(
            _change(),
            observed_device_keys=["dev-a", "dev-b"],
            device_collection_times={
                "dev-a": APPLIED + timedelta(minutes=5),
                "dev-b": APPLIED + timedelta(minutes=5),
            },
        )

        self.assertEqual(verdict, ForwardChangeVerdictChoices.HOLD)
        self.assertTrue(any("nothing to verify against" in r for r in reasons))


class APrematureAttestationIsCaughtTest(SimpleTestCase):
    """APPLIED cannot be proved from here, but its usual failure is detectable.

    If every criterion reports the same result before and after, Forward
    observed no change. That is not proof the change never landed - a change
    can legitimately leave every criterion where it was - so it holds with a
    question rather than an accusation.
    """

    # NOT `_outcome`: unittest.TestCase sets an internal `_outcome`
    # attribute during run(), which shadows a method of that name and
    # fails with "'_Outcome' object is not callable".
    def _make_outcome(self, flip):
        return SimpleNamespace(flip=flip)

    def test_no_movement_at_all_holds(self):
        from forward_netbox.change_control.evidence import PRESERVED
        from forward_netbox.change_control.gates import attestation_looks_premature

        result = attestation_looks_premature(
            _change(), [self._make_outcome(PRESERVED), self._make_outcome(PRESERVED)]
        )

        self.assertFalse(result)
        self.assertIn("observed no change to the network", result.reasons[0])

    def test_the_reason_offers_the_innocent_explanation_too(self):
        from forward_netbox.change_control.evidence import PRESERVED
        from forward_netbox.change_control.gates import attestation_looks_premature

        result = attestation_looks_premature(_change(), [self._make_outcome(PRESERVED)])

        self.assertIn("changed nothing the criteria ask about", result.reasons[0])

    def test_any_movement_satisfies_it(self):
        from forward_netbox.change_control.evidence import FIX
        from forward_netbox.change_control.evidence import PRESERVED
        from forward_netbox.change_control.gates import attestation_looks_premature

        result = attestation_looks_premature(
            _change(), [self._make_outcome(PRESERVED), self._make_outcome(FIX)]
        )

        self.assertTrue(result)

    def test_nothing_measured_is_not_treated_as_no_movement(self):
        # An unmeasured criterion says nothing about whether the network moved,
        # and must not be turned into evidence that it did not.
        from forward_netbox.change_control.gates import attestation_looks_premature

        self.assertTrue(
            attestation_looks_premature(_change(), [self._make_outcome("")])
        )
