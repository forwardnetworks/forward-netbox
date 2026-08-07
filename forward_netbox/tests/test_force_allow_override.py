"""An operator's force-allow must carry forward to the next run.

`_forced_validation_override_applies` read `sync.latest_validation_run`, and on
the sync path `record_plan_validation` CREATES the new run before blocking
reasons are evaluated. So the lookup returned the run being recorded, whose
`override_applied` is always False, and the override could never fire on the
only path that matters: an operator force-allowed a blocked run, and the same
reason blocked the next one with nothing to say why.

The old coverage called `_blocking_reasons` directly with no current run, which
exercises the branch that still worked and passes straight over the dead one.
These tests go through `record_plan_validation`, so the run exists before the
decision is made - which is the whole condition of the defect.
"""

from unittest.mock import Mock

from django.test import TestCase

from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.validation import ForwardValidationRunner

SNAPSHOT = "snapshot-blocked"


class ForceAllowOverrideCarriesForwardTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="override-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        self.policy = ForwardDriftPolicy.objects.create(
            name="override-policy",
            enabled=True,
            require_processed_snapshot=True,
        )
        self.sync = ForwardSync.objects.create(
            name="override-sync",
            source=self.source,
            drift_policy=self.policy,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )

    def context(self):
        # An unprocessed snapshot: a blocking reason that is stable across runs,
        # so the only thing that can change the verdict is the override.
        return {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": SNAPSHOT,
            "snapshot_info": {"state": "UNPROCESSED"},
            "snapshot_metrics": {},
        }

    def runner(self):
        return ForwardValidationRunner(sync=self.sync, client=None, logger_=Mock())

    def force_allowed_previous_run(self, **overrides):
        values = {
            "sync": self.sync,
            "policy": self.policy,
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": SNAPSHOT,
            "override_applied": True,
            "override_blocking_reasons": ["Target snapshot is not processed."],
            "status": ForwardValidationStatusChoices.PASSED,
        }
        values.update(overrides)
        return ForwardValidationRun.objects.create(**values)

    def record(self):
        return self.runner().record_plan_validation(
            self.context(),
            plan=[],
            model_results=[],
            raise_on_block=False,
        )

    def test_without_an_override_the_run_is_blocked(self):
        # Establishes that the reason really does block, so the next test is
        # measuring the override rather than an absent reason.
        self.record()
        run = self.sync.validation_runs.order_by("-pk").first()
        self.assertTrue(run.blocking_reasons)

    def test_a_previous_force_allow_carries_forward_through_the_sync_path(self):
        self.force_allowed_previous_run()

        self.record()

        run = self.sync.validation_runs.order_by("-pk").first()
        self.assertEqual(
            run.blocking_reasons or [],
            [],
            "the operator's force-allow did not carry forward; the override "
            "resolved against the run being recorded again",
        )

    def test_an_override_for_a_different_snapshot_does_not_carry_forward(self):
        self.force_allowed_previous_run(snapshot_id="some-other-snapshot")

        self.record()

        run = self.sync.validation_runs.order_by("-pk").first()
        self.assertTrue(run.blocking_reasons)

    def test_an_override_for_a_different_policy_does_not_carry_forward(self):
        other = ForwardDriftPolicy.objects.create(name="other-policy", enabled=True)
        self.force_allowed_previous_run(policy=other)

        self.record()

        run = self.sync.validation_runs.order_by("-pk").first()
        self.assertTrue(run.blocking_reasons)

    def test_a_previous_run_that_was_not_force_allowed_does_not_carry_forward(self):
        self.force_allowed_previous_run(
            override_applied=False, override_blocking_reasons=[]
        )

        self.record()

        run = self.sync.validation_runs.order_by("-pk").first()
        self.assertTrue(run.blocking_reasons)
