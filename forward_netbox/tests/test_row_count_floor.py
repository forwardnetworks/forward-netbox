"""The per-model row-count floor.

A query head that still declares this plugin's parameters and still returns the
fields the model needs, but returns a narrower row set, passes every other check
in the pipeline: parameters validate, row shape validates, the sync reports
success, and the rows that are no longer returned are reconciled as deletions.
These tests pin the one thing that sees it - a comparison against what the same
models returned in the last ingestion that promoted a baseline - and, just as
importantly, pin the cases it must not block.
"""

from unittest.mock import Mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardDriftPolicyBaselineChoices
from forward_netbox.choices import ForwardValidationStatusChoices
from forward_netbox.models import ForwardDriftPolicy
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardValidationRun
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.validation import DEFAULT_MAX_ROW_SHRINK_PERCENT
from forward_netbox.utilities.validation import ForwardValidationRunner
from forward_netbox.utilities.validation import MIN_ROW_SHRINK_ROWS
from forward_netbox.utilities.validation import ROW_SHRINK_REASON_PREFIX
from forward_netbox.utilities.validation import row_shrink_findings


def model_result(
    model,
    row_count,
    *,
    sync_mode="full",
    failure_count=0,
    scope_config_fingerprint="scope-a",
):
    """One entry in the `model_results` list, in its persisted `as_dict` shape."""
    return {
        "model": model,
        "row_count": row_count,
        "delete_count": 0,
        "failure_count": failure_count,
        "sync_mode": sync_mode,
        "scope_config_fingerprint": scope_config_fingerprint,
    }


class RowShrinkFindingsTest(TestCase):
    """The pure threshold arithmetic, with no database and no sync."""

    def findings(self, current, baseline, *, percent=DEFAULT_MAX_ROW_SHRINK_PERCENT):
        return row_shrink_findings(
            current_results=current,
            baseline_results=baseline,
            enabled_models=["dcim.device"],
            max_shrink_percent=percent,
        )

    def test_shrinkage_past_the_threshold_is_reported_with_both_counts(self):
        findings = self.findings(
            [model_result("dcim.device", 1200)],
            [model_result("dcim.device", 3403)],
        )

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["model"], "dcim.device")
        self.assertEqual(findings[0]["baseline_rows"], 3403)
        self.assertEqual(findings[0]["current_rows"], 1200)
        self.assertEqual(findings[0]["dropped_rows"], 2203)
        self.assertEqual(findings[0]["dropped_percent"], 64.7)

    def test_shrinkage_within_the_threshold_is_not_reported(self):
        # 3403 -> 2500 is a 26.5% drop: real, visible in reporting, and well
        # inside what a decommissioning month does to an estate this size.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 2500)],
                [model_result("dcim.device", 3403)],
            ),
            [],
        )

    def test_shrinkage_exactly_at_the_threshold_is_not_reported(self):
        # Exactly 30% of 1000. The limit is the largest drop that still passes,
        # so an operator who sets 30 is not surprised by a 30% run.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 700)],
                [model_result("dcim.device", 1000)],
            ),
            [],
        )

    def test_growth_never_blocks(self):
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 9000)],
                [model_result("dcim.device", 3403)],
            ),
            [],
        )

    def test_small_absolute_drops_are_not_reported_however_large_the_percentage(self):
        # A reference model going 12 -> 2 is an 83% drop and means nothing.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 2)],
                [model_result("dcim.device", 12)],
            ),
            [],
        )

    def test_the_absolute_floor_is_the_documented_constant(self):
        baseline = 10 * MIN_ROW_SHRINK_ROWS
        just_under = self.findings(
            [model_result("dcim.device", baseline - MIN_ROW_SHRINK_ROWS + 1)],
            [model_result("dcim.device", baseline)],
            percent=0,
        )
        at_the_floor = self.findings(
            [model_result("dcim.device", baseline - MIN_ROW_SHRINK_ROWS)],
            [model_result("dcim.device", baseline)],
            percent=0,
        )

        self.assertEqual(just_under, [])
        self.assertEqual(len(at_the_floor), 1)

    def test_no_baseline_rows_for_the_model_is_not_a_comparison(self):
        # A newly enabled model has no prior count. Nothing to measure against.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 40)],
                [model_result("dcim.site", 900)],
            ),
            [],
        )

    def test_a_baseline_of_zero_rows_can_only_grow(self):
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 0)],
                [model_result("dcim.device", 0)],
            ),
            [],
        )

    def test_a_failed_fetch_is_not_read_as_shrinkage(self):
        # A failed fetch returns zero rows. That failure is already loud and
        # separately blocking; reporting it a second time as a 100% collapse
        # would bury the real cause.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 0, failure_count=1)],
                [model_result("dcim.device", 3403)],
            ),
            [],
        )

    def test_a_diff_run_is_not_comparable_to_a_full_run(self):
        # A diff `row_count` is the number of changed rows, not the size of the
        # row set. Comparing the two would read a quiet snapshot as a collapse.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 4, sync_mode="diff")],
                [model_result("dcim.device", 3403)],
            ),
            [],
        )
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 1200)],
                [model_result("dcim.device", 6, sync_mode="diff")],
            ),
            [],
        )

    def test_rows_are_summed_across_every_map_for_a_model(self):
        # Several maps can feed one model. The floor is about the model's whole
        # row set, so one map dropping out is caught by the total.
        findings = self.findings(
            [model_result("dcim.device", 1000)],
            [
                model_result("dcim.device", 1000),
                model_result("dcim.device", 2403),
            ],
        )

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]["baseline_rows"], 3403)
        self.assertEqual(findings[0]["current_rows"], 1000)

    def test_a_changed_scope_configuration_suspends_the_comparison(self):
        # The operator narrowed scope themselves. A smaller row set is them
        # getting what they asked for, so there is nothing to refuse.
        self.assertEqual(
            self.findings(
                [model_result("dcim.device", 400, scope_config_fingerprint="scope-b")],
                [model_result("dcim.device", 3403, scope_config_fingerprint="scope-a")],
            ),
            [],
        )

    def test_an_unknown_scope_fingerprint_does_not_suspend_the_comparison(self):
        # An older baseline may carry no fingerprint. Unknown is not evidence
        # that the operator changed anything, so the floor still applies.
        findings = self.findings(
            [model_result("dcim.device", 400)],
            [model_result("dcim.device", 3403, scope_config_fingerprint="")],
        )

        self.assertEqual(len(findings), 1)

    def test_models_not_enabled_on_this_sync_are_ignored(self):
        self.assertEqual(
            row_shrink_findings(
                current_results=[model_result("dcim.interface", 10)],
                baseline_results=[model_result("dcim.interface", 5000)],
                enabled_models=["dcim.device"],
                max_shrink_percent=DEFAULT_MAX_ROW_SHRINK_PERCENT,
            ),
            [],
        )


class RowShrinkBlockingTest(TestCase):
    """The floor as the sync actually reaches it, through the validation gate."""

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="row-floor-owner")
        self.source = ForwardSource.objects.create(
            name="row-floor-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )

    def build_sync(self, *, policy=None, name="row-floor-sync"):
        return ForwardSync.objects.create(
            name=name,
            source=self.source,
            drift_policy=policy,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )

    def build_baseline(self, sync, rows, *, snapshot_id="snapshot-baseline"):
        return ForwardIngestion.objects.create(
            sync=sync,
            baseline_ready=True,
            snapshot_id=snapshot_id,
            model_results=[model_result("dcim.device", rows)],
        )

    def context(self, snapshot_id="snapshot-current"):
        return {
            "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
            "snapshot_id": snapshot_id,
            "snapshot_info": {"state": "PROCESSED"},
            "snapshot_metrics": {},
        }

    def reasons(self, sync, current_rows, *, policy=None, validation_run=None):
        runner = ForwardValidationRunner(sync=sync, client=None, logger_=Mock())
        return runner._blocking_reasons(
            self.context(),
            plan=[],
            model_results=[model_result("dcim.device", current_rows)],
            policy=policy,
            validation_run=validation_run,
        )

    def test_narrowing_past_the_threshold_blocks_and_names_the_model(self):
        sync = self.build_sync()
        self.build_baseline(sync, 3403)

        reasons = self.reasons(sync, 1200)

        self.assertEqual(len(reasons), 1)
        reason = reasons[0]
        self.assertTrue(reason.startswith(ROW_SHRINK_REASON_PREFIX))
        self.assertIn("`dcim.device`", reason)
        self.assertIn("1200", reason)
        self.assertIn("3403", reason)
        self.assertIn("force-allow", reason)

    def test_narrowing_within_the_threshold_proceeds(self):
        sync = self.build_sync()
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 2500), [])

    def test_growth_proceeds(self):
        sync = self.build_sync()
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 9000), [])

    def test_a_first_run_proceeds(self):
        # No ingestion at all. Nothing to compare against; a first run must
        # never be blocked by a comparison it cannot make.
        sync = self.build_sync()

        self.assertEqual(self.reasons(sync, 12), [])

    def test_a_sync_whose_runs_never_promoted_a_baseline_proceeds(self):
        sync = self.build_sync()
        ForwardIngestion.objects.create(
            sync=sync,
            baseline_ready=False,
            snapshot_id="snapshot-never-promoted",
            model_results=[model_result("dcim.device", 3403)],
        )

        self.assertEqual(self.reasons(sync, 12), [])

    def test_the_floor_is_on_with_no_drift_policy_at_all(self):
        # The point of the whole exercise: this must not be another optional
        # field nobody sets. A sync with no policy is guarded.
        sync = self.build_sync()
        self.build_baseline(sync, 3403)

        reasons = self.reasons(sync, 1200, policy=None)

        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith(ROW_SHRINK_REASON_PREFIX))

    def test_the_floor_is_on_for_a_policy_created_without_naming_it(self):
        policy = ForwardDriftPolicy.objects.create(name="row-floor-default-policy")
        self.assertTrue(policy.block_on_row_shrink)
        self.assertEqual(policy.max_row_shrink_percent, DEFAULT_MAX_ROW_SHRINK_PERCENT)

        sync = self.build_sync(policy=policy)
        self.build_baseline(sync, 3403)

        reasons = self.reasons(sync, 1200, policy=policy)

        self.assertTrue(
            any(reason.startswith(ROW_SHRINK_REASON_PREFIX) for reason in reasons)
        )

    def test_a_policy_can_widen_the_threshold(self):
        policy = ForwardDriftPolicy.objects.create(
            name="row-floor-wide-policy",
            max_row_shrink_percent=90,
        )
        sync = self.build_sync(policy=policy)
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 1200, policy=policy), [])

    def test_a_policy_can_turn_the_floor_off(self):
        policy = ForwardDriftPolicy.objects.create(
            name="row-floor-off-policy",
            block_on_row_shrink=False,
        )
        sync = self.build_sync(policy=policy)
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 1200, policy=policy), [])

    def test_a_disabled_policy_turns_the_floor_off(self):
        policy = ForwardDriftPolicy.objects.create(
            name="row-floor-disabled-policy",
            enabled=False,
        )
        sync = self.build_sync(policy=policy)
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 1200, policy=policy), [])

    def test_a_baseline_free_policy_turns_the_floor_off(self):
        policy = ForwardDriftPolicy.objects.create(
            name="row-floor-no-baseline-policy",
            baseline_mode=ForwardDriftPolicyBaselineChoices.NONE,
        )
        sync = self.build_sync(policy=policy)
        self.build_baseline(sync, 3403)

        self.assertEqual(self.reasons(sync, 1200, policy=policy), [])


class RowShrinkOverrideTest(TestCase):
    """The operator's way through, and the fact that it clears itself."""

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="row-floor-override")
        self.source = ForwardSource.objects.create(
            name="row-floor-override-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": "test-network",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="row-floor-override-sync",
            source=self.source,
            parameters={
                "snapshot_id": LATEST_PROCESSED_SNAPSHOT,
                "dcim.device": True,
            },
        )
        self.baseline = ForwardIngestion.objects.create(
            sync=self.sync,
            baseline_ready=True,
            snapshot_id="snapshot-baseline",
            model_results=[model_result("dcim.device", 3403)],
        )

    def blocked_run(self, *, snapshot_id, reasons):
        return ForwardValidationRun.objects.create(
            sync=self.sync,
            status=ForwardValidationStatusChoices.BLOCKED,
            allowed=False,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id=snapshot_id,
            baseline_snapshot_id=self.baseline.snapshot_id,
            blocking_reasons=reasons,
            started=timezone.now(),
            completed=timezone.now(),
        )

    def reasons(self, *, snapshot_id="snapshot-next", validation_run=None):
        runner = ForwardValidationRunner(sync=self.sync, client=None, logger_=Mock())
        return runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": snapshot_id,
                "snapshot_info": {"state": "PROCESSED"},
                "snapshot_metrics": {},
            },
            plan=[],
            model_results=[model_result("dcim.device", 1200)],
            policy=None,
            validation_run=validation_run,
        )

    def test_a_force_allowed_run_lets_the_next_run_through(self):
        run = self.blocked_run(
            snapshot_id="snapshot-blocked",
            reasons=[f"{ROW_SHRINK_REASON_PREFIX} `dcim.device` returned 1200 row(s)."],
        )
        run.force_allow(user=self.user, reason="Site retired; the drop is real.")

        self.assertEqual(self.reasons(), [])

    def test_the_override_survives_a_new_snapshot(self):
        # What the operator accepted is "smaller than this baseline", and that
        # stays true until the baseline moves. A snapshot-scoped acceptance
        # would lapse as soon as Forward processed the next snapshot, and an
        # operator stuck in that loop turns the guard off.
        run = self.blocked_run(
            snapshot_id="snapshot-blocked",
            reasons=[f"{ROW_SHRINK_REASON_PREFIX} `dcim.device` returned 1200 row(s)."],
        )
        run.force_allow(user=self.user, reason="Site retired; the drop is real.")

        self.assertEqual(self.reasons(snapshot_id="snapshot-much-later"), [])

    def test_the_override_lapses_once_a_new_baseline_is_promoted(self):
        run = self.blocked_run(
            snapshot_id="snapshot-blocked",
            reasons=[f"{ROW_SHRINK_REASON_PREFIX} `dcim.device` returned 1200 row(s)."],
        )
        run.force_allow(user=self.user, reason="Site retired; the drop is real.")

        # The accepted run got through and promoted its own baseline. The
        # override was about the old one, so it stops applying - and a further
        # collapse below the new, smaller count blocks again.
        ForwardIngestion.objects.create(
            sync=self.sync,
            baseline_ready=True,
            snapshot_id="snapshot-accepted",
            model_results=[model_result("dcim.device", 1200)],
        )
        runner = ForwardValidationRunner(sync=self.sync, client=None, logger_=Mock())
        reasons = runner._blocking_reasons(
            {
                "snapshot_selector": LATEST_PROCESSED_SNAPSHOT,
                "snapshot_id": "snapshot-later",
                "snapshot_info": {"state": "PROCESSED"},
                "snapshot_metrics": {},
            },
            plan=[],
            model_results=[model_result("dcim.device", 100)],
            policy=None,
        )

        self.assertEqual(len(reasons), 1)
        self.assertIn("1200", reasons[0])

    def test_an_override_of_some_other_reason_does_not_clear_the_floor(self):
        run = self.blocked_run(
            snapshot_id="snapshot-blocked",
            reasons=["Target snapshot is not processed."],
        )
        run.force_allow(user=self.user, reason="Accepted an unprocessed snapshot.")

        reasons = self.reasons()

        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith(ROW_SHRINK_REASON_PREFIX))

    def test_the_run_being_recorded_cannot_clear_itself(self):
        # The current run is created before blocking reasons are computed, so
        # it is the newest run in the table. It must be excluded, or a
        # force-allow would be self-fulfilling on the very next attempt.
        current = self.blocked_run(snapshot_id="snapshot-next", reasons=[])
        current.override_applied = True
        current.override_blocking_reasons = [
            f"{ROW_SHRINK_REASON_PREFIX} `dcim.device` returned 1200 row(s)."
        ]
        current.save(update_fields=["override_applied", "override_blocking_reasons"])

        reasons = self.reasons(validation_run=current)

        self.assertEqual(len(reasons), 1)
        self.assertTrue(reasons[0].startswith(ROW_SHRINK_REASON_PREFIX))
