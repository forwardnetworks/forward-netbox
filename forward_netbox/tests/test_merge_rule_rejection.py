# Non-field validation rules must be nameable, and must not wedge a baseline.
#
# A customer's merge failed on `ipam.ipaddress` with `ValidationError`, and every
# diagnostic surface said `__all__` - a field name meaning "no field". The message
# that would have identified the rule is interpolated by NetBox before the
# exception exists and is discarded by our recorder, so it reached neither the
# log, the database, nor the support bundle. Asking the customer to grep for it
# was guaranteed to return nothing.
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db.utils import IntegrityError
from django.test import SimpleTestCase
from django.test import TestCase
from ipam.models import IPAddress
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.choices import ForwardCatchupStatusChoices
from forward_netbox.exceptions import ForwardPartialMergeError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.diagnostics import describe_failure
from forward_netbox.utilities.diagnostics import is_preexisting_rule_rejection
from forward_netbox.utilities.diagnostics import redacted_message_shape
from forward_netbox.utilities.diagnostics import structured_failure_diagnosis
from forward_netbox.utilities.drift_report import build_latest_sync_evidence
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.health_checks import ingestion_check_message
from forward_netbox.utilities.health_checks import ingestion_check_status
from forward_netbox.utilities.merge import _is_destination_rule_rejection


class DestinationRuleRejectionTests(SimpleTestCase):
    """Only validation rejections are unsatisfiable; the rest stay retryable."""

    def test_validation_error_is_a_destination_rule_rejection(self):
        self.assertTrue(_is_destination_rule_rejection(ValidationError("nope")))

    def test_integrity_error_remains_a_retryable_failure(self):
        # Integrity errors are usually ordering or contention, where retrying
        # is exactly right - and where skipping would hide a real defect.
        self.assertFalse(_is_destination_rule_rejection(IntegrityError("dup")))

    def test_arbitrary_exceptions_remain_retryable_failures(self):
        self.assertFalse(_is_destination_rule_rejection(RuntimeError("boom")))

    def test_a_rejection_the_merge_caused_is_a_retryable_failure(self):
        # A catalogued rule landing on a field this change writes is our defect,
        # not a property of the destination. Retrying is the right response, and
        # calling it unsatisfiable hides it behind a disposition meant for rows
        # nothing can fix.
        exc = ValidationError(
            {
                "untagged_vlan": [
                    "The untagged VLAN (Vlan211 (211)) must belong to the same "
                    "site as the interface's parent device, or it must be global."
                ]
            }
        )
        exc.forward_written_fields = {"untagged_vlan", "mtu"}
        self.assertFalse(_is_destination_rule_rejection(exc))

    def test_the_same_rule_on_an_unwritten_field_stays_unsatisfiable(self):
        exc = ValidationError(
            {
                "untagged_vlan": [
                    "The untagged VLAN (Vlan211 (211)) must belong to the same "
                    "site as the interface's parent device, or it must be global."
                ]
            }
        )
        exc.forward_written_fields = {"mtu"}
        self.assertTrue(_is_destination_rule_rejection(exc))

    def test_an_uncatalogued_rule_stays_unsatisfiable_even_on_a_written_field(self):
        # The merge default is to skip, so narrowing it the way the sync path is
        # narrowed would flip every uncatalogued rejection to a failure and wedge
        # the baselines this classifier exists to protect.
        exc = ValidationError({"mtu": ["Ensure this value is less than 65536."]})
        exc.forward_written_fields = {"mtu"}
        self.assertTrue(_is_destination_rule_rejection(exc))

    def test_a_caller_that_never_said_what_it_writes_stays_unsatisfiable(self):
        # Today's behaviour for every path not yet taught to attach the set.
        self.assertTrue(
            _is_destination_rule_rejection(
                ValidationError(
                    {
                        "__all__": [
                            "10.0.0.0/24 is a network ID, which "
                            "may not be assigned to an interface."
                        ]
                    }
                )
            )
        )

    def test_the_customer_network_id_rejection_still_skips(self):
        # `__all__` cannot intersect a written field, so the rejection that this
        # classifier was built for keeps its disposition either way.
        exc = ValidationError(
            {
                "__all__": [
                    "10.0.0.0/24 is a network ID, which may not be assigned "
                    "to an interface."
                ]
            }
        )
        exc.forward_written_fields = {"address", "status"}
        self.assertTrue(_is_destination_rule_rejection(exc))


class NonFieldValidationRuleTests(SimpleTestCase):
    """Each catalogued rule resolves to its slug, and values never persist."""

    def _diagnose(self, message):
        return structured_failure_diagnosis(ValidationError(message))

    def test_network_id_rule_is_named(self):
        diagnosis = self._diagnose(
            "10.24.8.0/22 is a network ID, which may not be assigned "
            "to an interface."
        )
        self.assertEqual(["network-id-not-assignable"], diagnosis["validation_rules"])

    def test_broadcast_rule_is_named(self):
        diagnosis = self._diagnose(
            "10.24.8.255/22 is a broadcast address, which may not be assigned "
            "to an interface."
        )
        self.assertEqual(["broadcast-not-assignable"], diagnosis["validation_rules"])

    def test_primary_ip_reassignment_rule_is_named(self):
        diagnosis = self._diagnose(
            "Cannot reassign IP address while it is designated as the primary "
            "IP for the parent object"
        )
        self.assertEqual(
            ["primary-ip-reassignment-blocked"], diagnosis["validation_rules"]
        )

    def test_oob_ip_reassignment_rule_is_named(self):
        diagnosis = self._diagnose(
            "Cannot reassign IP address while it is designated as the OOB IP "
            "for the parent object"
        )
        self.assertEqual(["oob-ip-reassignment-blocked"], diagnosis["validation_rules"])

    def test_catalogued_rules_never_persist_the_address(self):
        diagnosis = self._diagnose(
            "10.24.8.0/22 is a network ID, which may not be assigned "
            "to an interface."
        )
        self.assertNotIn("10.24.8.0", repr(diagnosis))


class UnrecognizedRuleTests(SimpleTestCase):
    """An uncatalogued rule must still be readable, without leaking values."""

    def test_wording_survives_and_values_do_not(self):
        diagnosis = structured_failure_diagnosis(
            ValidationError(
                "Device core-sw-01.dc11 already claims 10.1.1.1/24 on Vlan211"
            )
        )
        shape = " ".join(diagnosis["unrecognized_validation_rules"])
        self.assertIn("already claims", shape)
        for secret in ("core-sw-01.dc11", "10.1.1.1/24", "Vlan211"):
            self.assertNotIn(secret, shape)

    def test_a_rule_nobody_has_read_yet_still_reaches_the_message(self):
        diagnosis = structured_failure_diagnosis(
            ValidationError("Some future rule forbids this thing")
        )
        message = describe_failure("Merge for ipam.ipaddress failed.", diagnosis)
        self.assertIn("forbids this thing", message)

    def test_tokens_bearing_digits_or_punctuation_are_masked(self):
        shape = redacted_message_shape("host-9 at 2001:db8::1 failed badly")
        self.assertNotIn("host-9", shape)
        self.assertNotIn("2001:db8::1", shape)
        self.assertIn("failed", shape)
        self.assertIn("badly", shape)


class DescribeFailurePrecedenceTests(SimpleTestCase):
    """One helper, so the three recorders cannot drift apart again."""

    def test_constraint_wins_over_fields(self):
        message = describe_failure(
            "Merge for dcim.device failed.",
            {
                "constraint_name": "dcim_device_primary_ip4_id_key",
                "invalid_fields": ["x"],
            },
        )
        self.assertIn("on constraint dcim_device_primary_ip4_id_key.", message)

    def test_rules_win_over_bare_field_names(self):
        message = describe_failure(
            "Merge for ipam.ipaddress failed.",
            {
                "validation_rules": ["network-id-not-assignable"],
                "invalid_fields": ["__all__"],
            },
        )
        self.assertIn("violating network-id-not-assignable.", message)
        self.assertNotIn("__all__", message)

    def test_field_names_are_still_used_when_nothing_better_exists(self):
        message = describe_failure(
            "Merge for ipam.ipaddress failed.",
            {"invalid_fields": ["address"]},
        )
        self.assertIn("on invalid field(s) address.", message)

    def test_a_named_field_and_its_rule_are_both_reported(self):
        # They are different facts. The field says where the rejection landed,
        # the rule says what rejected it, and reporting only the field is what
        # left a customer's `untagged_vlan` failure ambiguous between two
        # unrelated NetBox rules.
        message = describe_failure(
            "Forward ingestion failed (ValidationError).",
            {
                "invalid_fields": ["untagged_vlan"],
                "validation_rules": ["untagged-vlan-outside-device-site"],
            },
        )
        self.assertIn("on invalid field(s) untagged_vlan", message)
        self.assertIn("violating untagged-vlan-outside-device-site.", message)


class PreexistingRuleRejectionTests(SimpleTestCase):
    """One predicate for every apply path, so they cannot disagree again."""

    CROSS_SITE = ValidationError(
        {
            "untagged_vlan": [
                "The untagged VLAN (Vlan211 (211)) must belong to the same site "
                "as the interface's parent device, or it must be global."
            ]
        }
    )

    def test_catalogued_rule_on_an_unwritten_field_is_preexisting(self):
        self.assertTrue(is_preexisting_rule_rejection(self.CROSS_SITE, {"mtu"}))

    def test_the_same_rule_on_a_written_field_is_ours(self):
        self.assertFalse(
            is_preexisting_rule_rejection(self.CROSS_SITE, {"mtu", "untagged_vlan"})
        )

    def test_an_uncatalogued_rule_is_never_preexisting(self):
        # Skipping is the disposition for a rejection we understand. Treating
        # what we cannot name as someone else's problem downgrades real defects.
        self.assertFalse(
            is_preexisting_rule_rejection(
                ValidationError({"mtu": ["Ensure this value is less than 65536."]}),
                {"description"},
            )
        )

    def test_a_non_validation_error_is_never_preexisting(self):
        self.assertFalse(is_preexisting_rule_rejection(IntegrityError("dup"), set()))

    def test_no_written_fields_still_requires_a_catalogued_rule(self):
        self.assertFalse(
            is_preexisting_rule_rejection(ValidationError("something odd"), set())
        )


class FieldScopedValidationRuleTests(SimpleTestCase):
    """A rule must be nameable whether or not NetBox scoped it to a field.

    Reading `__all__` alone made a rule legible exactly when NetBox declined to
    say which field it concerned, and illegible whenever NetBox did say. One
    field routinely carries several unrelated rules, so the field name alone
    does not identify the rejection.
    """

    UNTAGGED_VLAN_SITE = ValidationError(
        {
            "untagged_vlan": [
                "The untagged VLAN (Vlan211 (211)) must belong to the same site "
                "as the interface's parent device, or it must be global."
            ]
        }
    )
    UNTAGGED_VLAN_MODE = ValidationError(
        {"untagged_vlan": ["Interface mode does not support an untagged vlan."]}
    )

    def test_cross_site_untagged_vlan_is_named(self):
        diagnosis = structured_failure_diagnosis(self.UNTAGGED_VLAN_SITE)
        self.assertEqual(["untagged_vlan"], diagnosis["invalid_fields"])
        self.assertEqual(
            ["untagged-vlan-outside-device-site"], diagnosis["validation_rules"]
        )

    def test_untagged_vlan_without_mode_is_named(self):
        diagnosis = structured_failure_diagnosis(self.UNTAGGED_VLAN_MODE)
        self.assertEqual(
            ["untagged-vlan-needs-interface-mode"], diagnosis["validation_rules"]
        )

    def test_the_two_untagged_vlan_rules_are_distinguishable(self):
        # The whole point: a customer sync recorded `untagged_vlan` and nothing
        # else, and these two have different causes and different fixes.
        self.assertNotEqual(
            structured_failure_diagnosis(self.UNTAGGED_VLAN_SITE)["validation_rules"],
            structured_failure_diagnosis(self.UNTAGGED_VLAN_MODE)["validation_rules"],
        )

    def test_an_uncatalogued_field_rule_keeps_its_wording(self):
        diagnosis = structured_failure_diagnosis(
            ValidationError(
                {"mtu": ["Ensure this value is less than or equal to 65536."]}
            )
        )
        self.assertNotIn("validation_rules", diagnosis)
        shape = " ".join(diagnosis["unrecognized_validation_rules"])
        self.assertIn("Ensure this value is less than or equal to", shape)

    def test_values_in_a_field_scoped_message_are_still_masked(self):
        # Field-scoped messages quote submitted values just as freely as
        # non-field ones; reading more messages must not start persisting them.
        diagnosis = structured_failure_diagnosis(
            ValidationError({"name": ["Device host-9.internal.example is invalid."]})
        )
        shape = " ".join(diagnosis["unrecognized_validation_rules"])
        self.assertNotIn("host-9.internal.example", shape)
        self.assertIn("invalid", shape)


PRIMARY_IP_REJECTION = ValidationError(
    {
        "__all__": [
            "Cannot reassign IP address while it is designated as the primary "
            "IP for the parent object"
        ]
    }
)


class UnsatisfiableRowDispositionTest(TestCase):
    """The skip has to reach the counters, not just the raise decision.

    2.6.12 stopped a validation rejection from raising `ForwardPartialMergeError`,
    so the branch attests and the baseline promotes. The row was still persisted
    in `failed_change_count`, and every readiness surface reads that field: drift
    evidence returned `failed`, the ingestion health check returned `fail`, and
    throughput read "remains incomplete". A customer whose only exception was one
    permanently unsatisfiable `ipam.ipaddress` row therefore saw
    "applied N, failed 1" and an unmeasurable drift report on every single run,
    which is indistinguishable from the hard block the skip was meant to remove.
    """

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="rule-owner")
        self.source = ForwardSource.objects.create(
            name="rule-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="rule-sync",
            source=self.source,
            user=self.user,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        self.branch = Branch.objects.create(
            name=f"rule-{uuid4().hex[:10]}",
            schema_id=f"rule_{uuid4().hex[:10]}",
            status=BranchStatusChoices.READY,
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            branch=self.branch,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-rule",
        )

    @staticmethod
    def _changes(total):
        changes = MagicMock()
        changes.order_by.return_value = changes
        changes.exists.return_value = total > 0
        changes.values.return_value.distinct.return_value.count.return_value = total
        changes.values_list.return_value.distinct.return_value = []
        changes.annotate.return_value = []
        return changes

    def _merge(self, exceptions, *, applied):
        """Merge `applied` clean rows plus one row per exception."""
        total = applied + len(exceptions)
        changes = self._changes(total)

        def bulk(*_args, record_failed=None, result_metadata=None, **_kwargs):
            for exception in exceptions:
                record_failed(
                    SimpleNamespace(
                        model_class=IPAddress,
                        key=("ipam.ipaddress", 1),
                    ),
                    exception,
                )
            result_metadata.update(
                logical_total=total,
                logical_action_counts={"update": applied},
            )
            return applied, len(exceptions), set()

        with (
            patch.object(Branch, "get_unmerged_changes", return_value=changes),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes", side_effect=bulk
            ),
        ):
            self.ingestion.sync_merge(remove_branch=False)

    def test_a_rule_rejection_is_recorded_skipped_and_promotes_the_baseline(self):
        self._merge([PRIMARY_IP_REJECTION], applied=509)
        self.ingestion.refresh_from_db()

        self.assertIsNotNone(
            self.ingestion.merge_applied_at,
            "an unsatisfiable row must not stop the merge being attested",
        )
        self.assertTrue(
            self.ingestion.baseline_ready,
            "the baseline must promote over a permanently unsatisfiable row",
        )
        self.assertEqual(0, self.ingestion.failed_change_count)
        self.assertEqual(1, self.ingestion.skipped_change_count)
        self.assertEqual(509, self.ingestion.applied_change_count)
        # The promotion that matters: this ingestion is now the sync's baseline,
        # which is what makes drift measurable and later runs diff-eligible.
        self.assertEqual(self.ingestion, self.sync.latest_baseline_ingestion())

    def test_the_skipped_row_stays_recorded_and_named(self):
        # Skipping must never mean discarding: the row remains an ingestion
        # issue and still names the rule that refused it.
        self._merge([PRIMARY_IP_REJECTION], applied=509)

        issues = list(self.ingestion.issues.values_list("model", "message"))
        self.assertEqual(1, len(issues))
        model, message = issues[0]
        self.assertEqual("ipam.ipaddress", model)
        self.assertIn("primary-ip-reassignment-blocked", message)
        self.assertIn("Recorded and skipped", message)

    def test_drift_evidence_no_longer_reports_the_run_as_failed(self):
        self._merge([PRIMARY_IP_REJECTION], applied=509)
        self.ingestion.refresh_from_db()

        evidence = build_latest_sync_evidence(self.ingestion)
        self.assertNotEqual(
            "failed",
            evidence["status"],
            "a skipped row is not a failed run; reporting it as one is what "
            "left drift unmeasurable forever",
        )
        self.assertEqual(0, evidence["failed"])
        self.assertEqual(1, evidence["skipped"])

    def test_health_reports_the_skip_rather_than_a_failure(self):
        self._merge([PRIMARY_IP_REJECTION], applied=509)
        self.ingestion.refresh_from_db()
        # Post-merge snapshot catch-up is reported ahead of everything else and
        # would mask the disposition; settle it so this asserts what it claims.
        self.ingestion.catchup_status = ForwardCatchupStatusChoices.CURRENT
        self.ingestion.save(update_fields=["catchup_status"])

        self.assertNotEqual(
            "fail",
            ingestion_check_status(self.ingestion),
            "a skipped row must not present as a failed ingestion",
        )
        message = ingestion_check_message(self.ingestion)
        self.assertIn("skipped 1 row(s)", message)
        self.assertNotIn("failed change(s)", message)

    def test_an_integrity_error_still_fails_and_still_blocks(self):
        # The other half of the contract. An integrity error is usually ordering
        # or contention, where a retry is exactly right and skipping would hide
        # a real defect.
        with self.assertRaises(ForwardPartialMergeError):
            self._merge([IntegrityError("dup")], applied=509)
        self.ingestion.refresh_from_db()

        self.assertIsNone(
            self.ingestion.merge_applied_at,
            "a retryable failure must still leave the merge unattested",
        )
        self.assertFalse(self.ingestion.baseline_ready)
        self.assertIsNone(
            self.sync.latest_baseline_ingestion(),
            "a retryable failure must still block baseline promotion",
        )
        self.assertEqual(1, self.ingestion.failed_change_count)
        self.assertEqual(0, self.ingestion.skipped_change_count)
        self.assertEqual("failed", build_latest_sync_evidence(self.ingestion)["status"])
        self.assertTrue(
            self.ingestion.can_accept_merge_failures,
            "the operator escape hatch must stay reachable for real failures",
        )

    def test_a_mixed_run_keeps_the_counters_summing_to_the_branch_total(self):
        # A READY branch reports every change as unmerged, so a partial-merge
        # retry recomputes the same logical total and refuses to run when it
        # disagrees with the persisted counters. Losing the skipped row from
        # that sum would trade this dead end for a new one.
        with self.assertRaises(ForwardPartialMergeError):
            self._merge(
                [PRIMARY_IP_REJECTION, IntegrityError("dup")],
                applied=508,
            )
        self.ingestion.refresh_from_db()

        self.assertEqual(508, self.ingestion.applied_change_count)
        self.assertEqual(1, self.ingestion.failed_change_count)
        self.assertEqual(1, self.ingestion.skipped_change_count)
        self.assertEqual(
            510,
            self.ingestion.applied_change_count
            + self.ingestion.failed_change_count
            + self.ingestion.skipped_change_count,
        )

    def test_the_retry_guard_accepts_a_mixed_run_unchanged(self):
        with self.assertRaises(ForwardPartialMergeError):
            self._merge(
                [PRIMARY_IP_REJECTION, IntegrityError("dup")],
                applied=508,
            )
        # The retry re-merges the whole branch. It must not read the recorded
        # counters as a changed branch total.
        with self.assertRaises(ForwardPartialMergeError):
            self._merge(
                [PRIMARY_IP_REJECTION, IntegrityError("dup")],
                applied=508,
            )
        self.ingestion.refresh_from_db()
        self.assertEqual(1, self.ingestion.failed_change_count)
        self.assertEqual(1, self.ingestion.skipped_change_count)
