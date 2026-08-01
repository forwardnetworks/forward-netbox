# A failed ownership reconciliation used to record the exception class and
# nothing else, so a customer's errored job read "failed
# (OwnershipConflictError)" whatever had refused it - the same shape as the
# merge `__all__` failure that cost a support round-trip to diagnose.
#
# The first fix enriched one job's error path. The job the customer actually
# hit was a different function that formats the same sentence, so the fix
# passed its own tests and changed nothing they saw. These tests pin the
# behaviour at the shared formatter *and* at that job's path, because covering
# only the helper is what let the gap through.

from unittest.mock import patch

from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.diagnostics import ownership_conflict_reason
from forward_netbox.utilities.diagnostics import safe_operation_failure
from forward_netbox.utilities.ownership import OwnershipConflictError


class OwnershipConflictReasonTest(TestCase):
    CONDITIONS = (
        ("Forward device identity is ambiguous for source key abc", "identity-ambiguous"),
        (
            "Identity evidence does not match merged device state",
            "identity-evidence-mismatch",
        ),
        (
            "Source key abc maps to multiple live NetBox devices",
            "source-key-multiple-devices",
        ),
        (
            "Device dev-1 is already mapped to Forward source key abc",
            "device-already-mapped",
        ),
    )

    def test_every_known_condition_resolves_to_its_slug(self):
        for message, slug in self.CONDITIONS:
            with self.subTest(slug=slug):
                self.assertEqual(
                    ownership_conflict_reason(OwnershipConflictError(message)),
                    slug,
                )

    def test_an_uncatalogued_message_is_recorded_not_dropped(self):
        self.assertEqual(
            ownership_conflict_reason(OwnershipConflictError("brand new condition")),
            "unrecognized-ownership-conflict",
        )

    def test_the_shared_formatter_names_the_rule(self):
        # Every failure path formats through this function, which is why the
        # reason belongs here rather than at any one call site.
        message = safe_operation_failure(
            "Forward scope reconciliation",
            OwnershipConflictError(self.CONDITIONS[0][0]),
        )
        self.assertEqual(
            message,
            "Forward scope reconciliation failed "
            "(OwnershipConflictError: identity-ambiguous).",
        )

    def test_the_message_never_carries_the_device_key(self):
        # The raw message embeds a source device key, which is customer data.
        message = safe_operation_failure(
            "Forward scope reconciliation",
            OwnershipConflictError(
                "Device dc11-edge-01 is already mapped to Forward source key SRC-77"
            ),
        )
        self.assertNotIn("dc11-edge-01", message)
        self.assertNotIn("SRC-77", message)
        self.assertIn("device-already-mapped", message)

    def test_other_exceptions_are_unchanged(self):
        self.assertEqual(
            safe_operation_failure("Forward sync", ValueError("boom")),
            "Forward sync failed (ValueError).",
        )


class ScopeTagJobRecordsTheConflictReasonTest(TestCase):
    """The path a customer actually hit: reconcile device scope tags."""

    def setUp(self):
        source = ForwardSource.objects.create(
            name="conflict-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(name="conflict-sync", source=source)

    def test_the_job_data_names_the_rule_in_message_and_field(self):
        from forward_netbox.jobs import _reconcile_forward_device_scope_tags_work

        job = type("Job", (), {})()
        job.object_id = self.sync.pk
        job.data = None
        job.save = lambda **kwargs: None

        failure = OwnershipConflictError(
            "Source key SRC-9 maps to multiple live NetBox devices"
        )
        with (
            patch(
                "forward_netbox.utilities.scope_reconciliation.tag_backfilled_devices",
                side_effect=failure,
            ),
            patch("forward_netbox.jobs._mark_overlay_ownership_failed"),
        ):
            with self.assertRaises(OwnershipConflictError):
                _reconcile_forward_device_scope_tags_work(job)

        self.assertEqual(job.data["error_type"], "OwnershipConflictError")
        self.assertEqual(job.data["conflict_reason"], "source-key-multiple-devices")
        self.assertIn("source-key-multiple-devices", job.data["error"])
        self.assertNotIn("SRC-9", job.data["error"])
