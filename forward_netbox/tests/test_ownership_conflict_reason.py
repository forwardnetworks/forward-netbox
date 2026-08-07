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
        (
            "Forward device identity is ambiguous for source key abc",
            "identity-ambiguous",
        ),
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
        # Tag-ownership refusals. Every one of these was uncatalogued, so the
        # entire scope-tag reconciliation job could only report
        # `unrecognized-ownership-conflict` - which is what a customer hit.
        (
            "Tag slug `owner-a` is already controlled as `backfilled` "
            "and cannot also be `scope`.",
            "tag-claim-type-conflict",
        ),
        (
            "Tag slug `forward-backfilled` is reserved for Forward status "
            "ownership but already exists without plugin provenance.",
            "tag-slug-reserved-without-provenance",
        ),
        (
            "Scope tag name `A.Person` and normalized slug `aperson` identify "
            "different NetBox tags.",
            "scope-tag-name-slug-disagree",
        ),
        (
            "Refusing name-only tag mutation because device identity is "
            "unresolved or ambiguous: abc",
            "tag-mutation-identity-unresolved",
        ),
        (
            "Virtual-parent claims disagree for one or more devices; durable "
            "claims were retained and the existing parent links were preserved.",
            "virtual-parent-claims-disagree",
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

    def test_every_raise_site_in_the_source_is_catalogued(self):
        # Listing conditions by hand is what let the tag-side refusals go
        # uncatalogued: the four that were listed all came from one function,
        # and nobody noticed the other five existed. This reads the raise sites
        # out of the modules themselves, so adding one without a slug fails
        # here rather than in a customer's job record.
        import ast
        import pathlib

        import forward_netbox.utilities.ownership as ownership_module
        import forward_netbox.utilities.vsys_parent as vsys_module

        uncatalogued = []
        for module in (ownership_module, vsys_module):
            path = pathlib.Path(module.__file__)
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Raise) or node.exc is None:
                    continue
                call = node.exc
                if not isinstance(call, ast.Call):
                    continue
                func = call.func
                name = getattr(func, "id", None) or getattr(func, "attr", None)
                if name != "OwnershipConflictError" or not call.args:
                    continue
                # Keep only the literal parts: an f-string placeholder holds a
                # tag name or device key, which is exactly what must not be
                # matched on.
                literal = "".join(
                    part.value
                    for part in ast.walk(call.args[0])
                    if isinstance(part, ast.Constant) and isinstance(part.value, str)
                )
                reason = ownership_conflict_reason(OwnershipConflictError(literal))
                if reason == "unrecognized-ownership-conflict":
                    uncatalogued.append(f"{path.name}:{node.lineno}")

        self.assertEqual(
            uncatalogued,
            [],
            "OwnershipConflictError raise sites with no catalogued slug: "
            f"{uncatalogued}. Add one to _OWNERSHIP_CONFLICT_REASONS.",
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
