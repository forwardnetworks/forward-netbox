# Non-field validation rules must be nameable, and must not wedge a baseline.
#
# A customer's merge failed on `ipam.ipaddress` with `ValidationError`, and every
# diagnostic surface said `__all__` - a field name meaning "no field". The message
# that would have identified the rule is interpolated by NetBox before the
# exception exists and is discarded by our recorder, so it reached neither the
# log, the database, nor the support bundle. Asking the customer to grep for it
# was guaranteed to return nothing.
from django.core.exceptions import ValidationError
from django.db.utils import IntegrityError
from django.test import SimpleTestCase

from forward_netbox.utilities.diagnostics import describe_failure
from forward_netbox.utilities.diagnostics import redacted_message_shape
from forward_netbox.utilities.diagnostics import structured_failure_diagnosis
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
