# The 2.8.6 mixed-model ChangeDiff corruption was fixed at the sink and its
# plan recorded "no persistent damage is expected in main" - an expectation.
# This is the measurement that makes it answerable against a real database.
from dcim.models import Device
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from ipam.models import IPAddress

from forward_netbox.utilities.changediff_audit import classify_change_diff
from forward_netbox.utilities.changediff_audit import foreign_payload_keys

DEVICE_SNAPSHOT = {
    "id": 5,
    "name": "leaf-1",
    "device_type": 3,
    "role": 2,
    "site": 1,
    "status": "active",
    "custom_fields": {},
    "tags": [],
}
ADDRESS_SNAPSHOT = {
    "id": 9,
    "address": "10.0.0.1/24",
    "vrf": None,
    "status": "active",
    "assigned_object_type": None,
    "assigned_object_id": None,
    "custom_fields": {},
    "tags": [],
}


class ChangeDiffClassificationTest(TestCase):
    def test_a_device_payload_under_an_address_diff_is_flagged(self):
        # The exact corruption: object_type says IPAddress, original holds a
        # serialized Device - the devices snapshot()-ed by the primary-IP
        # release, grouped into an IPAddress batch.
        findings = classify_change_diff(
            ContentType.objects.get_for_model(IPAddress),
            {"original": DEVICE_SNAPSHOT, "modified": None, "current": None},
        )
        self.assertEqual(set(findings), {"original"})
        self.assertIn("device_type", findings["original"])
        self.assertIn("site", findings["original"])

    def test_a_payload_that_fits_its_model_is_clean(self):
        self.assertEqual(
            classify_change_diff(
                ContentType.objects.get_for_model(IPAddress),
                {
                    "original": ADDRESS_SNAPSHOT,
                    "modified": ADDRESS_SNAPSHOT,
                    "current": None,
                },
            ),
            {},
        )
        self.assertEqual(
            classify_change_diff(
                ContentType.objects.get_for_model(Device),
                {"original": DEVICE_SNAPSHOT, "modified": None, "current": None},
            ),
            {},
        )

    def test_one_stray_key_is_not_a_finding(self):
        # A serializer quirk, not another model. Two or more foreign keys is
        # the threshold; one alone stays unreported.
        payload = {**ADDRESS_SNAPSHOT, "unexpected": 1}
        self.assertEqual(
            classify_change_diff(
                ContentType.objects.get_for_model(IPAddress), {"original": payload}
            ),
            {},
        )

    def test_serializer_keys_are_never_foreign(self):
        self.assertEqual(
            foreign_payload_keys(
                IPAddress, {"id": 1, "display": "x", "url": "y", "tags": []}
            ),
            set(),
        )

    def test_values_are_never_reported(self):
        findings = classify_change_diff(
            ContextTypeStub.address(),
            {"original": DEVICE_SNAPSHOT},
        )
        self.assertNotIn("leaf-1", str(findings))


class ContextTypeStub:
    @staticmethod
    def address():
        return ContentType.objects.get_for_model(IPAddress)
