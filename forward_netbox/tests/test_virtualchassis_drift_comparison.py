# The last spec-driven model that declined to answer: `dcim.virtualchassis`.
#
# The bulk path returns None under preview because its second phase reads the
# first phase's pk. Production syncs run in a branch, where the bulk path defers
# to `apply_dcim_virtualchassis`, and that adapter's decisions ARE classifiable
# row by row: an absent chassis is a create outright, a member out of place is
# an update, a member in place takes the chassis's own verdict. So the
# comparison goes through the adapter, exactly as the apply does in a branch.
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from dcim.models import VirtualChassis
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows

MODEL = "dcim.virtualchassis"


class VirtualChassisPreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="VC Site", slug="vc-site")
        mfr = Manufacturer.objects.create(name="VC Mfr", slug="vc-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="VC DT", slug="vc-dt")
        role = DeviceRole.objects.create(name="VC Role", slug="vc-role")
        self.member = Device.objects.create(
            name="stack-1", site=site, device_type=dtype, role=role, status="active"
        )
        self.other = Device.objects.create(
            name="stack-2", site=site, device_type=dtype, role=role, status="active"
        )

    def _row(self, **extra):
        return {"vc_name": "STACK", "vc_domain": "", **extra}

    def _existing(self, *, domain=""):
        return VirtualChassis.objects.create(name="STACK", domain=domain)

    def test_a_preview_writes_nothing(self):
        before = (
            VirtualChassis.objects.count(),
            Device.objects.filter(virtual_chassis__isnull=False).count(),
        )
        counts = compare_model_rows(
            None, MODEL, [self._row(device="stack-1", vc_position=1)]
        )
        self.assertEqual(counts["creates"], 1)
        after = (
            VirtualChassis.objects.count(),
            Device.objects.filter(virtual_chassis__isnull=False).count(),
        )
        self.assertEqual(after, before)

    def test_an_absent_chassis_is_a_create(self):
        counts = compare_model_rows(None, MODEL, [self._row()])
        self.assertEqual(counts["creates"], 1)

    def test_an_existing_chassis_is_unchanged(self):
        self._existing()
        counts = compare_model_rows(None, MODEL, [self._row()])
        self.assertEqual(counts["unchanged"], 1)

    def test_a_domain_change_is_an_update(self):
        self._existing(domain="old")
        counts = compare_model_rows(None, MODEL, [self._row(vc_domain="new")])
        self.assertEqual(counts["updates"], 1)

    def test_a_member_not_yet_in_the_chassis_is_an_update(self):
        self._existing()
        counts = compare_model_rows(
            None, MODEL, [self._row(device="stack-1", vc_position=1)]
        )
        self.assertEqual(counts["updates"], 1)

    def test_a_member_in_place_takes_the_chassis_verdict(self):
        vc = self._existing()
        Device.objects.filter(pk=self.member.pk).update(
            virtual_chassis=vc, vc_position=1
        )
        counts = compare_model_rows(
            None, MODEL, [self._row(device="stack-1", vc_position=1)]
        )
        self.assertEqual(counts["unchanged"], 1)

    def test_a_member_at_the_wrong_position_is_an_update(self):
        vc = self._existing()
        Device.objects.filter(pk=self.member.pk).update(
            virtual_chassis=vc, vc_position=2
        )
        counts = compare_model_rows(
            None, MODEL, [self._row(device="stack-1", vc_position=1)]
        )
        self.assertEqual(counts["updates"], 1)

    def test_a_member_row_without_a_position_is_rejected(self):
        # The apply skips it with an aggregated warning; the preview says the
        # same thing, in the count the report keeps apart from drift.
        counts = compare_model_rows(None, MODEL, [self._row(device="stack-1")])
        self.assertEqual(counts["rejected"], 1)
        self.assertEqual(counts["creates"], 0)

    def test_an_unknown_member_device_is_rejected(self):
        self._existing()
        counts = compare_model_rows(
            None, MODEL, [self._row(device="nope", vc_position=1)]
        )
        self.assertEqual(counts["rejected"], 1)

    def test_a_position_conflict_is_rejected_not_drift(self):
        vc = self._existing()
        Device.objects.filter(pk=self.other.pk).update(
            virtual_chassis=vc, vc_position=1
        )
        counts = compare_model_rows(
            None, MODEL, [self._row(device="stack-1", vc_position=1)]
        )
        self.assertEqual(counts["rejected"], 1)

    def test_the_model_is_now_measured_when_empty(self):
        self.assertIsNotNone(compare_model_rows(None, MODEL, []))
