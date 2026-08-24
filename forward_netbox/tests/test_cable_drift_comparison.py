# Slice two of the adapter-only drift comparison: `dcim.cable`.
#
# Cables differ from tagged items in where the writes live. Both of this path's
# writes are DIRECT - `cable.save()` for a status change, `Cable(...).save()`
# for a new one - so the preview runner's firewall covers neither, and
# `Cable.save()` additionally makes NetBox core persist the two
# `CableTermination` rows that are the durable relationship.
#
# The refusals carry as much weight as the writes here. A LAG endpoint and an
# already-cabled interface are rows the apply declines to write; counting
# either as a create would report drift that no apply would ever resolve, so
# the report would never converge.
from dcim.models import Cable
from dcim.models import CableTermination
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows


class CablePreviewTest(TestCase):
    def setUp(self):
        site = Site.objects.create(name="C Site", slug="c-site")
        mfr = Manufacturer.objects.create(name="C Mfr", slug="c-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="C DT", slug="c-dt")
        role = DeviceRole.objects.create(name="C Role", slug="c-role")
        self.left = Device.objects.create(
            name="left-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.right = Device.objects.create(
            name="right-dev", site=site, device_type=dtype, role=role, status="active"
        )
        self.left_if = Interface.objects.create(
            device=self.left, name="Ethernet1", type="1000base-t"
        )
        self.right_if = Interface.objects.create(
            device=self.right, name="Ethernet1", type="1000base-t"
        )

    def _row(self, **extra):
        row = {
            "device": "left-dev",
            "interface": "Ethernet1",
            "remote_device": "right-dev",
            "remote_interface": "Ethernet1",
            "status": "connected",
        }
        row.update(extra)
        return row

    def _cable_them(self, status="connected"):
        cable = Cable(
            a_terminations=[self.left_if],
            b_terminations=[self.right_if],
            status=status,
        )
        cable.full_clean()
        cable.save()
        return cable

    # --- the negative space -------------------------------------------------

    def test_a_preview_creates_no_cable_and_no_terminations(self):
        cables_before = Cable.objects.count()
        terminations_before = CableTermination.objects.count()

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(Cable.objects.count(), cables_before)
        # The terminations are what NetBox core writes inside `Cable.save()`,
        # so they are the assertion that a "return before the save" would have
        # been insufficient if the return had been placed one line later.
        self.assertEqual(CableTermination.objects.count(), terminations_before)
        self.assertEqual(result["creates"], 1)

    def test_a_preview_does_not_rewrite_a_drifted_status(self):
        cable = self._cable_them(status="planned")

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        cable.refresh_from_db()
        self.assertEqual(cable.status, "planned")
        self.assertEqual(result["updates"], 1)

    def test_a_preview_deletes_no_cable(self):
        self._cable_them()
        cables_before = Cable.objects.count()

        compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(Cable.objects.count(), cables_before)

    # --- classification -----------------------------------------------------

    def test_an_uncabled_pair_is_a_create(self):
        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_a_matching_cable_is_unchanged(self):
        self._cable_them(status="connected")

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 0, "unchanged": 1, "rejected": 0}
        )

    def test_a_cable_whose_status_drifted_is_an_update(self):
        self._cable_them(status="planned")

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(
            result, {"creates": 0, "updates": 1, "unchanged": 0, "rejected": 0}
        )

    # --- the refusals, which must not read as drift -------------------------

    def test_a_lag_endpoint_is_rejected_not_a_create(self):
        # NetBox refuses cables terminated directly to a LAG, so the apply
        # declines the row. Counting it as a create would report drift that no
        # run could ever clear.
        self.left_if.type = "lag"
        self.left_if.save()

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 1)

    def test_an_already_cabled_interface_is_rejected_not_a_create(self):
        # The near interface is cabled to a THIRD interface, so the row's pair
        # is not connected and never will be while that cable stands.
        other = Interface.objects.create(
            device=self.right, name="Ethernet99", type="1000base-t"
        )
        cable = Cable(
            a_terminations=[self.left_if],
            b_terminations=[other],
            status="connected",
        )
        cable.full_clean()
        cable.save()
        cables_before = Cable.objects.count()

        result = compare_model_rows(None, "dcim.cable", [self._row()])

        self.assertEqual(Cable.objects.count(), cables_before)
        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 1)

    def test_an_unknown_device_is_rejected(self):
        result = compare_model_rows(
            None, "dcim.cable", [self._row(remote_device="no-such-device")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_an_unknown_interface_is_rejected(self):
        result = compare_model_rows(
            None, "dcim.cable", [self._row(remote_interface="Ethernet404")]
        )

        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_mixed_batch_is_not_all_or_nothing(self):
        third = Device.objects.create(
            name="third-dev",
            site=self.left.site,
            device_type=self.left.device_type,
            role=self.left.role,
            status="active",
        )
        third_if = Interface.objects.create(
            device=third, name="Ethernet1", type="1000base-t"
        )
        cable = Cable(
            a_terminations=[self.right_if],
            b_terminations=[third_if],
            status="connected",
        )
        cable.full_clean()
        cable.save()

        result = compare_model_rows(
            None,
            "dcim.cable",
            [
                # right-dev:Ethernet1 is taken by the cable above.
                self._row(),
                self._row(remote_device="no-such-device"),
            ],
        )

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 2)

    # --- parity with the apply ----------------------------------------------

    def test_the_preview_count_matches_what_an_apply_actually_writes(self):
        rows = [self._row()]

        predicted = compare_model_rows(None, "dcim.cable", rows)

        before = Cable.objects.count()
        self._apply_for_real(rows)
        written = Cable.objects.count() - before

        self.assertEqual(predicted["creates"], written)

    def test_a_second_preview_after_the_apply_reports_no_drift(self):
        rows = [self._row()]
        self._apply_for_real(rows)

        result = compare_model_rows(None, "dcim.cable", rows)

        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)

    def _apply_for_real(self, rows):
        from forward_netbox.utilities.sync_cable import apply_dcim_cable

        runner = _WritingRunner()
        for row in rows:
            apply_dcim_cable(runner, row)


class _WritingRunner:
    """The real primitives, no preview - so the parity tests apply for real."""

    def __init__(self):
        self._content_types = {}
        self._device_by_name_cache = {}
        self._missing_device_by_name_cache = set()
        self._interface_by_device_name_cache = {}
        self._interface_canonical_cache = {}
        self._missing_interface_by_device_name_cache = set()
        self._unique_lookup_cache = {}
        self._primed_missing_unique_lookup_keys = set()
        self._model_coalesce_fields = {}
        self._cable_between_cache = {}
        self.logger = _NullLogger()

    def _get_device_by_name(self, name):
        from forward_netbox.utilities.sync_primitives import get_device_by_name

        return get_device_by_name(self, name)

    def _lookup_interface(self, device, interface_name):
        from forward_netbox.utilities.sync_primitives import lookup_interface

        return lookup_interface(self, device, interface_name)

    def _get_unique_or_raise(self, model, lookup):
        from forward_netbox.utilities.sync_primitives import get_unique_or_raise

        return get_unique_or_raise(self, model, lookup)

    def _conflict_policy(self, model_string):
        return "strict"

    def _dependency_failed(self, model_string, key):
        return False

    def _record_issue(self, *args, **kwargs):
        return None

    def _record_aggregated_skip_warning(self, **kwargs):
        return None

    def _record_aggregated_conflict_warning(self, **kwargs):
        return None


class _NullLogger:
    def increment_statistics(self, *args, **kwargs):
        return None

    def log_warning(self, *args, **kwargs):
        return None
