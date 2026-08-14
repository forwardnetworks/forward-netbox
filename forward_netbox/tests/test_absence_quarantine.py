# A device disabled in Forward vanishes from `network.devices` and from the REST
# inventory alike, so the plugin cannot tell a maintenance window from a
# decommissioning. Before this quarantine, the very next sync deleted it - 76
# devices went that way in one run at a customer. These tests pin the negative
# space: what must NOT be deleted, and when.
from datetime import timedelta
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardDeviceAbsence
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import absence_quarantine_thresholds
from forward_netbox.utilities.scope_reconciliation import DEFAULT_PRUNE_ABSENCE_HOURS
from forward_netbox.utilities.scope_reconciliation import DEFAULT_PRUNE_ABSENCE_RUNS
from forward_netbox.utilities.scope_reconciliation import partition_quarantined_orphans
from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices
from forward_netbox.utilities.scope_reconciliation import record_device_absence
from forward_netbox.utilities.scope_reconciliation import ScopeShrinkGuardError


class AbsenceQuarantineTestBase(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="quarantine-src",
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
        self.sync = ForwardSync.objects.create(
            name="quarantine-sync",
            source=self.source,
        )
        site = Site.objects.create(name="Q Site", slug="q-site")
        manufacturer = Manufacturer.objects.create(name="Q Mfr", slug="q-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Q Model",
            slug="q-model",
        )
        role = DeviceRole.objects.create(name="Q Role", slug="q-role")
        self.devices = [
            Device.objects.create(
                name=f"quarantine-device-{index}",
                site=site,
                device_type=device_type,
                role=role,
            )
            for index in range(3)
        ]

    def _report(self, devices):
        names = {device.name for device in devices}
        return {
            "_out_of_scope": names,
            "_out_of_scope_pks": [device.pk for device in devices],
            "_tagged_names": {"still-here"},
            "_device_tagged_names": {"still-here"},
            "netbox_out_of_scope": len(devices),
            # Large enough that the shrink guard never fires; this file is about
            # the quarantine, and one guard masking another proves nothing.
            "forward_previously_managed": 10000,
        }

    def _set_absent(self, device, *, runs, hours_ago):
        moment = timezone.now() - timedelta(hours=hours_ago)
        return ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=device,
            consecutive_absent_runs=runs,
            first_absent_at=moment,
            last_absent_at=timezone.now(),
        )


class AbsenceQuarantineHoldsTest(AbsenceQuarantineTestBase):
    def test_a_device_with_no_absence_record_is_not_pruned(self):
        """Fail closed. Never recorded absent means zero confirmed absences."""
        result = prune_orphan_devices(self.sync, report=self._report(self.devices))

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["quarantine_held_device_count"], 3)
        self.assertEqual(Device.objects.filter(pk=self.devices[0].pk).count(), 1)

    def test_too_few_runs_is_not_pruned_however_old(self):
        # The exact shape of a device disabled a fortnight ago and confirmed
        # once: old enough, not confirmed enough.
        self._set_absent(self.devices[0], runs=1, hours_ago=336)

        result = prune_orphan_devices(
            self.sync,
            report=self._report([self.devices[0]]),
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["quarantine_held_device_count"], 1)
        self.assertTrue(Device.objects.filter(pk=self.devices[0].pk).exists())

    def test_enough_runs_too_recent_is_not_pruned(self):
        # And the mirror image: an hourly sync running the count up inside an
        # hour. This is the case the run threshold alone cannot catch, and the
        # reason both thresholds exist.
        self._set_absent(
            self.devices[0],
            runs=DEFAULT_PRUNE_ABSENCE_RUNS + 5,
            hours_ago=1,
        )

        result = prune_orphan_devices(
            self.sync,
            report=self._report([self.devices[0]]),
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["quarantine_held_device_count"], 1)
        self.assertTrue(Device.objects.filter(pk=self.devices[0].pk).exists())

    def test_a_device_past_both_thresholds_is_pruned(self):
        """A quarantine that never releases is just a broken prune."""
        self._set_absent(
            self.devices[0],
            runs=DEFAULT_PRUNE_ABSENCE_RUNS,
            hours_ago=DEFAULT_PRUNE_ABSENCE_HOURS + 1,
        )

        result = prune_orphan_devices(
            self.sync,
            report=self._report([self.devices[0]]),
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["quarantine_held_device_count"], 0)
        self.assertFalse(Device.objects.filter(pk=self.devices[0].pk).exists())

    def test_only_the_released_device_is_pruned(self):
        # A mixed batch must not be all-or-nothing in either direction.
        self._set_absent(
            self.devices[0],
            runs=DEFAULT_PRUNE_ABSENCE_RUNS,
            hours_ago=DEFAULT_PRUNE_ABSENCE_HOURS + 1,
        )
        self._set_absent(self.devices[1], runs=1, hours_ago=1)

        result = prune_orphan_devices(
            self.sync,
            report=self._report(self.devices),
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["quarantine_held_device_count"], 2)
        self.assertFalse(Device.objects.filter(pk=self.devices[0].pk).exists())
        self.assertTrue(Device.objects.filter(pk=self.devices[1].pk).exists())
        self.assertTrue(Device.objects.filter(pk=self.devices[2].pk).exists())


class AbsenceQuarantineOverrideTest(AbsenceQuarantineTestBase):
    def test_the_manual_override_deletes_held_devices(self):
        """An override that cannot fire leaves an operator with no way through."""
        result = prune_orphan_devices(
            self.sync,
            report=self._report([self.devices[0]]),
            include_quarantined=True,
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["quarantine_overridden_device_count"], 1)
        self.assertEqual(result["quarantine_held_device_count"], 0)

    def test_the_scheduled_path_does_not_pass_the_override(self):
        """The unattended path is the one that caused the harm."""
        from forward_netbox.jobs import _prune_forward_orphans_work

        captured = {}

        def _capture(sync, **kwargs):
            captured.update(kwargs)
            return {"pruned_device_count": 0, "pruned_object_count": 0}

        job = type("Job", (), {"object_id": self.sync.pk, "data": None})()
        job.save = lambda **kwargs: None
        with patch(
            "forward_netbox.utilities.scope_reconciliation.compute_scope_reconciliation",
            return_value=self._report([]),
        ), patch(
            "forward_netbox.utilities.scope_reconciliation.prune_orphan_devices",
            side_effect=_capture,
        ), patch(
            "forward_netbox.utilities.scope_reconciliation.prune_orphan_sites",
            return_value={"pruned_site_count": 0},
        ):
            _prune_forward_orphans_work(job)

        self.assertIs(captured["include_quarantined"], False)


class AbsenceStreakRecordingTest(AbsenceQuarantineTestBase):
    def test_an_absence_starts_then_advances(self):
        first = record_device_absence(self.sync, [self.devices[0].pk])
        second = record_device_absence(self.sync, [self.devices[0].pk])

        self.assertEqual(first["started"], 1)
        self.assertEqual(second["advanced"], 1)
        row = ForwardDeviceAbsence.objects.get(sync=self.sync, device=self.devices[0])
        self.assertEqual(row.consecutive_absent_runs, 2)

    def test_a_returning_device_clears_its_streak_entirely(self):
        # Not decremented. Two absences either side of a presence say nothing
        # together, so the second one starts from zero.
        record_device_absence(self.sync, [self.devices[0].pk])
        record_device_absence(self.sync, [self.devices[0].pk])

        cleared = record_device_absence(self.sync, [])

        self.assertEqual(cleared["cleared"], 1)
        self.assertFalse(ForwardDeviceAbsence.objects.filter(sync=self.sync).exists())

    def test_a_streak_does_not_resume_across_a_presence(self):
        record_device_absence(self.sync, [self.devices[0].pk])
        record_device_absence(self.sync, [self.devices[0].pk])
        record_device_absence(self.sync, [])
        record_device_absence(self.sync, [self.devices[0].pk])

        row = ForwardDeviceAbsence.objects.get(sync=self.sync, device=self.devices[0])
        self.assertEqual(row.consecutive_absent_runs, 1)

    def test_first_absent_at_survives_an_advance(self):
        # The time threshold measures from the FIRST absence; if an advance
        # refreshed it, the clock would restart every run and never expire.
        record_device_absence(self.sync, [self.devices[0].pk])
        original = ForwardDeviceAbsence.objects.get(
            sync=self.sync,
            device=self.devices[0],
        ).first_absent_at
        record_device_absence(self.sync, [self.devices[0].pk])

        refreshed = ForwardDeviceAbsence.objects.get(
            sync=self.sync,
            device=self.devices[0],
        )
        self.assertEqual(refreshed.first_absent_at, original)

    def test_another_syncs_streak_is_untouched(self):
        other_sync = ForwardSync.objects.create(
            name="other-quarantine-sync",
            source=self.source,
        )
        record_device_absence(other_sync, [self.devices[0].pk])

        record_device_absence(self.sync, [])

        self.assertTrue(
            ForwardDeviceAbsence.objects.filter(sync=other_sync).exists(),
        )


class AbsenceQuarantineConfigurationTest(AbsenceQuarantineTestBase):
    def test_the_defaults_apply_when_unset(self):
        self.assertEqual(
            absence_quarantine_thresholds(self.sync),
            (DEFAULT_PRUNE_ABSENCE_RUNS, DEFAULT_PRUNE_ABSENCE_HOURS),
        )

    def test_zeroing_both_thresholds_disables_the_quarantine(self):
        # "0" in both boxes has to mean no quarantine. Fail-closed on a missing
        # row would otherwise hold every device forever and read as a bug.
        self.source.parameters["device_tag_prune_absence_runs"] = 0
        self.source.parameters["device_tag_prune_absence_hours"] = 0
        self.source.save()
        self.sync.refresh_from_db()

        partition = partition_quarantined_orphans(
            self.sync,
            [device.pk for device in self.devices],
        )

        self.assertEqual(len(partition["eligible_pks"]), 3)
        self.assertEqual(partition["held_pks"], [])

    def test_a_garbage_threshold_falls_back_rather_than_disabling(self):
        # A bad value must not silently become "delete immediately".
        self.source.parameters["device_tag_prune_absence_runs"] = "not-a-number"
        self.source.parameters["device_tag_prune_absence_hours"] = -5
        self.source.save()
        self.sync.refresh_from_db()

        self.assertEqual(
            absence_quarantine_thresholds(self.sync),
            (DEFAULT_PRUNE_ABSENCE_RUNS, DEFAULT_PRUNE_ABSENCE_HOURS),
        )


class PruneResultShapeIsUniformTest(AbsenceQuarantineTestBase):
    """Every exit from the prune returns the same keys.

    The early returns used to omit keys the full path carried, so a caller
    reading `result["ownership_blocked_device_count"]` worked or raised
    KeyError depending on how far the prune happened to get - a difference
    nothing in the signature hints at.
    """

    EXPECTED_KEYS = {
        "pruned_device_count",
        "pruned_object_count",
        "out_of_scope_sample",
        "ownership_blocked_device_count",
        "protected_device_count",
        "quarantine_required_runs",
        "quarantine_required_hours",
        "quarantine_held_device_count",
        "quarantine_overridden_device_count",
    }

    def test_the_no_orphans_exit_carries_every_key(self):
        result = prune_orphan_devices(self.sync, report=self._report([]))

        self.assertEqual(self.EXPECTED_KEYS - set(result), set())

    def test_the_all_held_exit_carries_every_key(self):
        result = prune_orphan_devices(self.sync, report=self._report(self.devices))

        self.assertEqual(self.EXPECTED_KEYS - set(result), set())

    def test_the_deleting_exit_carries_every_key(self):
        self._set_absent(
            self.devices[0],
            runs=DEFAULT_PRUNE_ABSENCE_RUNS,
            hours_ago=DEFAULT_PRUNE_ABSENCE_HOURS + 1,
        )

        result = prune_orphan_devices(
            self.sync,
            report=self._report([self.devices[0]]),
        )

        self.assertEqual(self.EXPECTED_KEYS - set(result), set())


class QuarantineDoesNotReplaceTheShrinkGuardTest(AbsenceQuarantineTestBase):
    """The two guards compose, and nothing else pins that they do.

    Each file that tests one guard switches the other off, for good reasons
    both times - one guard masking another proves nothing, and every case in
    `test_scope_shrink_guard` would otherwise be held for want of an absence
    row and assert zero pruned devices without reaching the guard at all. But
    between them that leaves the composition untested, and the design's claim
    that the quarantine is "an additional gate, not a replacement" resting on
    reading order alone. A collapsed scope must still RAISE, not return a quiet
    held-count that an operator would read as the quarantine doing its job.
    """

    # `_require_survivable_scope_shrink` only consults the ratio once the orphan
    # count is past SCOPE_SHRINK_REFUSAL_FLOOR (25), so 30 orphans of 40 claimed
    # is the smallest honest shape: 75%, far past the 25% refusal ratio.
    ORPHAN_COUNT = 30
    PREVIOUSLY_MANAGED = 40

    def _collapsed_scope(self):
        template = self.devices[0]
        devices = [
            Device.objects.create(
                name=f"collapsed-device-{index}",
                site=template.site,
                device_type=template.device_type,
                role=template.role,
            )
            for index in range(self.ORPHAN_COUNT)
        ]
        report = self._report(devices)
        report["forward_previously_managed"] = self.PREVIOUSLY_MANAGED
        return devices, report

    def _assert_all_survived(self, devices):
        self.assertEqual(
            Device.objects.filter(pk__in=[device.pk for device in devices]).count(),
            self.ORPHAN_COUNT,
        )

    def test_a_collapsed_scope_raises_even_though_the_quarantine_would_hold_it(self):
        # No absence rows at all, so the quarantine on its own would hold every
        # one of these and return zero pruned without complaint. Only the shrink
        # guard running FIRST turns a collapsed scope into a refusal an operator
        # actually sees. Move the partition above the guard and this is the test
        # that notices.
        devices, report = self._collapsed_scope()

        with self.assertRaises(ScopeShrinkGuardError):
            prune_orphan_devices(self.sync, report=report)

        self._assert_all_survived(devices)

    def test_a_served_quarantine_does_not_buy_past_the_shrink_guard(self):
        # The mirror image: every device has served the quarantine in full.
        # Waiting out the delay is not evidence that the scope result is sound,
        # so the guard must still refuse.
        devices, report = self._collapsed_scope()
        for device in devices:
            self._set_absent(
                device,
                runs=DEFAULT_PRUNE_ABSENCE_RUNS,
                hours_ago=DEFAULT_PRUNE_ABSENCE_HOURS + 1,
            )

        with self.assertRaises(ScopeShrinkGuardError):
            prune_orphan_devices(self.sync, report=report)

        self._assert_all_survived(devices)

    def test_the_shrink_override_does_not_also_release_the_quarantine(self):
        # Two overrides, two decisions. An operator confirming that a large
        # removal is genuine has not also confirmed that each absence has
        # persisted - so overriding the guard must leave the quarantine standing
        # rather than silently granting both.
        devices, report = self._collapsed_scope()

        result = prune_orphan_devices(
            self.sync,
            report=report,
            allow_scope_shrink=True,
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["quarantine_held_device_count"], self.ORPHAN_COUNT)
        self._assert_all_survived(devices)


class AbsenceRowDoesNotPinItsDeviceTest(AbsenceQuarantineTestBase):
    def test_deleting_a_device_does_not_raise_protected_error(self):
        """Bookkeeping must never hold hostage the object it describes.

        A hidden PROTECT relation is what made ingestions permanently
        undeletable once already, and that one was invisible until a customer
        hit it.
        """
        self._set_absent(self.devices[0], runs=1, hours_ago=1)

        self.devices[0].delete()

        self.assertFalse(ForwardDeviceAbsence.objects.filter(sync=self.sync).exists())
