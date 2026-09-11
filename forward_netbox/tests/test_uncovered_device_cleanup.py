# Deleting uncovered devices. The orphan prune acts on devices claimed by the
# run that produced the current result; a device created by an earlier run and
# dropped from scope since is not in that set. At a customer whose orphan count
# reads zero that made the prune a no-op while the uncovered count climbed, so
# the bucket was diagnosable and not actionable.
#
# Most of what is below is the negative space. A deleting path in this plugin
# has removed live customer devices before, so what it must REFUSE to touch is
# pinned harder than what it deletes.
from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from django.utils import timezone

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardDeviceAbsence
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import EmptyForwardScopeError
from forward_netbox.utilities.scope_reconciliation import prune_uncovered_devices
from forward_netbox.utilities.scope_reconciliation import record_device_absence
from forward_netbox.utilities.scope_reconciliation import ScopeCensusUnavailableError
from forward_netbox.utilities.scope_reconciliation import ScopeShrinkGuardError


class _Fixture(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="cleanup-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="cleanup-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1", baseline_ready=True
        )
        mfr = Manufacturer.objects.create(name="MfrC", slug="mfr-c")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-c", slug="dt-c")
        self.role = DeviceRole.objects.create(name="RoleC", slug="role-c")
        self.site = Site.objects.create(name="SiteC", slug="site-c")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _own(self, device):
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key=device.name,
            device=device,
            ingestion_id=self.ingestion.pk,
            snapshot_id="snap-1",
        )
        return device

    def _report(self, scope_rows, census_rows=None):
        client = Mock()
        responses = [scope_rows]
        if census_rows is not None:
            responses.append(census_rows)
        client.run_nqe_query = Mock(side_effect=responses)
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
        ):
            return compute_scope_reconciliation(self.sync)

    def _served_quarantine(self, device, *, runs=25):
        """Put a device far enough past the quarantine to be eligible."""
        long_ago = timezone.now() - timedelta(days=30)
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=device,
            consecutive_absent_runs=runs,
            first_absent_at=long_ago,
            last_absent_at=timezone.now(),
            last_absent_snapshot_id="snap-1",
        )


class AbsenceIsTrackedForUncoveredDevicesTest(_Fixture):
    """The enabling fix. Streaks were kept for orphans only.

    `partition_quarantined_orphans` fails closed, so a device with no absence
    row is held. An uncovered device never had a row, which meant it could
    never leave quarantine however long it had been gone - the cleanup would
    have been a permanent no-op without this.
    """

    def test_an_uncovered_device_accumulates_a_streak(self):
        gone = self._own(self._device("gone"))

        record_device_absence(self.sync, (), uncovered_pks=[gone.pk])
        record_device_absence(self.sync, (), uncovered_pks=[gone.pk])

        row = ForwardDeviceAbsence.objects.get(sync=self.sync, device=gone)
        self.assertEqual(row.consecutive_absent_runs, 2)

    def test_a_device_that_comes_back_loses_its_streak(self):
        gone = self._own(self._device("gone"))
        record_device_absence(self.sync, (), uncovered_pks=[gone.pk])
        record_device_absence(self.sync, ())

        self.assertFalse(
            ForwardDeviceAbsence.objects.filter(sync=self.sync, device=gone).exists()
        )

    def test_orphans_are_still_tracked_alongside(self):
        orphan = self._own(self._device("orphan"))
        uncovered = self._own(self._device("uncovered"))

        record_device_absence(self.sync, [orphan.pk], uncovered_pks=[uncovered.pk])

        self.assertEqual(ForwardDeviceAbsence.objects.filter(sync=self.sync).count(), 2)


class OnlyAbsentDevicesAreDeletedTest(_Fixture):
    """The allowlist. Absent from Forward, owned by this sync, past quarantine."""

    def test_an_absent_owned_device_past_quarantine_is_deleted(self):
        self._device("in-scope")
        gone = self._own(self._device("gone"))
        self._served_quarantine(gone)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertFalse(Device.objects.filter(pk=gone.pk).exists())

    def test_a_device_forward_still_reports_is_never_deleted(self):
        # Present in Forward, carrying no include tag. That is a scoping
        # decision, and deleting it would destroy a device that exists.
        self._device("in-scope")
        untagged = self._own(self._device("untagged"))
        self._served_quarantine(untagged)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [
                {"name": "in-scope", "vendor": "CISCO"},
                {"name": "untagged", "vendor": "CISCO"},
            ],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=untagged.pk).exists())

    def test_a_vendor_excluded_device_is_never_deleted(self):
        self._device("in-scope")
        custom = self._own(self._device("custom"))
        self._served_quarantine(custom)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [
                {"name": "in-scope", "vendor": "CISCO"},
                {"name": "custom", "vendor": "FORWARD_CUSTOM"},
            ],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=custom.pk).exists())

    def test_a_device_this_sync_never_created_is_never_deleted(self):
        # The unclaimed half of "untagged". Another source made it, or a person
        # did. There is no code path to it here at all.
        self._device("in-scope")
        stranger = self._device("stranger")
        self._served_quarantine(stranger)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=stranger.pk).exists())


class QuarantineHoldsUnprovenAbsenceTest(_Fixture):
    """Disabling a device in Forward is indistinguishable from removing it.

    Confirmed against a live customer snapshot, from both the NQE result and
    the REST inventory. Sustained absence is the only thing that tells them
    apart, so an absence that has not lasted is not evidence.
    """

    def test_a_recently_absent_device_is_held_not_deleted(self):
        self._device("in-scope")
        gone = self._own(self._device("gone"))
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=gone,
            consecutive_absent_runs=1,
            first_absent_at=timezone.now(),
            last_absent_at=timezone.now(),
            last_absent_snapshot_id="snap-1",
        )

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["quarantine_held_device_count"], 1)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_a_device_with_no_absence_row_is_held(self):
        # Fails closed: never seen absent is not the same as absent for long.
        self._device("in-scope")
        gone = self._own(self._device("gone"))

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_the_override_deletes_what_the_quarantine_held(self):
        # For a person looking at a named list and choosing. Not for a job.
        self._device("in-scope")
        gone = self._own(self._device("gone"))

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(
            self.sync, report=report, include_quarantined=True
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["quarantine_overridden_device_count"], 1)
        self.assertFalse(Device.objects.filter(pk=gone.pk).exists())


class RefusalsTest(_Fixture):
    """Gates this cleanup does not get to skip."""

    def test_an_empty_forward_result_refuses(self):
        self._own(self._device("gone"))
        report = self._report([], [])

        with self.assertRaises(EmptyForwardScopeError):
            prune_uncovered_devices(self.sync, report=report)

    def test_a_failed_census_refuses_rather_than_assuming(self):
        # A census failure and a genuinely empty absent set read the same from
        # the counts, and only one of them makes deleting safe.
        self._device("in-scope")
        gone = self._own(self._device("gone"))
        self._served_quarantine(gone)

        client = Mock()
        client.run_nqe_query = Mock(
            side_effect=[[{"name": "in-scope", "completed": True}], Exception("boom")]
        )
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
        ):
            report = compute_scope_reconciliation(self.sync)

        with self.assertRaises(ScopeCensusUnavailableError):
            prune_uncovered_devices(self.sync, report=report)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_deleting_most_of_what_this_sync_created_refuses(self):
        # The orphan guard cannot cover this: it measures orphans, which are
        # zero in exactly the shape this cleanup exists for.
        self._device("in-scope")
        for index in range(30):
            device = self._own(self._device(f"gone-{index}"))
            self._served_quarantine(device)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        with self.assertRaises(ScopeShrinkGuardError):
            prune_uncovered_devices(self.sync, report=report)
        self.assertEqual(Device.objects.filter(name__startswith="gone-").count(), 30)

    def test_the_override_allows_a_deliberate_large_removal(self):
        self._device("in-scope")
        for index in range(30):
            device = self._own(self._device(f"gone-{index}"))
            self._served_quarantine(device)

        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(
            self.sync, report=report, allow_scope_shrink=True
        )
        self.assertEqual(result["pruned_device_count"], 30)

    def test_nothing_uncovered_is_a_no_op_not_an_error(self):
        self._own(self._device("in-scope"))
        report = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "CISCO"}],
        )
        result = prune_uncovered_devices(self.sync, report=report)
        self.assertEqual(result["pruned_device_count"], 0)


class RestrictedPruneAllowlistTest(_Fixture):
    """A named device can only ever be a SUBSET of what the prune would take.

    The device page narrows this prune to one device rather than running its
    own deletion, because a second deletion path is how a delete acquires a
    guard the other one lacks. These pin the negative space: every way a
    specifically-requested device must be refused, and that the request can
    never widen the set.
    """

    def _absent_report(self, *devices):
        """A report where each named device is owned, uncovered and absent."""
        for device in devices:
            self._own(device)
        return self._report(
            [{"name": "in-scope", "completed": True, "tagNames": ["Prod_Core"]}],
            census_rows=[{"name": "in-scope", "vendor": "Vendor.CISCO"}],
        )

    def test_a_device_this_sync_does_not_own_is_refused_and_named(self):
        mine = self._device("mine-gone")
        theirs = self._device("not-mine")
        report = self._absent_report(mine)
        self._served_quarantine(mine)

        result = prune_uncovered_devices(
            self.sync, report=report, restrict_to_device_pks=[theirs.pk]
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["restricted_refusals"]["not_owned"], [theirs.pk])
        self.assertTrue(Device.objects.filter(pk=theirs.pk).exists())
        # And the device that WAS eligible is untouched, because it was not asked for.
        self.assertTrue(Device.objects.filter(pk=mine.pk).exists())

    def test_a_device_forward_still_reports_is_refused(self):
        # Uncovered but present in the census: a scoping decision, never a delete.
        device = self._own(self._device("still-there"))
        report = self._report(
            [{"name": "in-scope", "completed": True, "tagNames": ["Prod_Core"]}],
            census_rows=[
                {"name": "in-scope", "vendor": "Vendor.CISCO"},
                {"name": "still-there", "vendor": "Vendor.CISCO"},
            ],
        )
        self._served_quarantine(device)

        result = prune_uncovered_devices(
            self.sync, report=report, restrict_to_device_pks=[device.pk]
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["restricted_refusals"]["not_absent"], [device.pk])
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())

    def test_a_held_device_is_refused_without_the_override(self):
        device = self._own(self._device("held-gone"))
        report = self._absent_report()

        result = prune_uncovered_devices(
            self.sync, report=report, restrict_to_device_pks=[device.pk]
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["restricted_refusals"]["held"], [device.pk])
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())

    def test_the_override_deletes_only_the_named_held_device(self):
        named = self._device("named-held")
        other = self._device("other-held")
        report = self._absent_report(named, other)

        result = prune_uncovered_devices(
            self.sync,
            report=report,
            restrict_to_device_pks=[named.pk],
            include_quarantined=True,
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertFalse(Device.objects.filter(pk=named.pk).exists())
        # The other held device was equally eligible under the override and is
        # still here, because the restriction is a subset and not a filter the
        # override bypasses.
        self.assertTrue(Device.objects.filter(pk=other.pk).exists())

    def test_the_restriction_never_widens_the_set(self):
        eligible = self._device("eligible-gone")
        unrelated = self._device("unrelated-in-scope")
        report = self._absent_report(eligible)
        self._served_quarantine(eligible)

        result = prune_uncovered_devices(
            self.sync,
            report=report,
            restrict_to_device_pks=[eligible.pk, unrelated.pk],
        )

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertFalse(Device.objects.filter(pk=eligible.pk).exists())
        self.assertTrue(Device.objects.filter(pk=unrelated.pk).exists())
        self.assertEqual(result["restricted_refusals"]["not_owned"], [unrelated.pk])

    def test_an_unrestricted_prune_still_reports_empty_refusals(self):
        # The key is always present and always means the same thing, so a
        # caller reading it cannot get a KeyError depending on the path taken.
        device = self._own(self._device("bulk-gone"))
        report = self._absent_report()
        self._served_quarantine(device)

        result = prune_uncovered_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(
            result["restricted_refusals"],
            {"not_owned": [], "not_absent": [], "held": []},
        )
