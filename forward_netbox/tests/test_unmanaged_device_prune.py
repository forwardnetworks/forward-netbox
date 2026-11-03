# Deleting devices no sync is bound to that Forward does not report at all.
# These are devices that predate the sync or lost their binding, so nothing can
# quarantine or prune them unattended; the operator names them after seeing the
# list. As with the other prunes, what it must REFUSE is pinned harder than what
# it deletes.
from unittest.mock import patch

from dcim.models import Device

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_uncovered_device_cleanup import _Fixture
from forward_netbox.utilities.scope_reconciliation import EmptyForwardScopeError
from forward_netbox.utilities.scope_reconciliation import prune_unmanaged_devices
from forward_netbox.utilities.scope_reconciliation import ScopeCensusUnavailableError
from forward_netbox.utilities.scope_reconciliation import UNMANAGED_PRUNE_MAX_DEVICES

IN_SCOPE = [{"name": "in-scope", "completed": True}]
CENSUS = [{"name": "in-scope", "vendor": "CISCO"}]


class UnmanagedPruneTest(_Fixture):
    def _prune(self, devices, *, census=None, classic=None, **kwargs):
        # `classic` is Forward's configured device list {name: collect}; an
        # Exception instance makes the read fail.
        read = (
            {"side_effect": classic}
            if isinstance(classic, Exception)
            else {"return_value": {} if classic is None else classic}
        )
        with patch(
            "forward_netbox.utilities.scope_reconciliation.get_classic_device_collection",
            **read,
        ):
            report = self._report(IN_SCOPE, CENSUS if census is None else census)
            return prune_unmanaged_devices(
                self.sync,
                restrict_to_device_pks=[device.pk for device in devices],
                report=report,
                **kwargs,
            )

    def test_an_unbound_device_forward_does_not_report_is_deleted(self):
        self._device("in-scope")
        gone = self._device("gone")

        result = self._prune([gone])

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertFalse(Device.objects.filter(pk=gone.pk).exists())

    def test_only_the_named_devices_are_deleted(self):
        self._device("in-scope")
        named = self._device("named")
        unnamed = self._device("unnamed")

        self._prune([named])

        self.assertFalse(Device.objects.filter(pk=named.pk).exists())
        self.assertTrue(Device.objects.filter(pk=unnamed.pk).exists())

    def test_nothing_named_deletes_nothing(self):
        self._device("in-scope")
        gone = self._device("gone")

        result = self._prune([])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_a_device_forward_still_reports_is_refused(self):
        self._device("in-scope")
        present = self._device("present")

        result = self._prune(
            [present],
            census=CENSUS + [{"name": "present", "vendor": "CISCO"}],
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["not_absent"], [present.pk])
        self.assertTrue(Device.objects.filter(pk=present.pk).exists())

    def test_a_vendor_excluded_device_is_refused(self):
        self._device("in-scope")
        custom = self._device("custom")

        result = self._prune(
            [custom],
            census=CENSUS + [{"name": "custom", "vendor": "FORWARD_CUSTOM"}],
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=custom.pk).exists())

    def test_a_device_this_sync_owns_is_refused(self):
        # Owned devices keep their own quarantined prune.
        self._device("in-scope")
        owned = self._own(self._device("owned"))

        result = self._prune([owned])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["not_unclaimed"], [owned.pk])
        self.assertTrue(Device.objects.filter(pk=owned.pk).exists())

    def test_a_device_another_sync_manages_is_refused(self):
        from forward_netbox.models import ForwardDeviceIdentity

        self._device("in-scope")
        other_sync = ForwardSync.objects.create(
            name="other-sync", source=self.source, parameters={}
        )
        other_ingestion = ForwardIngestion.objects.create(
            sync=other_sync, snapshot_id="snap-1", baseline_ready=True
        )
        elsewhere = self._device("elsewhere")
        ForwardDeviceIdentity.objects.create(
            sync=other_sync,
            source_device_key=elsewhere.name,
            device=elsewhere,
            ingestion_id=other_ingestion.pk,
            snapshot_id="snap-1",
        )

        result = self._prune([elsewhere])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["bound_to_a_sync"], [elsewhere.pk])
        self.assertTrue(Device.objects.filter(pk=elsewhere.pk).exists())

    def test_an_unknown_pk_is_refused_not_widened(self):
        self._device("in-scope")
        gone = self._device("gone")
        report = self._report(IN_SCOPE, CENSUS)

        result = prune_unmanaged_devices(
            self.sync, restrict_to_device_pks=[gone.pk + 9999], report=report
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_the_cap_bounds_one_run_and_names_the_rest(self):
        self._device("in-scope")
        devices = [self._device(f"gone-{index}") for index in range(5)]

        result = self._prune(devices, max_devices=3)

        self.assertEqual(result["pruned_device_count"], 3)
        self.assertEqual(
            result["refusals"]["over_cap"], sorted(device.pk for device in devices)[3:]
        )
        self.assertEqual(Device.objects.filter(name__startswith="gone-").count(), 2)

    def test_the_default_cap_is_modest(self):
        self.assertLessEqual(UNMANAGED_PRUNE_MAX_DEVICES, 100)

    def test_an_empty_forward_scope_refuses(self):
        gone = self._device("gone")
        report = self._report([], [])

        with self.assertRaises(EmptyForwardScopeError):
            prune_unmanaged_devices(
                self.sync, restrict_to_device_pks=[gone.pk], report=report
            )
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_without_a_census_it_refuses_rather_than_assuming_absence(self):
        self._device("in-scope")
        gone = self._device("gone")
        report = self._report(IN_SCOPE, CENSUS)
        report["_absence_kinds"] = None

        with self.assertRaises(ScopeCensusUnavailableError):
            prune_unmanaged_devices(
                self.sync, restrict_to_device_pks=[gone.pk], report=report
            )
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())


class UnmanagedPruneSafetyTest(_Fixture):
    """The refusals a reviewer found missing: each could delete a live device."""

    def _prune(self, devices, *, census=None, classic=None, **kwargs):
        # `classic` is Forward's configured device list {name: collect}; an
        # Exception instance makes the read fail.
        read = (
            {"side_effect": classic}
            if isinstance(classic, Exception)
            else {"return_value": {} if classic is None else classic}
        )
        with patch(
            "forward_netbox.utilities.scope_reconciliation.get_classic_device_collection",
            **read,
        ):
            report = self._report(IN_SCOPE, CENSUS if census is None else census)
            return prune_unmanaged_devices(
                self.sync,
                restrict_to_device_pks=[device.pk for device in devices],
                report=report,
                **kwargs,
            )

    def test_a_case_only_name_difference_reads_as_present_not_gone(self):
        self._device("in-scope")
        mixed = self._device("core-rtr1")

        result = self._prune(
            [mixed],
            census=CENSUS + [{"name": "CORE-RTR1", "vendor": "CISCO"}],
        )

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=mixed.pk).exists())

    def test_a_scope_tag_claim_means_it_is_in_a_quarantine_and_is_refused(self):
        from forward_netbox.models import ForwardDeviceTagClaim

        self._device("in-scope")
        claimed = self._device("claimed")
        ForwardDeviceTagClaim.objects.create(
            sync=self.sync, device=claimed, tag=self._tag("t1")
        )

        result = self._prune([claimed])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["bound_to_a_sync"], [claimed.pk])
        self.assertTrue(Device.objects.filter(pk=claimed.pk).exists())

    def test_an_absence_row_means_it_is_in_a_quarantine_and_is_refused(self):
        self._device("in-scope")
        held = self._device("held")
        self._served_quarantine(held)

        result = self._prune([held])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["bound_to_a_sync"], [held.pk])
        self.assertTrue(Device.objects.filter(pk=held.pk).exists())

    def test_another_forward_network_refuses_the_whole_action(self):
        self._device("in-scope")
        gone = self._device("gone")
        other_source = ForwardSource.objects.create(
            name="other-net-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-2"},
        )
        ForwardSync.objects.create(name="other-net", source=other_source, parameters={})

        with self.assertRaises(ScopeCensusUnavailableError):
            self._prune([gone])
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_a_second_sync_on_the_same_network_does_not_refuse(self):
        self._device("in-scope")
        gone = self._device("gone")
        ForwardSync.objects.create(name="same-net", source=self.source, parameters={})

        result = self._prune([gone])

        self.assertEqual(result["pruned_device_count"], 1)

    def _tag(self, slug):
        from extras.models import Tag

        return Tag.objects.create(name=slug, slug=slug)

    def test_a_device_configured_in_forward_but_disabled_is_refused(self):
        # Disabled in Forward vanishes from the snapshot exactly like a removed
        # device, and an unmanaged one rarely carries an include tag, so the
        # tag-scoped evidence never sees it.
        self._device("in-scope")
        disabled = self._device("core-fw9")

        result = self._prune([disabled], classic={"CORE-FW9": False})

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["refusals"]["not_absent"], [disabled.pk])
        self.assertTrue(Device.objects.filter(pk=disabled.pk).exists())

    def test_when_forwards_device_configuration_cannot_be_read_nothing_is_deleted(self):
        self._device("in-scope")
        gone = self._device("gone")

        result = self._prune([gone], classic=RuntimeError("boom"))

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())

    def test_a_pinned_snapshot_refuses(self):
        self._device("in-scope")
        gone = self._device("gone")
        self.sync.parameters = {"snapshot_id": "pinned"}
        self.sync.save()

        with self.assertRaises(ScopeCensusUnavailableError):
            self._prune([gone])
        self.assertTrue(Device.objects.filter(pk=gone.pk).exists())
