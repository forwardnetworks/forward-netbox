"""The GUI repair for existing site-relabel duplicate pairs.

The apply-path fix (`apply_engine_bulk.py`'s `_relabel_move_candidate`) stops
a sync from creating a second device on a site relabel going forward, but the
customer's ~248 pairs already exist. This is the one-time repair: for each
pair proven safe, delete the newer (sync-created) device and move the older
one to its site, so it takes over Forward's identity for that name.

Every test here pins the negative space as hard as the happy path - a pair
this cannot prove safe must be left completely alone, never guessed at.
"""

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from unittest.mock import patch

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_IDENTITY_OLDER,
)
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_IDENTITY_OTHER,
)
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_MANUAL_OBJECTS,
)
from forward_netbox.utilities.scope_reconciliation import (
    SITE_RELABEL_HOLD_NO_IDENTITY,
)
from forward_netbox.utilities.scope_reconciliation import merge_site_relabel_duplicates
from forward_netbox.utilities.scope_reconciliation import site_relabel_pairs


class SiteRelabelPairsTest(TestCase):
    def setUp(self):
        self.old_site = Site.objects.create(name="old-site", slug="old-site")
        self.new_site = Site.objects.create(name="new-site", slug="new-site")
        manufacturer = Manufacturer.objects.create(name="Cisco", slug="cisco")
        self.device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="model-a", slug="model-a"
        )
        self.role = DeviceRole.objects.create(
            name="role-a", slug="role-a", color="9e9e9e"
        )
        source = ForwardSource.objects.create(
            name="relabel-repair-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="relabel-repair-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.other_source = ForwardSource.objects.create(
            name="relabel-repair-other-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-2"},
        )
        self.other_sync = ForwardSync.objects.create(
            name="relabel-repair-other-sync",
            source=self.other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _device(self, name, site):
        return Device.objects.create(
            name=name,
            site=site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _bound_pair(self, name="core-sw-01"):
        older = self._device(name, self.old_site)
        newer = self._device(name, self.new_site)
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key=name, device=newer
        )
        return older, newer

    # -- detection: the happy path -----------------------------------

    def test_a_bound_newer_device_forms_a_mergeable_pair(self):
        older, newer = self._bound_pair()

        report = site_relabel_pairs(self.sync)

        self.assertEqual(len(report["pairs"]), 1)
        self.assertEqual(report["pairs"][0]["older_pk"], older.pk)
        self.assertEqual(report["pairs"][0]["newer_pk"], newer.pk)
        self.assertEqual(report["held"], [])

    def test_a_same_site_duplicate_is_not_a_pair_at_all(self):
        # `dcim_device_unique_name_site` only allows two devices to share a
        # (name, site) when their tenants differ - the actual constraint this
        # whole fix exists because of. Not a relabel: nothing about it
        # changed sites, so it must not surface as a pair or a hold either.
        from tenancy.models import Tenant

        tenant = Tenant.objects.create(name="t", slug="t")
        self._device("core-sw-01", self.old_site)
        Device.objects.create(
            name="core-sw-01",
            site=self.old_site,
            role=self.role,
            device_type=self.device_type,
            status="active",
            tenant=tenant,
        )

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"], [])

    # -- detection: negative space, never guess ------------------------

    def test_no_identity_binding_is_held(self):
        older = self._device("core-sw-01", self.old_site)
        newer = self._device("core-sw-01", self.new_site)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(len(report["held"]), 1)
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_NO_IDENTITY)
        self.assertEqual(
            sorted(report["held"][0]["device_pks"]), sorted([older.pk, newer.pk])
        )

    def test_identity_bound_to_the_older_device_is_held(self):
        older = self._device("core-sw-01", self.old_site)
        self._device("core-sw-01", self.new_site)
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=older
        )

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_IDENTITY_OLDER)

    def test_identity_bound_to_a_third_device_entirely_is_held(self):
        older, newer = self._bound_pair()
        third_site = Site.objects.create(name="s3", slug="s3")
        orphan = self._device("some-other-device", third_site)
        ForwardDeviceIdentity.objects.filter(
            sync=self.sync, source_device_key="core-sw-01"
        ).update(device=orphan)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_IDENTITY_OTHER)

    def test_identity_bound_to_a_different_sync_reads_as_no_binding_here(self):
        older, newer = self._bound_pair()
        # Rebind to the OTHER sync - this sync has no opinion on this name.
        ForwardDeviceIdentity.objects.filter(sync=self.sync).update(
            sync=self.other_sync
        )

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_NO_IDENTITY)

    def test_three_devices_sharing_a_name_are_held_not_guessed(self):
        third_site = Site.objects.create(name="s3", slug="s3")
        a = self._device("core-sw-01", self.old_site)
        b = self._device("core-sw-01", self.new_site)
        c = self._device("core-sw-01", third_site)

        report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(len(report["held"]), 1)
        self.assertEqual(
            sorted(report["held"][0]["device_pks"]), sorted([a.pk, b.pk, c.pk])
        )

    def test_a_real_protecting_reference_on_the_newer_device_holds_the_pair(self):
        # `describe_protecting_references` is the real, schema-driven check;
        # this pins that its result actually gates the pair, without needing
        # a specific PROTECT-backed model wired into the test fixtures. The
        # exclusion of Forward's own provenance FKs (which also report as
        # protecting, for a plain delete) is covered separately by the
        # happy-path test actually forming a pair.
        older, newer = self._bound_pair()

        with patch(
            "forward_netbox.utilities.bulk_merge.describe_protecting_references",
            return_value=[("ipam.Service", 1)],
        ):
            report = site_relabel_pairs(self.sync)

        self.assertEqual(report["pairs"], [])
        self.assertEqual(report["held"][0]["reason"], SITE_RELABEL_HOLD_MANUAL_OBJECTS)
        self.assertEqual(report["held"][0]["blocking_models"], ["ipam.Service"])

    # -- the merge action itself ----------------------------------------

    def test_the_older_device_survives_at_the_new_site_with_its_primary_ip(self):
        from ipam.models import IPAddress

        older, newer = self._bound_pair()
        interface = Interface.objects.create(
            device=older, name="mgmt0", type="1000base-t"
        )
        addr = IPAddress.objects.create(address="10.0.0.1/32")
        addr.assigned_object = interface
        addr.save()
        older.primary_ip4 = addr
        older.save()

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
        older.refresh_from_db()
        self.assertEqual(older.site_id, self.new_site.pk)
        self.assertEqual(older.primary_ip4_id, addr.pk)
        self.assertTrue(
            ForwardDeviceIdentity.objects.filter(
                sync=self.sync, source_device_key="core-sw-01", device=older
            ).exists()
        )

    def test_a_held_pair_is_untouched_by_the_merge(self):
        older = self._device("core-sw-01", self.old_site)
        newer = self._device("core-sw-01", self.new_site)

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 0)
        self.assertEqual(result["held_count"], 1)
        self.assertTrue(Device.objects.filter(pk=older.pk, site=self.old_site).exists())
        self.assertTrue(Device.objects.filter(pk=newer.pk, site=self.new_site).exists())

    def test_an_out_of_list_pk_is_never_touched(self):
        # A pair for a totally unrelated name, never passed to the merge.
        self._bound_pair(name="core-sw-01")
        untouched = self._device("untouched-01", self.old_site)

        merge_site_relabel_duplicates(self.sync)

        untouched.refresh_from_db()
        self.assertEqual(untouched.site_id, self.old_site.pk)

    def test_two_independent_pairs_both_merge(self):
        older_a, newer_a = self._bound_pair(name="core-sw-01")
        older_b, newer_b = self._bound_pair(name="core-sw-02")

        result = merge_site_relabel_duplicates(self.sync)

        self.assertEqual(result["merged_count"], 2)
        for older, newer in ((older_a, newer_a), (older_b, newer_b)):
            self.assertFalse(Device.objects.filter(pk=newer.pk).exists())
            older.refresh_from_db()
            self.assertEqual(older.site_id, self.new_site.pk)
