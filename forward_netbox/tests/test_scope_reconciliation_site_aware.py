"""Scope membership is site-aware, not just name-aware.

Both apply paths now move a device instead of duplicating it when Forward
relabels its site (`apply_engine_bulk.py`'s `_relabel_move_candidate`,
`sync_device.py`'s `_relabel_move_device`), but `compute_scope_reconciliation`
still tested scope membership by bare device name - which cannot tell a
device at Forward's current site apart from one stranded at a stale site
after a relabel: both carry the same name, so both used to read as "in
scope". That is the customer-diagnosed mechanism behind 243 of 248 duplicate
pairs being invisible to every report, orphan prune and uncovered cleanup.

These tests pin the fix directly against `compute_scope_reconciliation`,
following the pattern in `test_device_scope_reconciliation_audit_command.py`:
a mocked Forward client, a real call, real NetBox devices.
"""

from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation


class ScopeReconciliationSiteAwareTest(TestCase):
    def setUp(self):
        self.old_site = Site.objects.create(name="old-site", slug="old-site")
        self.new_site = Site.objects.create(name="new-site", slug="new-site")
        mfr = Manufacturer.objects.create(name="MfrS", slug="mfr-s")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-s", slug="dt-s")
        self.role = DeviceRole.objects.create(name="RoleS", slug="role-s")
        self.source = ForwardSource.objects.create(
            name="site-aware-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
                "device_tag_prune_absence_runs": 0,
                "device_tag_prune_absence_hours": 0,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="site-aware-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.tag = Tag.objects.create(name="Prod_Core", slug="prod-core")

    def _device(self, name, site):
        return Device.objects.create(
            name=name, site=site, role=self.role, device_type=self.dt
        )

    def _claim(self, device):
        return ForwardDeviceTagClaim.objects.create(
            sync=self.sync, device=device, tag=self.tag, claim_type="scope"
        )

    def _report(self, rows):
        client = Mock()
        client.run_nqe_query = Mock(return_value=rows)
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            return compute_scope_reconciliation(self.sync)

    def _row(self, name, site_slug, **extra):
        return {"name": name, "completed": True, "location": site_slug, **extra}

    # -- the customer's exact shape ---------------------------------------

    def test_the_stale_site_device_is_out_of_scope_and_the_live_one_is_not(self):
        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(old)
        # Forward's site relabel is already reflected here: the row reports
        # the SAME name, now at the new site.
        report = self._report([self._row("core-sw-01", "new-site")])

        self.assertEqual(list(report["_out_of_scope_pks"]), [old.pk])
        self.assertNotIn(new.pk, report["_out_of_scope_pks"])

    def test_the_live_site_device_is_out_of_scope_when_it_is_the_claimed_one(self):
        # Symmetric check: claiming the NEW device instead makes IT the one
        # tested, and it is correctly in scope (matches Forward's site).
        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(new)
        report = self._report([self._row("core-sw-01", "new-site")])

        self.assertEqual(report["_out_of_scope_pks"], [])
        self.assertNotIn(old.pk, report["_out_of_scope_pks"])

    def test_owned_untagged_is_also_site_aware(self):
        # `_unmanaged_device_summary` (the "Forward Uncovered" source) had
        # the identical bug: the stale device's name reads as tagged since
        # Forward still reports it, just at a different site.
        old = self._device("core-sw-01", self.old_site)
        self._device("core-sw-01", self.new_site)
        identity = ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=old
        )
        # `_unmanaged_device_summary` reads identity by whichever device it
        # points at; point it at the stale (old) device to test that path.
        self.assertEqual(identity.device_id, old.pk)
        report = self._report([self._row("core-sw-01", "new-site")])

        self.assertIn(old.pk, report["_owned_untagged_pks"])

    def test_a_device_at_the_site_forward_reports_is_never_owned_untagged(self):
        self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=new
        )
        report = self._report([self._row("core-sw-01", "new-site")])

        self.assertNotIn(new.pk, report["_owned_untagged_pks"])

    def test_pruning_orphans_only_deletes_the_stale_site_device(self):
        # End to end: the prune-eligibility re-derivation (kinds.get(name)
        # == "absent") tests by NAME after the pk-set is established, so
        # this pins that it never re-widens back to the live device sharing
        # that name and deletes it too.
        from forward_netbox.utilities.scope_reconciliation import (
            prune_orphan_devices,
        )

        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(old)
        client = Mock()
        client.run_nqe_query = Mock(return_value=[self._row("core-sw-01", "new-site")])
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch(
                "forward_netbox.utilities.scope_reconciliation._absence_census",
                return_value=({"core-sw-01": "absent"}, {}),
            ),
        ):
            report = compute_scope_reconciliation(self.sync)
            prune_orphan_devices(self.sync, report=report)

        self.assertFalse(Device.objects.filter(pk=old.pk).exists())
        self.assertTrue(Device.objects.filter(pk=new.pk).exists())

    def test_an_unmerged_site_relabel_pair_is_excluded_from_prune_entirely(self):
        # Once identity proves it (the shape `merge_site_relabel_duplicates`
        # would act on), the stale device is a candidate for THAT action,
        # not this prune - it must survive untouched until the operator
        # chooses to merge it.
        from forward_netbox.utilities.scope_reconciliation import (
            prune_orphan_devices,
        )

        old = self._device("core-sw-01", self.old_site)
        new = self._device("core-sw-01", self.new_site)
        self._claim(old)
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=new
        )
        client = Mock()
        client.run_nqe_query = Mock(return_value=[self._row("core-sw-01", "new-site")])
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch(
                "forward_netbox.utilities.scope_reconciliation._absence_census",
                return_value=({"core-sw-01": "absent"}, {}),
            ),
        ):
            report = compute_scope_reconciliation(self.sync)
            prune_orphan_devices(self.sync, report=report)

        self.assertTrue(Device.objects.filter(pk=old.pk).exists())
        self.assertTrue(Device.objects.filter(pk=new.pk).exists())

    # -- the endpoint fallback: unchanged, no site info available ----------

    def test_endpoint_derived_names_keep_the_name_only_fallback(self):
        # No `location` for an endpoint-derived name - the site-aware check
        # must not demand one it cannot have.
        self.source.parameters = {
            **self.source.parameters,
            "sync_endpoints": True,
        }
        self.source.save(update_fields=["parameters"])
        device = self._device("endpoint-a", self.old_site)
        self._claim(device)
        client = Mock()
        client.run_nqe_query.side_effect = [
            [],
            [{"name": "endpoint-a"}],
        ]
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            report = compute_scope_reconciliation(self.sync)

        self.assertEqual(report["_out_of_scope_pks"], [])
