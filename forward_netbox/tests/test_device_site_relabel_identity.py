"""A device whose SITE changed in Forward is the same device, not a new one.

Both apply paths matched existing devices by `(name, site)` only. When Forward
relabels a location - the customer's actual estate went from
`cdl0_dc00-roseland nj (njrsl)` to `dc00-cdl-roseland nj` - the row for that
device now carries the SAME name at a DIFFERENT site, the lookup misses, and a
second device is created. The old copy is stranded: it still holds the
device's primary IP, which the new copy then cannot acquire
("carry no identity from this sync, so the pointer cannot be released"), and
`scope_reconciliation` (keyed on bare name) never notices it went stale
because a device with that name still exists somewhere.

This is deliberately NOT a guess: the fallback only moves a device when it can
PROVE which one Forward means, via `ForwardDeviceIdentity` (this sync's own
name -> device binding) or, absent one, a single unbound same-named device at
another site. Anything else is held, never created, never silently chosen.
"""

from unittest.mock import Mock

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.test import TestCase

from forward_netbox.exceptions import ForwardSearchError
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_device import apply_dcim_device


class DeviceSiteRelabelIdentityTest(TestCase):
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
            name="relabel-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="relabel-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.other_source = ForwardSource.objects.create(
            name="other-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-2"},
        )
        self.other_sync = ForwardSync.objects.create(
            name="other-sync",
            source=self.other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _existing(self, name, site):
        return Device.objects.create(
            name=name,
            site=site,
            role=self.role,
            device_type=self.device_type,
            status="active",
        )

    def _runner(self, sync=None):
        return ForwardSyncRunner(
            sync=sync or self.sync, ingestion=None, client=None, logger_=Mock()
        )

    def _row(self, name, site):
        return {
            "name": name,
            "site": site.name,
            "site_slug": site.slug,
            "role": "role-a",
            "role_slug": "role-a",
            "role_color": "9e9e9e",
            "manufacturer": "Cisco",
            "manufacturer_slug": "cisco",
            "device_type": "model-a",
            "device_type_slug": "model-a",
            "platform": "",
            "platform_slug": "",
            "status": "active",
            "serial": "",
        }

    # -- unbound, unambiguous: the ordinary customer case ---------------

    def test_bulk_path_moves_the_single_unbound_same_name_device(self):
        old = self._existing("core-sw-01", self.old_site)
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("core-sw-01", self.new_site)])

        runner._record_issue.assert_not_called()
        self.assertEqual(Device.objects.count(), 1)
        old.refresh_from_db()
        self.assertEqual(old.pk, old.pk)  # same row, not a new one
        self.assertEqual(old.site_id, self.new_site.pk)

    def test_row_path_moves_the_single_unbound_same_name_device(self):
        old = self._existing("core-sw-01", self.old_site)

        apply_dcim_device(self._runner(), self._row("core-sw-01", self.new_site))

        self.assertEqual(Device.objects.count(), 1)
        old.refresh_from_db()
        self.assertEqual(old.site_id, self.new_site.pk)

    # -- bound: the identity binding wins even over an unbound sibling --

    def test_the_bound_identity_is_moved_even_with_another_candidate_present(self):
        bound = self._existing("core-sw-01", self.old_site)
        decoy = self._existing("core-sw-01", Site.objects.create(name="s3", slug="s3"))
        ForwardDeviceIdentity.objects.create(
            sync=self.sync, source_device_key="core-sw-01", device=bound
        )
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("core-sw-01", self.new_site)])

        runner._record_issue.assert_not_called()
        bound.refresh_from_db()
        decoy.refresh_from_db()
        self.assertEqual(bound.site_id, self.new_site.pk)
        self.assertEqual(decoy.site_id, Site.objects.get(slug="s3").pk)

    # -- negative space: never guess -------------------------------------

    def test_two_unbound_candidates_are_held_not_created_or_moved(self):
        a = self._existing("core-sw-01", self.old_site)
        b = self._existing("core-sw-01", Site.objects.create(name="s3", slug="s3"))
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("core-sw-01", self.new_site)])

        runner._record_issue.assert_called_once()
        self.assertEqual(Device.objects.count(), 2)
        a.refresh_from_db()
        b.refresh_from_db()
        self.assertEqual(a.site_id, self.old_site.pk)
        self.assertEqual(
            {a.site_id, b.site_id}, {self.old_site.pk, Site.objects.get(slug="s3").pk}
        )

    def test_a_device_bound_to_another_sync_is_untouched(self):
        other = self._existing("core-sw-01", self.old_site)
        ForwardDeviceIdentity.objects.create(
            sync=self.other_sync, source_device_key="core-sw-01", device=other
        )
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("core-sw-01", self.new_site)])

        runner._record_issue.assert_called_once()
        self.assertEqual(Device.objects.count(), 1)
        other.refresh_from_db()
        self.assertEqual(other.site_id, self.old_site.pk)

    def test_row_path_raises_rather_than_guess_on_two_candidates(self):
        self._existing("core-sw-01", self.old_site)
        self._existing("core-sw-01", Site.objects.create(name="s3", slug="s3"))

        with self.assertRaises(ForwardSearchError):
            apply_dcim_device(self._runner(), self._row("core-sw-01", self.new_site))

    # -- the primary IP consequence, made concrete -----------------------

    def test_the_primary_ip_moves_with_the_device_no_release_needed(self):
        from dcim.models import Interface
        from ipam.models import IPAddress

        old = self._existing("core-sw-01", self.old_site)
        interface = Interface.objects.create(
            device=old, name="mgmt0", type="1000base-t"
        )
        addr = IPAddress.objects.create(address="10.0.0.1/32")
        addr.assigned_object = interface
        addr.save()
        old.primary_ip4 = addr
        old.save()
        runner = self._runner()
        runner._record_issue = Mock()

        bulk_orm_apply_device(runner, [self._row("core-sw-01", self.new_site)])

        runner._record_issue.assert_not_called()
        self.assertEqual(Device.objects.count(), 1)
        old.refresh_from_db()
        self.assertEqual(old.site_id, self.new_site.pk)
        self.assertEqual(old.primary_ip4_id, addr.pk)

    # -- an unrelated new device is still created normally ---------------

    def test_an_unrelated_new_device_at_a_new_site_is_still_created(self):
        bulk_orm_apply_device(
            self._runner(), [self._row("brand-new-01", self.new_site)]
        )

        self.assertEqual(
            list(Device.objects.values_list("name", "site_id")),
            [("brand-new-01", self.new_site.pk)],
        )

    # -- the dependency preview's stand-in sync (no real pk) ---------------

    def test_the_bulk_path_survives_a_runner_with_no_real_sync(self):
        # `PreviewRunner`'s `_NullSync` stand-in has `pk = None` and is not a
        # `ForwardSync` instance - filtering a ForeignKey against it directly
        # (rather than its pk) raised `TypeError: Field 'id' expected a
        # number` and took out the WHOLE dependency preview for every model,
        # not just devices. Empty rows is the exact shape the priming-
        # contract test offers every model.
        from forward_netbox.utilities.drift_comparison import PreviewRunner

        bulk_orm_apply_device(PreviewRunner(), [])

    def test_the_row_path_helper_survives_a_runner_with_no_real_sync(self):
        # `apply_dcim_device` itself is never called with `PreviewRunner` in
        # production (only the bulk path is part of the preview's priming
        # contract) - this pins the helper it shares the vulnerability
        # with directly: a `.sync` with `pk = None` must read as unbound,
        # never raise.
        from forward_netbox.utilities.drift_comparison import _NullSync
        from forward_netbox.utilities.sync_device import _relabel_move_device

        old = self._existing("core-sw-01", self.old_site)
        runner = Mock()
        runner.sync = _NullSync()

        candidate = _relabel_move_device(runner, "core-sw-01", self.new_site)

        self.assertEqual(candidate.pk, old.pk)
