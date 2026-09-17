# A person deleting a device on main releases the plugin's own ownership
# rows instead of being refused by them; every engine path keeps the PROTECT
# it relies on.
#
# A customer bulk-deleting from the device list hit the raw `ProtectedError`
# NetBox renders for a `PROTECT` foreign key: three of the plugin's own
# record names, no cause, no remedy - the device page's ownership panel
# explains the same refusal, but the list's bulk delete never shows it.
# `PROTECT` is deliberate for the engine (the merge's held-back guard, and a
# diff delete of a device another sync still claims, both rely on it), so
# this teaches `release_on_operator_delete` to tell the two apart: a real
# `HttpRequest` in `netbox.context.current_request`, with no
# `netbox_branching` branch active, is a person on main; anything else - the
# sync, the merge under its `NetBoxFakeRequest`, branch staging - keeps
# PROTECT exactly as before.
import uuid

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.db.models import ProtectedError
from django.test import Client
from django.test import RequestFactory
from django.test import TestCase
from django.urls import reverse
from extras.models import Tag
from netbox.context import current_request
from netbox_branching.contextvars import active_branch
from netbox_branching.models import Branch

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.models import ForwardVirtualParentClaim
from forward_netbox.utilities.bulk_merge import protecting_relations


class OperatorDeleteReleasesOwnershipTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="release-src",
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
            name="release-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.other_sync = ForwardSync.objects.create(
            name="release-sync-other",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-release"
        )
        self.other_ingestion = ForwardIngestion.objects.create(
            sync=self.other_sync, snapshot_id="snap-release-other"
        )
        mfr = Manufacturer.objects.create(name="MfrR", slug="mfr-r")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-r", slug="dt-r")
        self.role = DeviceRole.objects.create(name="RoleR", slug="role-r")
        self.site = Site.objects.create(name="SiteR", slug="site-r")
        self.tag = Tag.objects.create(
            name="Forward Uncovered R", slug="forward-uncovered-r"
        )

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _own_it(self, device, *, sync=None):
        sync = sync or self.sync
        ingestion = self.other_ingestion if sync is self.other_sync else self.ingestion
        ForwardDeviceIdentity.objects.create(
            sync=sync,
            ingestion=ingestion,
            source_device_key=device.name,
            device=device,
        )
        ForwardDeviceTagClaim.objects.create(
            sync=sync,
            ingestion=ingestion,
            device=device,
            tag=self.tag,
            claim_type=ForwardDeviceTagClaim.ClaimType.UNCOVERED,
        )

    def _as_operator(self, device):
        """A person's request context: a real HttpRequest, no active branch."""
        request = RequestFactory().post("/")
        request.user = get_user_model().objects.create_user(username=f"op-{device.pk}")
        # NetBox's change-logging signal reads `request.id` unconditionally
        # (core/signals.py); a bare RequestFactory request has none.
        request.id = uuid.uuid4()
        token = current_request.set(request)
        self.addCleanup(current_request.reset, token)

    # --- the actual discriminator ------------------------------------------

    def test_a_real_request_releases_ownership_and_deletes_the_device(self):
        device = self._device("release-dev-1")
        self._own_it(device)
        self.assertEqual(ForwardDeviceIdentity.objects.filter(device=device).count(), 1)
        self.assertEqual(ForwardDeviceTagClaim.objects.filter(device=device).count(), 1)

        self._as_operator(device)
        device.delete()

        self.assertFalse(Device.objects.filter(pk=device.pk).exists())
        self.assertEqual(
            ForwardDeviceIdentity.objects.filter(device_id=device.pk).count(), 0
        )
        self.assertEqual(
            ForwardDeviceTagClaim.objects.filter(device_id=device.pk).count(), 0
        )

    def test_a_real_request_releases_every_syncs_rows_not_only_one(self):
        # The operator is deleting the DEVICE; bookkeeping about it must not
        # hold it hostage for a second sync either.
        device = self._device("release-dev-2")
        self._own_it(device, sync=self.sync)
        self._own_it(device, sync=self.other_sync)

        self._as_operator(device)
        device.delete()

        self.assertFalse(Device.objects.filter(pk=device.pk).exists())
        self.assertEqual(
            ForwardDeviceIdentity.objects.filter(device_id=device.pk).count(), 0
        )
        self.assertEqual(
            ForwardDeviceTagClaim.objects.filter(device_id=device.pk).count(), 0
        )

    def test_no_request_context_still_protects(self):
        # Every engine path - the sync, the fast baseline - runs with no
        # request at all. Nothing here should behave differently from before.
        device = self._device("release-dev-3")
        self._own_it(device)

        with self.assertRaises(ProtectedError) as ctx:
            device.delete()
        names = {obj.__class__.__name__ for obj in ctx.exception.protected_objects}
        self.assertEqual(names, {"ForwardDeviceIdentity", "ForwardDeviceTagClaim"})
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())

    def test_a_fake_request_still_protects(self):
        # The merge and the fast baseline attribute changes to a
        # `NetBoxFakeRequest`, not a real one - `event_tracking` wraps the
        # merge's apply in exactly this. A fake request must not be read as
        # an operator.
        from utilities.request import NetBoxFakeRequest

        device = self._device("release-dev-4")
        self._own_it(device)
        fake = NetBoxFakeRequest(
            {"META": {}, "POST": {}, "GET": {}, "FILES": {}, "user": None, "id": "x"}
        )
        token = current_request.set(fake)
        self.addCleanup(current_request.reset, token)

        with self.assertRaises(ProtectedError):
            device.delete()
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())

    def test_a_real_request_inside_an_active_branch_still_protects(self):
        # These tables are not branch-aware: netbox_branching does not
        # duplicate a plugin's own tables into a branch's isolated schema, so
        # a raw `Device.delete()` inside a merely-set `active_branch` (no
        # real schema provisioned) cannot exercise the actual cascade this
        # guards against - the ownership rows it would need to protect
        # simply are not visible through the branch's connection. The
        # contract under test is `release_on_operator_delete`'s own
        # discriminator, so it is called directly: given a real request AND
        # an active branch, it must still hand off to PROTECT, not CASCADE.
        from unittest.mock import Mock

        from forward_netbox.models import release_on_operator_delete

        device = self._device("release-dev-6")
        self._as_operator(device)
        branch = Branch.objects.create(name="release-test-branch")
        field = Mock()
        field.remote_field.model = ForwardDeviceIdentity

        token = active_branch.set(branch)
        try:
            with self.assertRaises(ProtectedError):
                release_on_operator_delete(Mock(), field, [device], "default")
        finally:
            active_branch.reset(token)

    def test_a_virtual_parent_claim_still_protects_under_a_real_request(self):
        # A parent with claimed virtual children is a real dependency the
        # panel names; it is not on the release path even for an operator.
        parent = self._device("release-parent")
        child = self._device("release-child")
        ForwardVirtualParentClaim.objects.create(
            sync=self.sync,
            ingestion=self.ingestion,
            device=child,
            parent_device=parent,
        )

        self._as_operator(parent)
        with self.assertRaises(ProtectedError) as ctx:
            parent.delete()
        names = {obj.__class__.__name__ for obj in ctx.exception.protected_objects}
        self.assertIn("ForwardVirtualParentClaim", names)
        self.assertTrue(Device.objects.filter(pk=parent.pk).exists())

    # --- protecting_relations still sees the custom on_delete ---------------

    def test_protecting_relations_still_lists_both_models(self):
        labels = {
            relation.related_model._meta.label
            for relation in protecting_relations(Device)
        }
        self.assertIn("forward_netbox.ForwardDeviceIdentity", labels)
        self.assertIn("forward_netbox.ForwardDeviceTagClaim", labels)

    # --- the actual bulk-delete view an operator uses ------------------------

    def test_bulk_delete_from_the_device_list_succeeds(self):
        device_a = self._device("release-bulk-a")
        device_b = self._device("release-bulk-b")
        self._own_it(device_a)
        self._own_it(device_b)

        user = get_user_model().objects.create_superuser(
            username="release-bulk-admin", password="x"
        )
        client = Client()
        client.force_login(user)
        response = client.post(
            reverse("dcim:device_bulk_delete"),
            {
                "pk": [device_a.pk, device_b.pk],
                "confirm": "true",
                "_confirm": "true",
            },
        )
        self.assertIn(response.status_code, (200, 302))
        self.assertFalse(
            Device.objects.filter(pk__in=[device_a.pk, device_b.pk]).exists()
        )
        self.assertEqual(
            ForwardDeviceIdentity.objects.filter(
                device_id__in=[device_a.pk, device_b.pk]
            ).count(),
            0,
        )
