# Before 3.0.14 an include tag that matched no device deleted every platform,
# and because a device's platform is SET_NULL the field was cleared on every
# device that had one. A device outside the sync's scope was never revisited.
# The restore puts back exactly what the change log proves a sync run took away.
import uuid
from datetime import timedelta

from core.models import ObjectChange
from core.models import ObjectType
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.test import TestCase
from django.utils import timezone

from extras.models import ConfigContext
from virtualization.models import Cluster
from virtualization.models import ClusterType
from virtualization.models import VirtualMachine

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.platform_restore import HOLD_CHANGED_SINCE
from forward_netbox.utilities.platform_restore import HOLD_PLATFORM_GONE
from forward_netbox.utilities.platform_restore import plan_platform_restore
from forward_netbox.utilities.platform_restore import PlatformRestoreRefused
from forward_netbox.utilities.platform_restore import preview_platform_restore
from forward_netbox.utilities.platform_restore import REFUSED_GREW_SINCE_PREVIEW
from forward_netbox.utilities.platform_restore import REFUSED_NO_PREVIEW
from forward_netbox.utilities.platform_restore import restore_platforms


class PlatformRestoreTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="plat-src",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="plat-sync", source=source, parameters={}
        )
        self.request_id = uuid.uuid4()
        ForwardIngestion.objects.create(
            sync=self.sync,
            change_request_id=self.request_id,
            merge_finalized_at=timezone.now(),
        )
        maker = Manufacturer.objects.create(name="Maker", slug="maker")
        self.site = Site.objects.create(name="Site", slug="site")
        self.role = DeviceRole.objects.create(name="Role", slug="role")
        self.device_type = DeviceType.objects.create(
            manufacturer=maker, model="Model", slug="model"
        )
        self.now = timezone.now()
        # A platform that was deleted by the run, and the one that replaced it.
        self.old = Platform.objects.create(name="os-a", slug="os-a")
        self.old_pk = self.old.pk
        self.new_pk = None
        self.device_ct = ObjectType.objects.get_for_model(Device)
        self.platform_ct = ObjectType.objects.get_for_model(Platform)

    def _device(self, name):
        return Device.objects.create(
            name=name,
            site=self.site,
            role=self.role,
            device_type=self.device_type,
            platform=self.old,
        )

    def _change(self, obj_pk, ct, action, pre, post, *, request_id=None, minutes=0):
        change = ObjectChange.objects.create(
            request_id=request_id or self.request_id,
            user_name="forward",
            changed_object_type=ct,
            changed_object_id=obj_pk,
            object_repr="x",
            action=action,
            prechange_data=pre,
            postchange_data=post,
        )
        ObjectChange.objects.filter(pk=change.pk).update(
            time=self.now + timedelta(minutes=minutes)
        )

    def _blank_by_the_run(self, device, *, request_id=None, minutes=1):
        self._change(
            device.pk,
            self.device_ct,
            "update",
            {"platform": self.old_pk},
            {"platform": None},
            request_id=request_id,
            minutes=minutes,
        )
        Device.objects.filter(pk=device.pk).update(platform=None)

    def _delete_and_recreate_platform(self):
        self._change(
            self.old_pk,
            self.platform_ct,
            "delete",
            {"slug": "os-a", "name": "os-a"},
            None,
            minutes=1,
        )
        self.old.delete()
        self.new_pk = Platform.objects.create(name="os-a", slug="os-a").pk

    def test_a_device_the_run_blanked_is_restorable_and_gets_its_platform_back(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device)
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(plan["restorable"], {("dcim.device", device.pk): self.new_pk})
        summary = preview_platform_restore(self.sync)
        self.assertEqual(summary["restorable"], 1)
        result = restore_platforms(self.sync, preview_restorable=1)
        self.assertEqual(result["restored"], 1)
        device.refresh_from_db()
        self.assertEqual(device.platform_id, self.new_pk)

    def test_a_platform_still_there_under_its_pk_is_used_as_is(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device)

        self.assertEqual(
            plan_platform_restore(self.sync)["restorable"],
            {("dcim.device", device.pk): self.old_pk},
        )

    def test_a_blanking_by_someone_else_is_never_restored(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device, request_id=uuid.uuid4())

        self.assertEqual(plan_platform_restore(self.sync)["restorable"], {})

    def test_a_platform_a_person_set_and_cleared_afterwards_is_left_alone(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device)
        self._change(
            device.pk,
            self.device_ct,
            "update",
            {"platform": self.old_pk},
            {"platform": None},
            request_id=uuid.uuid4(),
            minutes=30,
        )
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(plan["restorable"], {})
        self.assertEqual(plan["held"], {HOLD_CHANGED_SINCE: 1})

    def test_a_device_that_has_a_platform_again_is_not_a_candidate(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device)
        Device.objects.filter(pk=device.pk).update(platform=self.old)

        self.assertEqual(plan_platform_restore(self.sync)["restorable"], {})

    def test_a_platform_that_cannot_be_found_again_is_held(self):
        device = self._device("edge-1")
        self._blank_by_the_run(device)
        self._change(
            self.old_pk, self.platform_ct, "delete", {"slug": "os-a"}, None, minutes=2
        )
        self.old.delete()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(plan["restorable"], {})
        self.assertEqual(plan["held"], {HOLD_PLATFORM_GONE: 1})

    def test_a_sync_with_no_ingestion_has_nothing_to_restore(self):
        ForwardIngestion.objects.all().delete()

        self.assertEqual(plan_platform_restore(self.sync)["restorable"], {})

    def test_the_restore_needs_a_preview_and_never_exceeds_it(self):
        first = self._device("edge-1")
        second = self._device("edge-2")
        self._blank_by_the_run(first)
        self._blank_by_the_run(second)
        self._delete_and_recreate_platform()

        with self.assertRaises(PlatformRestoreRefused) as caught:
            restore_platforms(self.sync)
        self.assertEqual(caught.exception.reason, REFUSED_NO_PREVIEW)
        with self.assertRaises(PlatformRestoreRefused) as caught:
            restore_platforms(self.sync, preview_restorable=1)
        self.assertEqual(caught.exception.reason, REFUSED_GREW_SINCE_PREVIEW)
        self.assertIsNone(Device.objects.get(pk=first.pk).platform_id)

    def test_a_later_update_by_the_sync_that_leaves_the_platform_blank_is_not_a_blanking(
        self,
    ):
        device = self._device("edge-1")
        self._blank_by_the_run(device, minutes=1)
        self._delete_and_recreate_platform()
        # Any later change by the sync (tags, a custom field): the platform was
        # already blank, and NetBox still writes the key.
        self._change(
            device.pk,
            self.device_ct,
            "update",
            {"platform": None},
            {"platform": None},
            minutes=40,
        )

        plan = plan_platform_restore(self.sync)

        self.assertEqual(plan["restorable"], {("dcim.device", device.pk): self.new_pk})
        self.assertEqual(plan["blanked_by_this_sync"], 1)

    def test_a_virtual_machine_the_deleted_platform_cleared_is_restored_too(self):
        cluster = Cluster.objects.create(
            name="c1", type=ClusterType.objects.create(name="ct", slug="ct")
        )
        vm = VirtualMachine.objects.create(
            name="vm-1", cluster=cluster, platform=self.old
        )
        vm_ct = ObjectType.objects.get_for_model(VirtualMachine)
        self._change(
            vm.pk,
            vm_ct,
            "update",
            {"platform": self.old_pk},
            {"platform": None},
            minutes=1,
        )
        VirtualMachine.objects.filter(pk=vm.pk).update(platform=None)
        # A VM someone else cleared (not this sync's request) is never touched.
        other = VirtualMachine.objects.create(
            name="vm-2", cluster=cluster, platform=self.old
        )
        self._change(
            other.pk,
            vm_ct,
            "update",
            {"platform": self.old_pk},
            {"platform": None},
            request_id=uuid.uuid4(),
            minutes=1,
        )
        VirtualMachine.objects.filter(pk=other.pk).update(platform=None)
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(
            plan["restorable"], {("virtualization.virtualmachine", vm.pk): self.new_pk}
        )
        result = restore_platforms(self.sync, preview_restorable=1)
        self.assertEqual(result["restored"], 1)
        vm.refresh_from_db()
        other.refresh_from_db()
        self.assertEqual(vm.platform_id, self.new_pk)
        self.assertIsNone(other.platform_id)

    def test_a_device_and_a_vm_with_the_same_pk_are_both_restored(self):
        cluster = Cluster.objects.create(
            name="c2", type=ClusterType.objects.create(name="ct2", slug="ct2")
        )
        shared_pk = 900_000
        device = Device.objects.create(
            pk=shared_pk,
            name="edge-same",
            site=self.site,
            role=self.role,
            device_type=self.device_type,
            platform=self.old,
        )
        vm = VirtualMachine.objects.create(
            pk=shared_pk, name="vm-same", cluster=cluster, platform=self.old
        )
        self.assertEqual(device.pk, vm.pk)
        self._blank_by_the_run(device)
        self._change(
            vm.pk,
            ObjectType.objects.get_for_model(VirtualMachine),
            "update",
            {"platform": self.old_pk},
            {"platform": None},
            minutes=1,
        )
        VirtualMachine.objects.filter(pk=vm.pk).update(platform=None)
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(len(plan["restorable"]), 2)
        result = restore_platforms(self.sync, preview_restorable=2)
        self.assertEqual(result["restored"], 2)
        self.assertEqual(
            result["restorable_by_model"],
            {"dcim.device": 1, "virtualization.virtualmachine": 1},
        )

    def _context_change(self, context, before, after, *, request_id=None, minutes=1):
        self._change(
            context.pk,
            ObjectType.objects.get_for_model(ConfigContext),
            "update",
            {"platforms": before},
            {"platforms": after},
            request_id=request_id,
            minutes=minutes,
        )

    def test_a_config_context_the_deleted_platform_was_removed_from_is_restored(self):
        keep = Platform.objects.create(name="os-keep", slug="os-keep")
        context = ConfigContext.objects.create(name="cc-1", data={"a": 1})
        context.platforms.set([keep])
        # The run's deletion took os-a out of the context, logged as its update.
        self._context_change(context, [self.old_pk, keep.pk], [keep.pk])
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(
            plan["restorable"],
            {("extras.configcontext", context.pk, self.old_pk): self.new_pk},
        )
        result = restore_platforms(self.sync, preview_restorable=1)
        self.assertEqual(result["restored"], 1)
        self.assertEqual(
            set(context.platforms.values_list("pk", flat=True)), {keep.pk, self.new_pk}
        )

    def test_a_config_context_link_removed_by_someone_else_is_left_alone(self):
        context = ConfigContext.objects.create(name="cc-2", data={"a": 1})
        self._context_change(context, [self.old_pk], [], request_id=uuid.uuid4())
        self._delete_and_recreate_platform()

        self.assertEqual(plan_platform_restore(self.sync)["restorable"], {})

    def test_a_config_context_a_person_changed_since_is_held(self):
        context = ConfigContext.objects.create(name="cc-3", data={"a": 1})
        self._context_change(context, [self.old_pk], [], minutes=1)
        # A person later set and cleared the same platform on it.
        self._context_change(
            context, [], [self.old_pk], request_id=uuid.uuid4(), minutes=2
        )
        self._context_change(
            context, [self.old_pk], [], request_id=uuid.uuid4(), minutes=3
        )
        self._delete_and_recreate_platform()

        plan = plan_platform_restore(self.sync)

        self.assertEqual(plan["restorable"], {})
        self.assertEqual(plan["held"].get(HOLD_CHANGED_SINCE), 1)
