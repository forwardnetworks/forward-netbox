# The two IntegrityErrors that ended a customer sync on 2.6.7.
#
# Both were invisible before this release recorded the violated constraint on
# sync-phase issues: each read "row processing failed (IntegrityError)" and
# nothing more. With the constraint named, both root causes were identifiable
# from the issue list alone.
#
#   dcim.module   -> dcim_consoleport_unique_device_name
#   dcim.device   -> dcim_device_primary_ip4_id_key
from dcim.models import ConsolePort
from dcim.models import ConsolePortTemplate
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import ModuleBay
from dcim.models import ModuleType
from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.primary_ip import _existing_primary_ip_owners
from forward_netbox.utilities.sync_inventory_module import (
    _unadoptable_component_names,
)


def _device(name="dev-1"):
    manufacturer = Manufacturer.objects.create(name=f"m-{name}", slug=f"m-{name}")
    device_type = DeviceType.objects.create(
        manufacturer=manufacturer, model=f"t-{name}", slug=f"t-{name}"
    )
    site = Site.objects.create(name=f"s-{name}", slug=f"s-{name}")
    role = DeviceRole.objects.create(name=f"r-{name}", slug=f"r-{name}")
    return (
        Device.objects.create(name=name, device_type=device_type, role=role, site=site),
        manufacturer,
    )


class ModuleComponentCollisionTest(TestCase):
    """A component already owned by another module can be neither adopted nor made.

    NetBox builds its adoption candidates as
    `device.<components>.filter(module__isnull=True)`, so `_adopt_components`
    never sees a component belonging to a different module. The template
    instantiates a second one with the same name and
    `UNIQUE(device_id, name)` rejects it.
    """

    def _module_type(self, manufacturer, model, port_name):
        module_type = ModuleType.objects.create(manufacturer=manufacturer, model=model)
        ConsolePortTemplate.objects.create(module_type=module_type, name=port_name)
        return module_type

    def test_a_port_claimed_by_another_module_is_detected(self):
        device, manufacturer = _device()
        first_type = self._module_type(manufacturer, "mt-1", "Console")
        bay_one = ModuleBay.objects.create(device=device, name="bay-1")
        # Creating the module instantiates its console-port template and assigns
        # the port to that module, which is precisely what puts the name outside
        # adoption's candidate set for any later module.
        Module.objects.create(device=device, module_bay=bay_one, module_type=first_type)
        second_type = self._module_type(manufacturer, "mt-2", "Console")

        blocking = _unadoptable_component_names(device, second_type)

        self.assertEqual(blocking, ["consoleports:Console"])

    def test_an_unclaimed_port_is_left_to_adoption(self):
        # This is the case `_adopt_components` handles; flagging it would skip a
        # module that installs perfectly well.
        device, manufacturer = _device("dev-2")
        ConsolePort.objects.create(device=device, name="Console")
        module_type = self._module_type(manufacturer, "mt-3", "Console")

        self.assertEqual(_unadoptable_component_names(device, module_type), [])

    def test_a_module_type_with_no_templates_is_never_blocked(self):
        device, manufacturer = _device("dev-3")
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer, model="mt-bare"
        )

        self.assertEqual(_unadoptable_component_names(device, module_type), [])


class PrimaryIPOwnershipTest(TestCase):
    """NetBox allows one owner per primary IP; the Mgmt_ path never checked.

    `UNIQUE(primary_ip4_id)` means a second device resolving the same management
    address failed the entire ingestion at `device.save()` — after every workload
    had already been staged.
    """

    def test_an_unclaimed_address_has_no_owner(self):
        _device("dev-free")

        self.assertEqual(_existing_primary_ip_owners(), {})

    def test_an_existing_primary_is_reported_as_claimed(self):
        from ipam.models import IPAddress

        device, _manufacturer = _device("dev-owner")
        from dcim.models import Interface

        interface = Interface.objects.create(device=device, name="eth0", type="virtual")
        ip = IPAddress.objects.create(address="10.0.0.1/24", assigned_object=interface)
        device.primary_ip4 = ip
        device.save(update_fields=["primary_ip4"])

        owners = _existing_primary_ip_owners()

        self.assertEqual(owners.get(("primary_ip4", ip.pk)), device.pk)
        self.assertNotIn(("primary_ip6", ip.pk), owners)
