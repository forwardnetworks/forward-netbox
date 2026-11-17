# A customer selected 66 devices Forward does not report and ran the verified
# delete. Nothing happened and nothing said why: 25 of them carried routing
# records (BGP scopes, OSPF interfaces) that NetBox will not delete a device over,
# and the job reported only a bare count. These pin that a held device is reported
# per device, and that the operator's routing option actually frees it.
from unittest.mock import patch

from dcim.models import Device
from dcim.models import Interface

from forward_netbox.tests.test_uncovered_device_cleanup import _Fixture
from forward_netbox.tests.test_unmanaged_device_prune import CENSUS
from forward_netbox.tests.test_unmanaged_device_prune import IN_SCOPE
from forward_netbox.utilities.scope_reconciliation import prune_unmanaged_devices
from forward_netbox.utilities.scope_reconciliation import summarize_held_devices

OSPF_INTERFACE = "netbox_routing.ospfinterface"
OSPF_INSTANCE = "netbox_routing.ospfinstance"


def _ospf_on(device):
    from netbox_routing.models import OSPFArea
    from netbox_routing.models import OSPFInstance
    from netbox_routing.models import OSPFInterface

    interface = Interface.objects.create(device=device, name="Gi0/0", type="1000base-t")
    instance = OSPFInstance.objects.create(
        name=f"{device.name}-ospf", router_id="10.0.0.1", process_id=1, device=device
    )
    area = OSPFArea.objects.create(area_id="0.0.0.0", area_type="standard")
    return OSPFInterface.objects.create(
        instance=instance, area=area, interface=interface
    )


class HeldDeviceTest(_Fixture):
    def _prune(self, devices, **kwargs):
        with (
            patch(
                "forward_netbox.utilities.scope_reconciliation.get_classic_device_collection",
                return_value={},
            ),
            patch(
                "forward_netbox.utilities.scope_reconciliation.get_configured_device_tags",
                return_value={},
            ),
        ):
            report = self._report(IN_SCOPE, CENSUS)
            return prune_unmanaged_devices(
                self.sync,
                restrict_to_device_pks=[device.pk for device in devices],
                report=report,
                **kwargs,
            )

    def test_a_device_with_routing_records_is_held_and_named_by_pk_and_label(self):
        self._device("in-scope")
        held = self._device("held")
        _ospf_on(held)

        result = self._prune([held])

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["protected_device_count"], 1)
        self.assertTrue(Device.objects.filter(pk=held.pk).exists())
        self.assertEqual(set(result["protected_by_device"]), {held.pk})
        self.assertIn(OSPF_INTERFACE, result["protected_by_device"][held.pk])
        self.assertFalse(result["release_routing"])

    def test_the_routing_option_deletes_the_device_and_its_routing_records(self):
        from netbox_routing.models import OSPFInstance
        from netbox_routing.models import OSPFInterface

        self._device("in-scope")
        held = self._device("held")
        _ospf_on(held)

        result = self._prune([held], release_routing=True)

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["protected_by_device"], {})
        self.assertTrue(result["release_routing"])
        self.assertFalse(Device.objects.filter(pk=held.pk).exists())
        self.assertEqual(OSPFInterface.objects.count(), 0)
        self.assertEqual(OSPFInstance.objects.count(), 0)

    def test_a_device_without_routing_records_is_deleted_either_way(self):
        self._device("in-scope")
        plain = self._device("plain")

        result = self._prune([plain])

        self.assertEqual(result["pruned_device_count"], 1)
        self.assertEqual(result["protected_by_device"], {})

    def test_the_routing_option_does_not_widen_eligibility(self):
        # A device Forward still reports is never deleted, option or not.
        reported = self._device("in-scope")
        _ospf_on(reported)

        result = self._prune([reported], release_routing=True)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=reported.pk).exists())
        self.assertEqual(result["refusals"]["not_unclaimed"], [reported.pk])

    def test_a_row_a_surviving_device_also_uses_keeps_the_device_whole(self):
        from netbox_routing.models import OSPFArea
        from netbox_routing.models import OSPFInstance
        from netbox_routing.models import OSPFInterface

        self._device("in-scope")
        held = self._device("held")
        survivor = self._device("survivor")
        # An OSPF instance on `held`, used by an interface on a device that stays:
        # owned by both, so no delete of `held` may take it.
        instance = OSPFInstance.objects.create(
            name="held-ospf", router_id="10.9.9.9", process_id=1, device=held
        )
        survivor_interface = Interface.objects.create(
            device=survivor, name="Eth7", type="1000base-t"
        )
        shared = OSPFInterface.objects.create(
            instance=instance,
            area=OSPFArea.objects.create(area_id="0.0.0.0"),
            interface=survivor_interface,
        )

        result = self._prune([held], release_routing=True)

        # Refused whole, rolled back, and named: the shared row is still there.
        self.assertEqual(result["pruned_device_count"], 0)
        self.assertTrue(Device.objects.filter(pk=held.pk).exists())
        self.assertTrue(OSPFInterface.objects.filter(pk=shared.pk).exists())
        self.assertTrue(OSPFInstance.objects.filter(pk=instance.pk).exists())
        self.assertIn(held.pk, result["protected_by_device"])
        # The option was on, so this device is not one the option frees.
        summary = summarize_held_devices(
            result["protected_by_device"], option_was_on=True
        )
        self.assertEqual(summary["freed_by_routing_option"], 0)
        self.assertEqual(summary["needs_other_records"], 1)

    def test_every_return_carries_the_detail_keys(self):
        # Nothing named deletes nothing, and still answers with the same keys.
        result = self._prune([])

        self.assertEqual(result["protected_by_device"], {})
        self.assertFalse(result["release_routing"])


class HeldSummaryTest(_Fixture):
    def test_devices_held_only_by_routing_records_are_freed_by_the_option(self):
        summary = summarize_held_devices(
            {
                1: {OSPF_INTERFACE: 2, "netbox_routing.bgpscope": 1},
                2: {OSPF_INTERFACE: 1},
                3: {"extras.configcontext": 1},
                4: {OSPF_INTERFACE: 1, "extras.configcontext": 1},
            }
        )

        self.assertEqual(summary["held_devices"], 4)
        self.assertEqual(summary["freed_by_routing_option"], 2)
        self.assertEqual(summary["needs_other_records"], 2)
        self.assertEqual(summary["by_model"][OSPF_INTERFACE], 4)

    def test_after_the_option_ran_nothing_still_held_counts_as_freed_by_it(self):
        summary = summarize_held_devices({1: {OSPF_INTERFACE: 1}}, option_was_on=True)

        self.assertEqual(summary["freed_by_routing_option"], 0)
        self.assertEqual(summary["needs_other_records"], 1)

    def test_nothing_held_is_an_empty_summary(self):
        summary = summarize_held_devices({})

        self.assertEqual(summary["held_devices"], 0)
        self.assertEqual(summary["by_model"], {})
