"""An operator's manual device delete refused by another plugin's rows.

`describe_delete_blockers` already names them - a customer's device was
refused by ten `netbox_routing` BGP rows reached through the cascade, and the
ownership panel started saying so. Naming is not releasing: an operator still
had to go find and delete each one by hand before NetBox would let the device
go. These test `release_foreign_delete_blockers`, which does exactly that
release, scoped to an explicit app-label allowlist and nothing else, and
rolls back whole if it meets a blocker outside it.

Fake blocker classes stand in for real `netbox_routing` models rather than
exercising a real `Collector.collect` cascade, for the same reason the
operator-delete memory gives for testing that discriminator directly: these
tables are not branch-aware, and the object under test is the release loop's
own decisions, not Django's delete machinery.
"""

from unittest.mock import Mock
from unittest.mock import patch

from django.db.models.deletion import ProtectedError
from django.test import TestCase

from forward_netbox.utilities.workload_state import (
    ForeignDeleteBlockerNotAllowlisted,
)
from forward_netbox.utilities.workload_state import (
    ForeignDeleteBlockerSafetyCapExceeded,
)
from forward_netbox.utilities.workload_state import RELEASABLE_FOREIGN_APP_LABELS
from forward_netbox.utilities.workload_state import release_foreign_delete_blockers


def _make_blocker_class(app_label, model_name):
    """A stand-in for a Django model shaped just enough for the release loop:
    ``_meta.app_label``/``_meta.label`` to classify it, and
    ``objects.filter(pk__in=...).delete()`` to release it. Returns the class
    and the queryset mock its manager always returns, so a test can drive or
    assert on ``queryset.delete``.
    """
    queryset = Mock(name=f"{model_name}_queryset")
    queryset.delete.return_value = (1, {})
    manager = Mock(filter=Mock(return_value=queryset))
    meta = Mock(app_label=app_label, label=f"{app_label}.{model_name}")
    cls = type(model_name, (), {"_meta": meta, "objects": manager})
    return cls, queryset


class NoBlockersTest(TestCase):
    @patch("django.db.models.deletion.Collector.collect")
    def test_returns_empty_when_nothing_blocks_the_delete(self, mock_collect):
        mock_collect.return_value = None
        self.assertEqual(release_foreign_delete_blockers(Mock(pk=1)), {})


class SingleLevelReleaseTest(TestCase):
    @patch("django.db.models.deletion.Collector.collect")
    def test_releases_an_allowlisted_blocker(self, mock_collect):
        OSPFInstance, queryset = _make_blocker_class("netbox_routing", "OSPFInstance")
        self.assertIn("netbox_routing", RELEASABLE_FOREIGN_APP_LABELS)
        blocker = OSPFInstance()
        blocker.pk = 7
        exc = ProtectedError("blocked", [blocker])
        mock_collect.side_effect = [exc, None]

        released = release_foreign_delete_blockers(Mock(pk=1))

        self.assertEqual(released, {"netbox_routing.OSPFInstance": 1})
        OSPFInstance.objects.filter.assert_called_once_with(pk__in=[7])
        queryset.delete.assert_called_once()


class DeeperChainTest(TestCase):
    @patch("django.db.models.deletion.Collector.collect")
    def test_a_blocker_blocked_by_another_resolves_before_retrying(self, mock_collect):
        # OSPFInterface protects OSPFInstance in real netbox_routing; deleting
        # the instance first raised, then only succeeded once its own
        # blocker was cleared - exactly the shape `_delete_releasing` exists
        # for, rather than surfacing the deeper block as a fresh failure.
        OSPFInstance, instance_qs = _make_blocker_class(
            "netbox_routing", "OSPFInstance"
        )
        OSPFInterface, interface_qs = _make_blocker_class(
            "netbox_routing", "OSPFInterface"
        )
        instance_blocker = OSPFInstance()
        instance_blocker.pk = 1
        interface_blocker = OSPFInterface()
        interface_blocker.pk = 2

        outer_exc = ProtectedError("blocked", [instance_blocker])
        inner_exc = ProtectedError("blocked deeper", [interface_blocker])
        mock_collect.side_effect = [outer_exc, None]
        instance_qs.delete.side_effect = [inner_exc, (1, {})]

        released = release_foreign_delete_blockers(Mock(pk=1))

        self.assertEqual(
            released,
            {
                "netbox_routing.OSPFInterface": 1,
                "netbox_routing.OSPFInstance": 1,
            },
        )
        interface_qs.delete.assert_called_once()
        self.assertEqual(instance_qs.delete.call_count, 2)


class NonAllowlistedBlockerTest(TestCase):
    @patch("django.db.models.deletion.Collector.collect")
    def test_refuses_and_deletes_nothing(self, mock_collect):
        SoftwareVersion, queryset = _make_blocker_class("netbox_dlm", "SoftwareVersion")
        blocker = SoftwareVersion()
        blocker.pk = 3
        mock_collect.side_effect = [ProtectedError("blocked", [blocker])]

        with self.assertRaises(ForeignDeleteBlockerNotAllowlisted) as ctx:
            release_foreign_delete_blockers(Mock(pk=1))

        self.assertEqual(ctx.exception.label, "netbox_dlm.SoftwareVersion")
        self.assertEqual(ctx.exception.count, 1)
        queryset.delete.assert_not_called()

    @patch("django.db.models.deletion.Collector.collect")
    def test_a_mixed_chain_rolls_back_the_allowlisted_part_too(self, mock_collect):
        # The allowlisted OSPFInstance would have been released, but the
        # deeper blocker behind it belongs to an app outside the allowlist -
        # nothing should be deleted at all, not even the first row.
        OSPFInstance, instance_qs = _make_blocker_class(
            "netbox_routing", "OSPFInstance"
        )
        SoftwareVersion, sv_qs = _make_blocker_class("netbox_dlm", "SoftwareVersion")
        instance_blocker = OSPFInstance()
        instance_blocker.pk = 1
        sv_blocker = SoftwareVersion()
        sv_blocker.pk = 9

        outer_exc = ProtectedError("blocked", [instance_blocker])
        inner_exc = ProtectedError("blocked deeper", [sv_blocker])
        mock_collect.side_effect = [outer_exc]
        instance_qs.delete.side_effect = [inner_exc]

        with self.assertRaises(ForeignDeleteBlockerNotAllowlisted) as ctx:
            release_foreign_delete_blockers(Mock(pk=1))

        self.assertEqual(ctx.exception.label, "netbox_dlm.SoftwareVersion")
        sv_qs.delete.assert_not_called()


class SafetyCapTest(TestCase):
    @patch("django.db.models.deletion.Collector.collect")
    def test_a_non_converging_release_stops_instead_of_looping(self, mock_collect):
        OSPFInstance, instance_qs = _make_blocker_class(
            "netbox_routing", "OSPFInstance"
        )
        pks = iter(range(1000))

        def _always_blocked(*_args, **_kwargs):
            blocker = OSPFInstance()
            blocker.pk = next(pks)
            raise ProtectedError("blocked", [blocker])

        mock_collect.side_effect = _always_blocked

        with self.assertRaises(ForeignDeleteBlockerSafetyCapExceeded):
            release_foreign_delete_blockers(Mock(pk=1))


class ReleaseOfferTest(TestCase):
    """`_release_foreign_blockers_offer` decides whether the panel's button
    can appear at all - offering one that would just fail on the first
    non-allowlisted row is worse than the plain instructions it replaces."""

    def _offer(self, foreign_blockers, primary_sync_pk=1):
        from forward_netbox.template_content import (
            _release_foreign_blockers_offer,
        )

        return _release_foreign_blockers_offer(foreign_blockers, primary_sync_pk)

    def test_no_blockers_offers_nothing(self):
        offer = self._offer([])
        self.assertFalse(offer["offered"])
        self.assertEqual(offer["unreleasable"], [])

    def test_fully_allowlisted_blockers_are_offered(self):
        blockers = [("netbox_routing.OSPFInstance", 1)]
        offer = self._offer(blockers)
        self.assertTrue(offer["offered"])
        self.assertEqual(offer["releasable"], blockers)
        self.assertEqual(offer["unreleasable"], [])
        self.assertIn("netbox_routing.OSPFInstance", offer["expected_blockers_json"])
        self.assertIn("release-foreign-delete-blockers", offer["url"])

    def test_a_mix_offers_nothing_rather_than_a_partial_release(self):
        blockers = [
            ("netbox_routing.OSPFInstance", 1),
            ("netbox_dlm.SoftwareVersion", 2),
        ]
        offer = self._offer(blockers)
        self.assertFalse(offer["offered"])
        self.assertEqual(offer["unreleasable"], blockers)

    def test_no_owning_sync_offers_nothing(self):
        offer = self._offer([("netbox_routing.OSPFInstance", 1)], primary_sync_pk=None)
        self.assertFalse(offer["offered"])


class RealNetboxRoutingIntegrationTest(TestCase):
    """The mocked tests above prove the release loop's own decisions against
    controlled shapes; this proves it against the REAL netbox_routing
    on-delete graph. `OSPFInstance.device` is CASCADE and `OSPFInterface.
    interface` is CASCADE, but `OSPFInterface.instance` is PROTECT - so
    deleting a device with an OSPF instance that still has interfaces is
    refused not because the device's own relations protect it, but because
    the CASCADE to OSPFInstance runs into a PROTECT one level further in,
    exactly the shape `describe_delete_blockers`'s own docstring names for
    BGPRouter. If netbox_routing ever changes that on_delete, this fails
    LOUDLY instead of the mocked tests staying green while the real feature
    silently stops working.
    """

    @classmethod
    def setUpTestData(cls):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Interface
        from dcim.models import Manufacturer
        from dcim.models import Site

        manufacturer = Manufacturer.objects.create(name="Mfr-RT", slug="mfr-rt")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="dt-rt", slug="dt-rt"
        )
        role = DeviceRole.objects.create(name="Role-RT", slug="role-rt")
        site = Site.objects.create(name="Site-RT", slug="site-rt")
        cls.device = Device.objects.create(
            name="routing-dev", device_type=device_type, role=role, site=site
        )
        cls.interface = Interface.objects.create(
            device=cls.device, name="Vlan1", type="virtual"
        )

    def _ospf_instance_and_interface(self):
        from django.apps import apps

        OSPFArea = apps.get_model("netbox_routing", "OSPFArea")
        OSPFInstance = apps.get_model("netbox_routing", "OSPFInstance")
        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")

        area = OSPFArea.objects.create(area_id="0.0.0.0")
        instance = OSPFInstance.objects.create(
            name="OSPF-1",
            router_id="1.1.1.1",
            process_id=1,
            device=self.device,
        )
        ospf_interface = OSPFInterface.objects.create(
            instance=instance,
            area=area,
            interface=self.interface,
        )
        return instance, ospf_interface

    def test_ospf_interface_really_blocks_the_device_delete(self):
        # Establishes the premise the release feature exists for, against
        # the real schema, before proving the release itself.
        from forward_netbox.utilities.workload_state import describe_delete_blockers

        self._ospf_instance_and_interface()

        blockers = describe_delete_blockers(self.device)

        self.assertIn(("netbox_routing.OSPFInterface", 1), blockers)

    def test_release_clears_the_real_blocker_and_the_device_then_deletes(self):
        from dcim.models import Device
        from django.apps import apps

        from forward_netbox.utilities.workload_state import describe_delete_blockers

        OSPFInterface = apps.get_model("netbox_routing", "OSPFInterface")
        instance, ospf_interface = self._ospf_instance_and_interface()

        released = release_foreign_delete_blockers(self.device)

        self.assertEqual(released, {"netbox_routing.OSPFInterface": 1})
        self.assertFalse(OSPFInterface.objects.filter(pk=ospf_interface.pk).exists())
        # The instance itself was never a blocker (nothing points at it with
        # PROTECT once its one interface is gone) and is untouched - the
        # release deletes exactly what was named, not the instance too.
        self.assertTrue(type(instance).objects.filter(pk=instance.pk).exists())
        self.assertEqual(describe_delete_blockers(self.device), [])

        self.device.delete()

        self.assertFalse(Device.objects.filter(pk=self.device.pk).exists())
