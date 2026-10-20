from dcim.models import Device
from dcim.models import Interface
from django.contrib.contenttypes.models import ContentType
from extras.models import Tag
from ipam.models import IPAddress

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.utilities.explained_findings import explained_findings
from forward_netbox.utilities.primary_ip_reasons import BY_DESIGN
from forward_netbox.utilities.primary_ip_reasons import FIXABLE
from forward_netbox.utilities.primary_ip_reasons import NEEDS_A_PERSON
from forward_netbox.utilities.primary_ip_reasons import primary_ip_reasons
from forward_netbox.utilities.scope_reconciliation import UNCOVERED_TAG_SLUG


class PrimaryIpFixture(SubinterfaceFixture):
    """Each device without a primary IP has exactly one reason, and it adds up."""

    def new_device(self, name):
        return Device.objects.create(
            name=name,
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )

    def give_address(self, device, address):
        interface = Interface.objects.create(
            device=device, name=f"Eth-{device.name}", type="1000base-t"
        )
        return IPAddress.objects.create(
            address=address,
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )

    def build(self):
        uncovered = self.new_device("zq-gone-4481")
        uncovered.tags.add(
            Tag.objects.create(name="Uncovered", slug=UNCOVERED_TAG_SLUG)
        )
        owner = self.new_device("zq-owner-4482")
        owned = self.give_address(owner, "10.0.0.1/24")
        owner.primary_ip4 = owned
        owner.save()
        sharer = self.new_device("zq-sharer-4483")
        self.give_address(sharer, "10.0.0.1/32")
        unchosen = self.new_device("zq-unchosen-4484")
        self.give_address(unchosen, "10.0.0.9/24")
        bare = self.new_device("zq-bare-4485")
        Interface.objects.create(device=bare, name="Eth0", type="1000base-t")
        empty = self.new_device("zq-empty-4486")
        return [uncovered, sharer, unchosen, bare, empty]

    def missing(self, devices):
        return Device.objects.filter(pk__in=[d.pk for d in devices])


class DevicesWithoutAPrimaryIp(PrimaryIpFixture):
    def test_each_device_lands_in_exactly_one_reason_and_they_add_up(self):
        devices = self.build()

        rows = primary_ip_reasons(self.missing(devices))

        self.assertEqual(
            {row["reason"]: row["count"] for row in rows},
            {
                "not_refreshed_by_forward": 1,
                "shares_an_address_with_another_device": 1,
                "has_an_address_but_none_was_chosen": 1,
                "interfaces_carry_no_address": 1,
                "no_interfaces_synced": 1,
            },
        )
        self.assertEqual(sum(row["count"] for row in rows), len(devices))

    def test_only_the_unchosen_address_is_marked_fixable(self):
        rows = primary_ip_reasons(self.missing(self.build()))

        labels = {row["reason"]: row["fixable"] for row in rows}
        self.assertEqual(labels["has_an_address_but_none_was_chosen"], FIXABLE)
        self.assertEqual(labels["shares_an_address_with_another_device"], BY_DESIGN)
        self.assertEqual(labels["no_interfaces_synced"], NEEDS_A_PERSON)

    def test_every_reason_says_why_and_what_to_do(self):
        for row in primary_ip_reasons(self.missing(self.build())):
            self.assertTrue(row["why"].strip(), row["reason"])
            self.assertTrue(row["what_to_do"].strip(), row["reason"])

    def test_a_device_with_one_shared_and_one_unique_address_is_not_by_design(self):
        owner = self.new_device("o1")
        owned = self.give_address(owner, "10.7.0.1/24")
        owner.primary_ip4 = owned
        owner.save()
        mixed = self.new_device("m1")
        self.give_address(mixed, "10.7.0.1/32")
        second = Interface.objects.create(device=mixed, name="Eth9", type="1000base-t")
        IPAddress.objects.create(
            address="10.7.9.9/24",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=second.pk,
        )

        rows = primary_ip_reasons(self.missing([mixed]))

        self.assertEqual(
            [row["reason"] for row in rows], ["has_an_address_but_none_was_chosen"]
        )

    def test_two_devices_without_a_primary_that_share_an_address_are_by_design(self):
        first = self.new_device("h1")
        second = self.new_device("h2")
        self.give_address(first, "10.8.0.1/32")
        self.give_address(second, "10.8.0.1/32")

        rows = primary_ip_reasons(self.missing([first, second]))

        self.assertEqual(
            {row["reason"]: row["count"] for row in rows},
            {"shares_an_address_with_another_device": 2},
        )

    def test_an_ipv6_address_is_compared_by_host_not_mask(self):
        owner = self.new_device("o6")
        owned = self.give_address(owner, "2001:db8::1/64")
        owner.primary_ip6 = owned
        owner.save()
        sharer = self.new_device("s6")
        self.give_address(sharer, "2001:db8::1/128")

        rows = primary_ip_reasons(self.missing([sharer]))

        self.assertEqual(
            [row["reason"] for row in rows], ["shares_an_address_with_another_device"]
        )

    def test_the_advice_does_not_say_to_turn_on_what_is_on(self):
        unchosen = self.new_device("u1")
        self.give_address(unchosen, "10.9.0.9/24")

        off = primary_ip_reasons(self.missing([unchosen]))[0]["what_to_do"]
        on = primary_ip_reasons(self.missing([unchosen]), settings_on=True)[0][
            "what_to_do"
        ]

        self.assertIn("Turn on", off)
        self.assertNotIn("Turn on", on)

    def test_no_devices_means_no_rows(self):
        self.assertEqual(primary_ip_reasons(Device.objects.none()), [])


class ExplainedFindingsTest(PrimaryIpFixture):
    def bind(self, devices):
        for device in devices:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=device.name, device=device
            )

    def test_the_rows_carry_a_reason_a_fixable_label_and_an_action(self):
        devices = self.build()
        self.bind(devices)

        rows = explained_findings(self.sync)

        self.assertTrue(rows)
        for row in rows:
            for field in ("finding", "count", "reason", "fixable", "action"):
                self.assertTrue(str(row[field]).strip(), (row["finding"], field))
        self.assertIn(
            "Uncovered devices (all of NetBox)", {row["finding"] for row in rows}
        )
        self.assertEqual(
            sum(
                row["count"]
                for row in rows
                if row["finding"] == "Devices without a primary IP (this sync)"
            ),
            len(devices),
        )

    def test_nothing_to_explain_gives_no_rows(self):
        self.assertEqual(explained_findings(self.sync), [])

    def test_possible_orphans_are_reported_without_netbox_routing(self):
        from unittest import mock

        IPAddress.objects.create(address="10.99.0.1/32")

        with mock.patch(
            "forward_netbox.utilities.bgp_neighbour_ips.routing_installed",
            return_value=False,
        ):
            rows = explained_findings(self.sync)

        orphan = [r for r in rows if r["finding"].startswith("IP addresses")]
        self.assertEqual([r["count"] for r in orphan], [1])
        self.assertEqual(orphan[0]["fixable"], NEEDS_A_PERSON)

    def test_every_row_says_whose_devices_it_counts(self):
        devices = self.build()
        self.bind(devices)
        IPAddress.objects.create(address="10.98.0.1/32")

        for row in explained_findings(self.sync):
            self.assertRegex(row["finding"], r"\((this sync|all of NetBox)\)", row)


class HealthNeverBreaksOnThemTest(PrimaryIpFixture):
    def test_the_light_summary_does_not_build_them(self):
        from forward_netbox.utilities.health import sync_health_summary

        self.assertEqual(sync_health_summary(self.sync)["explained_findings"], [])

    def test_a_failure_building_them_gives_no_rows_not_a_broken_health(self):
        from unittest import mock

        from forward_netbox.utilities.health import _explained_findings

        with mock.patch(
            "forward_netbox.utilities.explained_findings.explained_findings",
            side_effect=RuntimeError("boom"),
        ):
            self.assertEqual(_explained_findings(self.sync), [])


class BundleTest(DevicesWithoutAPrimaryIp):
    def test_the_bundle_reasons_add_up_to_the_bare_count_and_hold_no_names(self):
        import json

        from forward_netbox.utilities.export_redaction import export_safe_payload
        from forward_netbox.views import _primary_ip_bundle_payload

        devices = self.build()
        for device in devices:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=f"key-{device.pk}", device=device
            )

        payload = _primary_ip_bundle_payload(self.sync)

        self.assertEqual(
            sum(row["count"] for row in payload["without_primary_ip_reasons"]),
            payload["without_primary_ip"],
        )
        exported = json.dumps(export_safe_payload(payload))
        for device in devices:
            self.assertNotIn(device.name, exported)
