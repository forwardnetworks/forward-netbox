from dcim.models import Device
from dcim.models import Interface
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.urls import reverse
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
            self.assertRegex(
                row["finding"], r"\((this sync|all of NetBox|latest ingestion)\)", row
            )


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


class SkipReasonCatalogueTest(PrimaryIpFixture):
    def test_every_catalogued_reason_is_one_the_code_really_saves(self):
        import pathlib

        import forward_netbox
        from forward_netbox.utilities.skip_reason_catalogue import KNOWN

        root = pathlib.Path(forward_netbox.__file__).parent / "utilities"
        source = "\n".join(
            path.read_text()
            for path in root.glob("*.py")
            if path.name != "skip_reason_catalogue.py"
        )
        for reason in KNOWN:
            self.assertIn(f'"{reason}"', source, reason)

    def test_the_parentless_interface_advice_never_says_to_set_a_parent(self):
        from forward_netbox.utilities.skip_reason_catalogue import explain_skip_reason

        why, fixable, action = explain_skip_reason(
            "subinterface-name-taken-by-parentless-interface"
        )

        # Those interfaces are real top-level ones; giving them a parent corrupts
        # them. The advice must say so, and it is not a fix an operator can make.
        self.assertIn("Do not set a parent", action)
        self.assertNotEqual(fixable, "yes")

    def test_every_named_reason_says_why_whether_fixable_and_what_to_do(self):
        from forward_netbox.utilities.skip_reason_catalogue import KNOWN

        for reason, (why, fixable, action) in KNOWN.items():
            self.assertTrue(why.strip() and fixable.strip() and action.strip(), reason)

    def test_the_open_ended_reasons_are_explained_not_dropped(self):
        from forward_netbox.utilities.skip_reason_catalogue import explain_skip_reason

        why, _, action = explain_skip_reason("missing-modulebay")
        self.assertIn("modulebay", why)
        self.assertIn("modulebay", action)
        why, _, action = explain_skip_reason("still-referenced-by-vlan")
        self.assertIn("vlan", why)

    def test_a_reason_nobody_catalogued_gets_an_honest_generic_answer(self):
        from forward_netbox.utilities.skip_reason_catalogue import explain_skip_reason
        from forward_netbox.utilities.skip_reason_catalogue import GENERIC

        self.assertEqual(explain_skip_reason("something-new"), GENERIC)
        self.assertEqual(explain_skip_reason(""), GENERIC)


class IssueRowsTest(PrimaryIpFixture):
    def test_skipped_rows_are_grouped_by_why_with_a_link_to_the_issues(self):
        from forward_netbox.models import ForwardIngestion
        from forward_netbox.models import ForwardIngestionIssue

        ingestion = ForwardIngestion.objects.create(sync=self.sync, snapshot_id="s")
        for _ in range(3):
            ForwardIngestionIssue.objects.create(
                ingestion=ingestion,
                model="dcim.interface",
                exception="ForwardDependencySkipError",
                message="m",
                raw_data={},
                coalesce_fields={
                    "skip_reason": "subinterface-name-taken-by-parentless-interface"
                },
            )

        rows = [
            r
            for r in explained_findings(self.sync)
            if r["finding"].startswith("Skipped rows in dcim.interface")
        ]

        self.assertEqual([r["count"] for r in rows], [3])
        self.assertIn("top-level interface", rows[0]["reason"])
        self.assertEqual(rows[0]["fixable"], NEEDS_A_PERSON)
        self.assertIn("model=dcim.interface", rows[0]["url"])
        # The link opens THIS ingestion's issues, not every ingestion's.
        self.assertTrue(
            rows[0]["url"].startswith(
                reverse(
                    "plugins:forward_netbox:forwardingestion_issues",
                    kwargs={"pk": ingestion.pk},
                )
            )
        )


class EveryRowLinksSomewhereTest(PrimaryIpFixture):
    def test_the_rows_that_have_something_to_open_carry_a_working_link(self):
        devices = self.build()
        for device in devices:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=device.name, device=device
            )
        IPAddress.objects.create(address="10.97.0.1/32")
        user = get_user_model().objects.create_superuser(
            "root-x", "x@example.com", "pw"
        )
        self.client.force_login(user)

        rows = explained_findings(self.sync)

        self.assertTrue(rows)
        for row in rows:
            self.assertTrue(row["url"], row["finding"])
            self.assertTrue(row["url_label"], row["finding"])
            self.assertEqual(self.client.get(row["url"]).status_code, 200, row["url"])


class DrillDownTest(PrimaryIpFixture):
    def setUp(self):
        super().setUp()
        self.devices = self.build()
        for device in self.devices:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=device.name, device=device
            )
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_explained", args=[self.sync.pk]
        )

    def test_it_lists_the_devices_behind_one_reason_and_only_those(self):
        user = get_user_model().objects.create_superuser(
            "root-y", "y@example.com", "pw"
        )
        self.client.force_login(user)

        response = self.client.get(
            self.url, {"reason": "has_an_address_but_none_was_chosen"}
        )

        self.assertContains(response, "zq-unchosen-4484")
        self.assertNotContains(response, "zq-bare-4485")
        self.assertContains(response, "Turn on the primary-IP settings")

    def test_an_unknown_reason_lists_nothing(self):
        user = get_user_model().objects.create_superuser(
            "root-z", "z@example.com", "pw"
        )
        self.client.force_login(user)

        response = self.client.get(self.url, {"reason": "made-up"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "zq-")

    def test_someone_without_view_permission_cannot_see_the_names(self):
        user = get_user_model().objects.create(username="nobody-here")
        self.client.force_login(user)

        response = self.client.get(
            self.url, {"reason": "has_an_address_but_none_was_chosen"}
        )

        self.assertNotEqual(response.status_code, 200)
        self.assertNotContains(
            response, "zq-unchosen-4484", status_code=response.status_code
        )


class UncoveredSplitTest(PrimaryIpFixture):
    """Uncovered devices are split by what the last scope report says about them."""

    def make(self, report=True):
        from unittest import mock

        from django.utils import timezone

        from forward_netbox.models import ForwardDeviceAbsence

        tag = Tag.objects.create(name="Uncovered", slug=UNCOVERED_TAG_SLUG)
        self.gone = self.new_device("zq-gone-9001")
        self.still = self.new_device("zq-still-9002")
        self.other = self.new_device("zq-other-9003")
        for device in (self.gone, self.still, self.other):
            device.tags.add(tag)
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=self.gone,
            consecutive_absent_runs=7,
            first_absent_at=timezone.now(),
            last_absent_at=timezone.now(),
            last_absent_snapshot_id="s1",
        )
        payload = {
            "unmanaged": {
                "owned_untagged_device_ids": [self.gone.pk, self.still.pk],
                "owned_absent_device_ids": [self.gone.pk],
                "owned_absence": {"available": True},
                "owned_detail_by_id": {
                    str(self.gone.pk): "absent_configured_disabled",
                    str(self.still.pk): "endpoint_untagged",
                },
            }
        }
        patcher = mock.patch(
            "forward_netbox.utilities.scope_reconciliation.latest_scope_report",
            return_value=(None, payload if report else None, timezone.now(), None),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def by_group(self):
        from forward_netbox.utilities.explained_findings import uncovered_devices

        return uncovered_devices(self.sync)[0]

    def test_a_device_forward_no_longer_returns_is_told_from_one_that_lost_its_tag(
        self,
    ):
        self.make()

        groups = self.by_group()

        self.assertEqual(groups["uncovered_absent"], [self.gone.pk])
        self.assertEqual(groups["uncovered_in_forward"], [self.still.pk])
        self.assertEqual(groups["uncovered_unrecorded"], [self.other.pk])

    def test_every_uncovered_device_is_in_exactly_one_group(self):
        self.make()

        groups = self.by_group()

        flat = [pk for pks in groups.values() for pk in pks]
        self.assertEqual(
            sorted(flat), sorted({self.gone.pk, self.still.pk, self.other.pk})
        )
        self.assertEqual(len(flat), len(set(flat)))

    def test_having_an_absence_record_alone_does_not_mean_forward_dropped_it(self):
        from django.utils import timezone

        from forward_netbox.models import ForwardDeviceAbsence

        self.make()
        # Still in Forward under other tags, yet reconciliation also recorded it.
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=self.still,
            consecutive_absent_runs=1,
            first_absent_at=timezone.now(),
            last_absent_at=timezone.now(),
        )

        groups = self.by_group()

        self.assertNotIn(self.still.pk, groups["uncovered_absent"])
        self.assertIn(self.still.pk, groups["uncovered_in_forward"])

    def test_a_failed_absence_check_does_not_make_gone_devices_look_present(self):
        self.make()
        from forward_netbox.utilities import scope_reconciliation

        _job, payload, at, err = scope_reconciliation.latest_scope_report(self.sync)
        payload["unmanaged"]["owned_absent_device_ids"] = []
        payload["unmanaged"]["owned_absence"] = {"available": False}

        groups = self.by_group()

        self.assertEqual(groups["uncovered_in_forward"], [])
        self.assertEqual(groups["uncovered_absent"], [])
        self.assertEqual(
            sorted(groups["uncovered_unclassified"]),
            sorted([self.gone.pk, self.still.pk]),
        )

    def test_a_report_from_before_the_absence_check_existed_is_unclassified(self):
        self.make()
        from forward_netbox.utilities import scope_reconciliation

        _job, payload, at, err = scope_reconciliation.latest_scope_report(self.sync)
        del payload["unmanaged"]["owned_absent_device_ids"]

        groups = self.by_group()

        self.assertEqual(groups["uncovered_in_forward"], [])
        self.assertEqual(len(groups["uncovered_unclassified"]), 2)

    def test_without_a_scope_report_nothing_is_described_by_a_guess(self):
        self.make(report=False)

        groups = self.by_group()

        self.assertEqual(groups["uncovered_absent"], [])
        self.assertEqual(groups["uncovered_in_forward"], [])
        self.assertEqual(len(groups["uncovered_unrecorded"]), 3)

    def test_the_drill_down_names_the_rule_and_how_long_it_has_been_uncovered(self):
        self.make()
        user = get_user_model().objects.create_superuser(
            "root-u", "u@example.com", "pw"
        )
        self.client.force_login(user)
        url = reverse(
            "plugins:forward_netbox:forwardsync_explained", args=[self.sync.pk]
        )

        absent = self.client.get(url, {"reason": "uncovered_absent"})
        in_forward = self.client.get(url, {"reason": "uncovered_in_forward"})

        self.assertContains(absent, "zq-gone-9001")
        self.assertContains(absent, "collection is disabled")
        self.assertNotContains(absent, "absent_configured_disabled")
        self.assertContains(absent, "7 run(s), since")
        self.assertNotContains(absent, "zq-still-9002")
        self.assertContains(in_forward, "zq-still-9002")
        self.assertContains(in_forward, "match none of the include tags")
        self.assertNotContains(in_forward, "endpoint_untagged")
        self.assertNotContains(in_forward, "zq-gone-9001")

    def test_a_user_who_may_open_the_sync_but_not_the_devices_sees_no_names(self):
        from forward_netbox.models import ForwardSync
        from forward_netbox.tests.test_change_control_comments import grant

        self.make()
        user = get_user_model().objects.create(username="sync-viewer-only")
        grant(user, ForwardSync, "view")
        self.client.force_login(user)
        url = reverse(
            "plugins:forward_netbox:forwardsync_explained", args=[self.sync.pk]
        )

        response = self.client.get(url, {"reason": "uncovered_absent"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "zq-gone-9001")
        # The header counts what this user can see, not the unrestricted total.
        self.assertContains(response, "<td>0</td>")

    def new_device(self, name):
        return Device.objects.create(
            name=name,
            device_type=self.device.device_type,
            role=self.device.role,
            site=self.device.site,
        )


class AllDevicesWithoutAPrimaryIpTest(PrimaryIpFixture):
    """One page for every device without a primary IP, by role and reason."""

    def setUp(self):
        super().setUp()
        from dcim.models import DeviceRole

        self.devices = self.build()
        for device in self.devices:
            ForwardDeviceIdentity.objects.create(
                sync=self.sync, source_device_key=device.name, device=device
            )
        # Two of the five are routers; the rest keep the fixture's role.
        self.router_role = DeviceRole.objects.create(
            name="zq-router-role", slug="zq-router-role"
        )
        for device in self.devices[:2]:
            device.role = self.router_role
            device.save()
        self.url = reverse(
            "plugins:forward_netbox:forwardsync_explained", args=[self.sync.pk]
        )
        user = get_user_model().objects.create_superuser(
            "root-a", "a@example.com", "pw"
        )
        self.client.force_login(user)

    def names(self):
        return [d.name for d in self.devices]

    def test_every_device_without_a_primary_ip_is_listed_with_its_reason(self):
        response = self.client.get(self.url, {"reason": "all"})

        for name in self.names():
            self.assertContains(response, name)
        for title in (
            "Has an address, none chosen",
            "Interfaces carry no address",
            "No interfaces synced",
            "Shares an address with another device",
        ):
            self.assertContains(response, title)

    def test_the_role_buttons_count_each_role(self):
        response = self.client.get(self.url, {"reason": "all"})

        chips = {c["name"]: c["count"] for c in response.context["chips"]}
        self.assertEqual(chips["All roles"], 5)
        self.assertEqual(chips["zq-router-role"], 2)

    def test_choosing_a_role_narrows_the_list_to_that_role(self):
        response = self.client.get(
            self.url, {"reason": "all", "role": self.router_role.pk}
        )

        shown = {row["device"].name for row in response.context["rows"]}
        self.assertEqual(shown, {self.devices[0].name, self.devices[1].name})
        self.assertEqual(response.context["total"], 2)

    def test_a_role_that_is_not_in_the_list_is_ignored(self):
        for bad in ("9999999", "abc", "-1", "\u00b2", "9" * 5000, ""):
            response = self.client.get(self.url, {"reason": "all", "role": bad})

            self.assertEqual(response.context["total"], 5, bad)

    def test_the_legend_names_each_reason_present_once_with_what_to_do(self):
        response = self.client.get(self.url, {"reason": "all"})

        legend = response.context["legend"]
        reasons = [row["reason"] for row in legend]
        self.assertEqual(len(reasons), len(set(reasons)))
        self.assertEqual(sum(row["count"] for row in legend), 5)
        for row in legend:
            self.assertTrue(row["why"].strip() and row["what_to_do"].strip())
        self.assertContains(response, "How to read this")

    def test_the_role_filter_works_on_a_single_reason_list_too(self):
        # "Interfaces carry no address" holds one router (devices[... via the bare
        # device is the fixture's role) - use the reason that spans both roles:
        # every reason is a list of one device here, so narrow by a role that the
        # list does offer, and check a role it does not offer is ignored.
        base = {"reason": "has_an_address_but_none_was_chosen"}
        offered = self.devices[2].role_id

        narrowed = self.client.get(self.url, {**base, "role": offered})
        not_offered = self.client.get(self.url, {**base, "role": self.router_role.pk})

        self.assertEqual(narrowed.context["total"], 1)
        self.assertEqual(
            [c["name"] for c in narrowed.context["chips"] if c["active"]],
            [self.devices[2].role.name],
        )
        # No button exists for a role with no device in this list, so a made-up
        # link for it shows the whole list rather than an empty one.
        self.assertEqual(not_offered.context["total"], 1)
        self.assertEqual(
            [c["name"] for c in not_offered.context["chips"] if c["active"]],
            ["All roles"],
        )

    def test_the_legend_adds_up_to_the_devices_under_the_chosen_role(self):
        response = self.client.get(
            self.url, {"reason": "all", "role": self.router_role.pk}
        )

        self.assertEqual(response.context["total"], 2)
        self.assertEqual(sum(r["count"] for r in response.context["legend"]), 2)

    def test_each_row_carries_its_own_reason(self):
        response = self.client.get(self.url, {"reason": "all"})

        by_name = {
            row["device"].name: row["reason_title"] for row in response.context["rows"]
        }
        self.assertEqual(by_name["zq-unchosen-4484"], "Has an address, none chosen")
        self.assertEqual(by_name["zq-empty-4486"], "No interfaces synced")
        self.assertEqual(by_name["zq-bare-4485"], "Interfaces carry no address")

    def test_someone_who_may_open_the_sync_but_not_the_devices_sees_none(self):
        from forward_netbox.models import ForwardSync
        from forward_netbox.tests.test_change_control_comments import grant

        user = get_user_model().objects.create(username="sync-viewer-all")
        grant(user, ForwardSync, "view")
        self.client.force_login(user)

        response = self.client.get(self.url, {"reason": "all"})

        self.assertEqual(response.status_code, 200)
        for name in self.names():
            self.assertNotContains(response, name)
        self.assertEqual(response.context["total"], 0)
        self.assertEqual([c["count"] for c in response.context["chips"]], [0])
        # The legend follows the viewer too: two numbers on one page must agree.
        self.assertEqual(response.context["legend"], [])

    def test_the_health_page_links_to_it(self):
        health = reverse(
            "plugins:forward_netbox:forwardsync_health", args=[self.sync.pk]
        )

        response = self.client.get(health)

        self.assertContains(response, f"{self.url}?reason=all")
