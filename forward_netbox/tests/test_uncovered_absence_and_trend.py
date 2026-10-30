# The two questions the uncovered-device count could not answer.
#
# `forward-uncovered` made the bucket listable. A customer's next two questions
# were WHY each device is uncovered, and whether the count is GROWING. The first
# had an answer for orphans since 2.7.x (`_classify_out_of_scope_absence`) and
# none for the owned-uncovered set; the second had no signal anywhere. Both are
# here, plus the unclaimed half listable in full without a tag, which was the
# one thing on the page an operator could see 25 names of and nothing more.
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from extras.models import Tag

from forward_netbox.choices import ForwardSyncStatusChoices
from forward_netbox.models import ForwardDeviceAbsence
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.health import sync_health_summary
from forward_netbox.utilities.scope_reconciliation import compute_scope_reconciliation
from forward_netbox.utilities.scope_reconciliation import UNCOVERED_TAG_SLUG


class _Fixture(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="why-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="why-sync",
            source=self.source,
            status=ForwardSyncStatusChoices.COMPLETED,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1", baseline_ready=True
        )
        mfr = Manufacturer.objects.create(name="MfrW", slug="mfr-w")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-w", slug="dt-w")
        self.role = DeviceRole.objects.create(name="RoleW", slug="role-w")
        self.site = Site.objects.create(name="SiteW", slug="site-w")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def _own(self, device):
        return ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            source_device_key=device.name,
            device=device,
            ingestion_id=self.ingestion.pk,
            snapshot_id="snap-1",
        )

    def _report(self, scope_rows, census_rows=None):
        client = Mock()
        responses = [scope_rows]
        if census_rows is not None:
            # The census is two halves - devices, then endpoints - and the
            # endpoint half runs whether or not endpoint sync is on.
            responses.extend([census_rows, []])
        client.run_nqe_query = Mock(side_effect=responses)
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
            patch(
                "forward_netbox.utilities.scope_reconciliation.run_nqe_query",
                side_effect=lambda c, *a, **kw: c.run_nqe_query(*a, **kw),
            ),
        ):
            return compute_scope_reconciliation(self.sync), client


class OwnedAbsenceIsClassifiedTest(_Fixture):
    """WHY each owned-uncovered device is uncovered."""

    def test_the_owned_set_is_split_by_cause(self):
        self._device("in-scope")
        self._own(self._device("gone"))
        self._own(self._device("untagged"))
        self._own(self._device("custom"))

        report, client = self._report(
            [{"name": "in-scope", "completed": True}],
            [
                {"name": "in-scope", "vendor": "Vendor.CISCO"},
                # `gone` is deliberately absent from the census.
                {"name": "untagged", "vendor": "Vendor.ARISTA"},
                {"name": "custom", "vendor": "Vendor.FORWARD_CUSTOM"},
            ],
        )

        owned = report["unmanaged"]["owned_absence"]
        self.assertTrue(owned["available"])
        self.assertEqual(owned["absent_from_snapshot"], 1)
        self.assertEqual(owned["present_untagged"], 1)
        self.assertEqual(owned["vendor_excluded"], 1)
        self.assertEqual(owned["absent_from_snapshot_sample"], ["gone"])
        self.assertEqual(owned["present_untagged_sample"], ["untagged"])
        self.assertEqual(owned["vendor_excluded_sample"], ["custom"])

    def test_orphans_and_owned_devices_share_one_census(self):
        """Widening what the census classifies costs no extra NQE execution
        when orphans already exist - the query carries no predicate."""
        from forward_netbox.utilities.ownership import reconcile_sync_scope_tag_claims

        self._device("in-scope")
        self._device("orphan")
        self._own(self._device("owned-gone"))
        reconcile_sync_scope_tag_claims(
            self.sync,
            {"in-scope": ["Prod_Core"], "orphan": ["Prod_Core"]},
            generation=self.ingestion.pk,
            snapshot_id="snap-1",
        )

        report, client = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
        )

        # Scope query + ONE census (its device and endpoint halves), however
        # many sets it classified.
        self.assertEqual(client.run_nqe_query.call_count, 3)
        self.assertEqual(report["out_of_scope_absence"]["absent_from_snapshot"], 1)
        # The orphan is ALSO owned-uncovered: claiming it gave it an identity,
        # so it is in both sets, and both sets say the same thing about it.
        self.assertEqual(
            report["unmanaged"]["owned_absence"]["absent_from_snapshot"], 2
        )

    def test_the_customer_shape_costs_one_census_where_it_cost_none(self):
        """Orphans zero, owned-uncovered present. Before this change no census
        ran (nothing was asked); now one does, because this is exactly the case
        with a question. Recorded so `forward_api_usage` readers know why the
        count moved by one."""
        self._device("in-scope")
        self._own(self._device("owned-gone"))

        report, client = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
        )

        self.assertEqual(report["netbox_out_of_scope"], 0)
        self.assertEqual(client.run_nqe_query.call_count, 3)
        self.assertEqual(
            report["unmanaged"]["owned_absence"]["absent_from_snapshot"], 1
        )

    def test_no_census_runs_when_nothing_is_absent(self):
        self._device("in-scope")

        report, client = self._report([{"name": "in-scope", "completed": True}])

        self.assertEqual(client.run_nqe_query.call_count, 1)
        self.assertEqual(
            report["unmanaged"]["owned_absence"]["absent_from_snapshot"], 0
        )

    def test_a_failed_census_is_unavailable_for_both_sets_not_a_zero(self):
        self._device("in-scope")
        self._own(self._device("owned-gone"))
        client = Mock()
        client.run_nqe_query = Mock(
            side_effect=[[{"name": "in-scope", "completed": True}], RuntimeError("x")]
        )
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
            patch(
                "forward_netbox.utilities.scope_reconciliation.run_nqe_query",
                side_effect=lambda c, *a, **kw: c.run_nqe_query(*a, **kw),
            ),
        ):
            report = compute_scope_reconciliation(self.sync)

        self.assertFalse(report["unmanaged"]["owned_absence"]["available"])
        self.assertNotIn("absent_from_snapshot", report["unmanaged"]["owned_absence"])

    def test_both_halves_persist_their_device_keys_not_their_names(self):
        owned = self._device("owned-gone")
        self._own(owned)
        unclaimed = self._device("someone-elses")

        report, _client = self._report(
            [{"name": "in-scope", "completed": True}],
            [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
        )

        unmanaged = report["unmanaged"]
        self.assertEqual(unmanaged["owned_untagged_device_ids"], [owned.pk])
        self.assertEqual(unmanaged["unclaimed_device_ids"], [unclaimed.pk])


class UncoveredHealthSignalTest(_Fixture):
    """Whether the count is GROWING."""

    def _tagged(self, *names):
        tag, _ = Tag.objects.get_or_create(
            slug=UNCOVERED_TAG_SLUG, defaults={"name": "Forward Uncovered"}
        )
        for name in names:
            self._device(name).tags.add(tag)

    def _history(self, *totals):
        content_type = ContentType.objects.get_for_model(ForwardSync)
        for total in totals:
            Job.objects.create(
                object_type=content_type,
                object_id=self.sync.pk,
                name="reconcile device scope tags",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid4(),
                data={"total_uncovered": total},
            )

    def test_no_uncovered_devices_is_info(self):
        summary = sync_health_summary(self.sync)["uncovered"]
        self.assertEqual(summary["status"], "info")
        self.assertEqual(summary["uncovered_count"], 0)

    def test_a_steady_count_warns(self):
        self._tagged("u-1", "u-2")
        self._history(2, 2)
        summary = sync_health_summary(self.sync)["uncovered"]
        self.assertEqual(summary["status"], "warn")
        self.assertEqual(summary["uncovered_count"], 2)
        self.assertEqual(summary["trend_delta"], 0)

    def test_a_growing_count_escalates_to_danger_and_says_so(self):
        # The customer's complaint, verbatim: "the number keeps growing".
        self._tagged("u-1", "u-2", "u-3")
        self._history(49, 552)
        summary = sync_health_summary(self.sync)["uncovered"]
        self.assertEqual(summary["status"], "danger")
        self.assertEqual(summary["trend_delta"], 503)
        self.assertIn("Up 503", summary["message"])
        self.assertIn("49 -> 552", summary["message"])

    def test_the_message_points_at_the_cause_split(self):
        self._tagged("u-1")
        summary = sync_health_summary(self.sync)["uncovered"]
        self.assertIn("split by cause", summary["message"])
        self.assertIn("forward-uncovered", summary["message"])

    def test_out_of_scope_now_escalates_on_growth_too(self):
        tag = Tag.objects.create(
            name="Forward Out Of Scope", slug="forward-out-of-scope"
        )
        self._device("o-1").tags.add(tag)
        content_type = ContentType.objects.get_for_model(ForwardSync)
        for total in (1, 40):
            Job.objects.create(
                object_type=content_type,
                object_id=self.sync.pk,
                name="reconcile device scope tags",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid4(),
                data={"total_out_of_scope": total},
            )
        summary = sync_health_summary(self.sync)["out_of_scope"]
        self.assertEqual(summary["status"], "danger")


class _ListFixture(_Fixture):
    """Client and stored-report helpers shared by the device list tests."""

    def _client(self):
        user = get_user_model().objects.create_user(username="admin-why", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        return client

    def _stored_report(self, extra_unclaimed=0):
        owned = self._device("owned-gone")
        self._own(owned)
        self._device("someone-elses")
        for index in range(extra_unclaimed):
            self._device(f"unclaimed-{index:02d}")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            side_effect=[
                [{"name": "in-scope", "completed": True}],
                [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
                [],  # the census's endpoint half
            ]
        )
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch(
                "forward_netbox.utilities.scope_reconciliation.run_nqe_query",
                side_effect=lambda c, *a, **kw: c.run_nqe_query(*a, **kw),
            ),
        ):
            from forward_netbox.jobs import _scope_reconciliation_work

            job = Job.objects.create(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=self.sync.pk,
                name=f"{self.sync.name} - scope reconciliation",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid4(),
            )
            _scope_reconciliation_work(job)

    def _urls(self):
        base = {"forward_sync": self.sync.pk, "forward_list": "unclaimed"}
        query = "&".join(f"{k}={v}" for k, v in base.items())
        return (
            reverse("plugins:forward_netbox:device_list_bulk_delete") + "?" + query,
            reverse("plugins:forward_netbox:device_list_bulk_edit") + "?" + query,
        )

    def _ospf_on(self, device):
        from dcim.models import Interface
        from netbox_routing.models import OSPFArea
        from netbox_routing.models import OSPFInstance
        from netbox_routing.models import OSPFInterface

        interface = Interface.objects.create(
            device=device, name="po3", type="1000base-t"
        )
        instance = OSPFInstance.objects.create(
            name=f"ospf-{device.name}",
            router_id="10.0.0.1",
            process_id=1,
            device=device,
        )
        area = OSPFArea.objects.create(area_id="0.0.0.0", area_type="standard")
        return OSPFInterface.objects.create(
            instance=instance, area=area, interface=interface
        )


class UncoveredListPagesTest(_ListFixture):
    """Both halves listable in full, from the stored report."""

    def test_the_unclaimed_half_is_listable_without_a_tag(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_unclaimed_devices",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "someone-elses")
        self.assertNotContains(response, "owned-gone")

    def test_a_list_longer_than_a_page_paginates_and_offers_bulk_edit(self):
        # The hand-rendered table showed the first 25 rows with no paginator
        # and no way to select the rest, so "add the include tag to these"
        # reached 25 devices of a 190-device list.
        self._stored_report(extra_unclaimed=30)
        url = reverse(
            "plugins:forward_netbox:forwardsync_unclaimed_devices",
            kwargs={"pk": self.sync.pk},
        )
        client = self._client()
        # A page length below the row count: the default is 50 here, and the
        # reported 25 is the operator's own preference.
        first = client.get(url, {"per_page": 10})
        self.assertEqual(first.status_code, 200)
        self.assertContains(first, "31 device(s)")
        self.assertContains(first, "Showing 1-10 of 31")
        self.assertContains(
            first, reverse("plugins:forward_netbox:device_list_bulk_edit")
        )
        # Selection checkboxes: the bulk action acts on ticked rows.
        self.assertContains(first, 'name="pk"')
        # The remaining rows are on the next page, not lost.
        second = client.get(url, {"per_page": 10, "page": 2})
        self.assertContains(second, "Showing 11-20 of 31")

    def test_the_owned_half_is_listable_from_the_report(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_uncovered_devices",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "owned-gone")
        self.assertNotContains(response, "someone-elses")

    def test_without_a_report_the_page_says_so_instead_of_listing_nothing(self):
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_unclaimed_devices",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No scope reconciliation report has run yet")

    def test_the_panel_shows_the_cause_and_links_both_lists(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "gone from Forward")
        for name in ("forwardsync_uncovered_devices", "forwardsync_unclaimed_devices"):
            self.assertContains(
                response,
                reverse(f"plugins:forward_netbox:{name}", kwargs={"pk": self.sync.pk}),
            )


class UnclaimedNameTwinsTest(_ListFixture):
    """An unclaimed device that shares a name with a managed one is a copy.

    One customer found "a good many" of the 190 not-managed devices were
    duplicates of devices the sync manages, at another site. The site-relabel
    repair only pairs devices this sync created, so nothing said so.
    """

    def _twin_setup(self):
        managed = self._device("twin-dev")
        self._own(managed)
        other_site = Site.objects.create(name="SiteOther", slug="site-other")
        twin = Device.objects.create(
            name="TWIN-dev",  # NetBox compares names case-insensitively
            device_type=self.dt,
            role=self.role,
            site=other_site,
        )
        self._stored_report()
        return managed, twin

    def test_only_the_copy_that_is_not_managed_is_listed_as_a_duplicate(self):
        managed, twin = self._twin_setup()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_unclaimed_twins",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "TWIN-dev")
        self.assertContains(response, f'value="{twin.pk}"')
        self.assertNotContains(response, f'value="{managed.pk}"')
        # A device with no managed namesake is not a duplicate.
        self.assertNotContains(response, "someone-elses")

    def test_the_scope_page_counts_the_duplicates_and_links_them(self):
        self._twin_setup()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertContains(response, "1 share a name with a device this sync manages")
        self.assertContains(
            response,
            reverse(
                "plugins:forward_netbox:forwardsync_unclaimed_twins",
                kwargs={"pk": self.sync.pk},
            ),
        )

    def test_select_all_on_the_duplicates_list_means_only_the_duplicates(self):
        managed, twin = self._twin_setup()
        url = (
            reverse("plugins:forward_netbox:device_list_bulk_delete")
            + f"?forward_sync={self.sync.pk}&forward_list=unclaimed_twins"
        )
        response = self._client().post(url, {"_all": "1"})
        self.assertContains(response, "TWIN-dev")
        self.assertNotContains(response, "someone-elses")


class DeviceListBulkActionsTest(_ListFixture):
    """Edit and Delete Selected on the stored lists.

    NetBox resolves "select all N matching" against every device matching the
    request's filters; these lists carry none, so unrestricted it would act on
    every device. And netbox-routing makes an OSPF interface PROTECT its
    instance, so NetBox refuses to delete any device with OSPF configured.
    """

    def test_select_all_is_held_to_the_stored_list_on_delete_and_edit(self):
        self._stored_report()
        self._device("bystander")  # in NetBox, in no stored list
        client = self._client()
        for url in self._urls():
            with self.subTest(url=url.split("/")[-2]):
                response = client.post(url, {"_all": "1"})
                self.assertEqual(response.status_code, 200)
                self.assertContains(response, "someone-elses")
                self.assertNotContains(response, "bystander")
                self.assertNotContains(response, "owned-gone")

    def test_select_all_without_a_stored_list_is_refused(self):
        self._stored_report()
        client = self._client()
        for name in ("device_list_bulk_delete", "device_list_bulk_edit"):
            response = client.post(
                reverse(f"plugins:forward_netbox:{name}"), {"_all": "1"}, follow=True
            )
            self.assertContains(response, "Select all is only available")
            self.assertTrue(Device.objects.filter(name="someone-elses").exists())

    def test_a_refused_delete_says_what_blocks_it_and_what_to_do(self):
        self._stored_report()
        device = Device.objects.get(name="someone-elses")
        self._ospf_on(device)
        response = self._client().post(
            self._urls()[0],
            {"pk": device.pk, "_confirm": "1", "confirm": "True"},
            follow=True,
        )
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())
        self.assertContains(response, "netbox_routing.ospfinterface")
        self.assertContains(response, "tick the box")

    def test_the_confirmation_page_lists_what_blocks_and_offers_the_box(self):
        self._stored_report()
        device = Device.objects.get(name="someone-elses")
        self._ospf_on(device)
        response = self._client().post(self._urls()[0], {"pk": device.pk})
        self.assertContains(response, 'name="cascade_routing"')
        self.assertContains(response, "netbox_routing.ospfinterface")

    def test_the_cascade_removes_the_routing_rows_then_the_device(self):
        from netbox_routing.models import OSPFInterface

        self._stored_report()
        device = Device.objects.get(name="someone-elses")
        self._ospf_on(device)
        response = self._client().post(
            self._urls()[0],
            {
                "pk": device.pk,
                "_confirm": "1",
                "confirm": "True",
                "cascade_routing": "1",
            },
            follow=True,
        )
        self.assertFalse(Device.objects.filter(pk=device.pk).exists())
        self.assertEqual(OSPFInterface.objects.count(), 0)
        self.assertContains(response, "routing record(s) first")

    def test_the_cascade_refuses_a_blocker_outside_routing_and_restores_the_rows(
        self,
    ):
        from dcim.models import Interface
        from netbox_routing.models import OSPFInterface

        self._stored_report()
        device = Device.objects.get(name="someone-elses")
        ospf = self._ospf_on(device)
        # A child interface on ANOTHER device restricts deleting its parent.
        other = self._device("other-device")
        Interface.objects.create(
            device=other, name="po3.10", type="virtual", parent=ospf.interface
        )
        response = self._client().post(
            self._urls()[0],
            {
                "pk": device.pk,
                "_confirm": "1",
                "confirm": "True",
                "cascade_routing": "1",
            },
            follow=True,
        )
        self.assertTrue(Device.objects.filter(pk=device.pk).exists())
        self.assertEqual(OSPFInterface.objects.count(), 1)
        self.assertContains(response, "dcim.interface")


class DeviceCascadeSafetyTest(_ListFixture):
    """What the cascade must not touch, and what a failed delete must undo."""

    def _post_cascade(self, device, **extra):
        data = {
            "pk": device.pk,
            "_confirm": "1",
            "confirm": "True",
            "cascade_routing": "1",
        }
        data.update(extra)
        return self._client().post(self._urls()[0], data, follow=True)

    def test_another_devices_bgp_peer_is_not_removed_for_this_delete(self):
        # A BGP peer on device B that uses an address on device A as its
        # neighbour protects that address, so Django names it as a blocker of
        # A's delete. It is B's row, not A's.
        from dcim.models import Interface
        from ipam.models import ASN
        from ipam.models import IPAddress
        from ipam.models import RIR
        from netbox_routing.models import BGPPeer
        from netbox_routing.models import BGPRouter
        from netbox_routing.models import BGPScope

        from django.contrib.contenttypes.models import ContentType

        self._stored_report()
        a = Device.objects.get(name="someone-elses")
        b = self._device("bgp-owner")
        interface = Interface.objects.create(device=a, name="Eth9", type="1000base-t")
        ip = IPAddress.objects.create(
            address="10.1.1.1/32",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        asn = ASN.objects.create(asn=65001, rir=RIR.objects.create(name="r", slug="r"))
        router = BGPRouter.objects.create(name="r1", assigned_object=b, asn=asn)
        scope = BGPScope.objects.create(router=router)
        peer = BGPPeer.objects.create(scope=scope, peer=ip, remote_as=asn)

        response = self._post_cascade(a)

        self.assertTrue(Device.objects.filter(pk=a.pk).exists())
        self.assertTrue(BGPPeer.objects.filter(pk=peer.pk).exists())
        self.assertContains(response, "netbox_routing.bgppeer")

    def _bgp_peer_on(self, owner, address_device, *, scope=True):
        """A BGP peer on ``owner`` that uses an address on ``address_device``."""
        from dcim.models import Interface
        from django.contrib.contenttypes.models import ContentType
        from ipam.models import ASN
        from ipam.models import IPAddress
        from ipam.models import RIR
        from netbox_routing.models import BGPPeer
        from netbox_routing.models import BGPRouter
        from netbox_routing.models import BGPScope

        interface = Interface.objects.create(
            device=address_device, name="Eth9", type="1000base-t"
        )
        ip = IPAddress.objects.create(
            address="10.1.1.1/32",
            assigned_object_type=ContentType.objects.get_for_model(Interface),
            assigned_object_id=interface.pk,
        )
        asn = ASN.objects.create(asn=65001, rir=RIR.objects.create(name="r", slug="r"))
        bgp_scope = None
        if scope:
            router = BGPRouter.objects.create(name="r1", assigned_object=owner, asn=asn)
            bgp_scope = BGPScope.objects.create(router=router)
        return BGPPeer.objects.create(scope=bgp_scope, peer=ip, remote_as=asn)

    def test_a_peer_with_no_established_owner_is_refused(self):
        from netbox_routing.models import BGPPeer

        self._stored_report()
        a = Device.objects.get(name="someone-elses")
        peer = self._bgp_peer_on(None, a, scope=False)
        response = self._post_cascade(a)
        self.assertTrue(Device.objects.filter(pk=a.pk).exists())
        self.assertTrue(BGPPeer.objects.filter(pk=peer.pk).exists())
        self.assertContains(response, "netbox_routing.bgppeer")

    def test_selecting_both_devices_releases_the_peer_that_belongs_to_one_of_them(
        self,
    ):
        from netbox_routing.models import BGPPeer

        self._stored_report()
        a = Device.objects.get(name="someone-elses")
        b = self._device("bgp-owner")
        self._bgp_peer_on(b, a)
        self._client().post(
            self._urls()[0],
            {
                "pk": [a.pk, b.pk],
                "_confirm": "1",
                "confirm": "True",
                "cascade_routing": "1",
            },
            follow=True,
        )
        self.assertFalse(Device.objects.filter(pk__in=[a.pk, b.pk]).exists())
        self.assertEqual(BGPPeer.objects.count(), 0)

    def test_a_refused_cascade_clears_the_events_it_queued(self):
        from unittest.mock import patch

        self._stored_report()
        a = Device.objects.get(name="someone-elses")
        b = self._device("bgp-owner")
        self._bgp_peer_on(b, a)
        with patch("core.signals.clear_events.send") as clear:
            self._post_cascade(a)
        clear.assert_called()

    def test_owner_of_a_row_is_found_through_its_own_links(self):
        from dcim.models import Interface
        from netbox_routing.models import OSPFArea
        from netbox_routing.models import OSPFInstance
        from netbox_routing.models import OSPFInterface

        from forward_netbox.utilities.device_cascade_delete import owning_device_ids

        a = self._device("owner-a")
        b = self._device("owner-b")
        instance = OSPFInstance.objects.create(
            name="i", router_id="10.0.0.1", process_id=1, device=b
        )
        row = OSPFInterface.objects.create(
            instance=instance,
            area=OSPFArea.objects.create(area_id="0.0.0.0", area_type="standard"),
            interface=Interface.objects.create(
                device=a, name="Eth0", type="1000base-t"
            ),
        )
        self.assertEqual(owning_device_ids(row), {a.pk, b.pk})
        self.assertEqual(owning_device_ids(a), {a.pk})

    def test_the_background_job_box_is_ignored_so_the_delete_stays_in_one_step(self):
        self._stored_report()
        device = Device.objects.get(name="someone-elses")
        self._ospf_on(device)
        self._post_cascade(device, background_job="1")
        self.assertFalse(Device.objects.filter(pk=device.pk).exists())

    def test_select_all_on_a_filtered_list_means_the_filtered_rows(self):
        self._stored_report(extra_unclaimed=3)
        url = self._urls()[0] + "&name=unclaimed-01"
        response = self._client().post(url, {"_all": "1"})
        self.assertContains(response, "unclaimed-01")
        self.assertNotContains(response, "unclaimed-02")
        self.assertNotContains(response, "someone-elses")


class OrphanAndBackfilledListPagesTest(_Fixture):
    """The two remaining sets the panel counted but would not list.

    Prune orphans deletes every out-of-scope device and the page showed 25 of
    them; the backfilled set had the same 25-row ceiling. Both were listable
    only by reading a command's JSON.
    """

    def _client(self):
        user = get_user_model().objects.create_user(
            username="admin-lists", password="x"
        )
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        return client

    def _stored_report(self):
        # An absence row is one half of "this sync previously managed it", so
        # this device is out of scope without needing a live scope claim.
        gone = self._device("orphan-gone")
        now = timezone.now()
        ForwardDeviceAbsence.objects.create(
            sync=self.sync,
            device=gone,
            consecutive_absent_runs=1,
            first_absent_at=now,
            last_absent_at=now,
        )
        self._device("backfilled-here")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            side_effect=[
                [
                    # Tagged but never completed => backfilled.
                    {
                        "name": "backfilled-here",
                        "completed": False,
                        "tagNames": ["Prod_Core"],
                    },
                ],
                [],
                [],  # the census's endpoint half
            ]
        )
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch(
                "forward_netbox.utilities.scope_reconciliation.run_nqe_query",
                side_effect=lambda c, *a, **kw: c.run_nqe_query(*a, **kw),
            ),
        ):
            from forward_netbox.jobs import _scope_reconciliation_work

            job = Job.objects.create(
                object_type=ContentType.objects.get_for_model(ForwardSync),
                object_id=self.sync.pk,
                name=f"{self.sync.name} - scope reconciliation",
                status=JobStatusChoices.STATUS_COMPLETED,
                job_id=uuid4(),
            )
            _scope_reconciliation_work(job)

    def test_the_prune_orphans_target_set_is_listable_in_full(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_out_of_scope_devices",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "orphan-gone")
        self.assertNotContains(response, "backfilled-here")

    def test_the_backfilled_set_is_listable_in_full(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_present_backfilled_devices",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "backfilled-here")
        self.assertNotContains(response, "orphan-gone")

    def test_without_a_report_both_pages_say_so(self):
        client = self._client()
        for name in (
            "forwardsync_out_of_scope_devices",
            "forwardsync_present_backfilled_devices",
        ):
            with self.subTest(view=name):
                response = client.get(
                    reverse(
                        f"plugins:forward_netbox:{name}", kwargs={"pk": self.sync.pk}
                    )
                )
                self.assertEqual(response.status_code, 200)
                self.assertContains(
                    response, "No scope reconciliation report has run yet"
                )

    def test_the_panel_links_both_new_lists(self):
        self._stored_report()
        response = self._client().get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        )
        self.assertEqual(response.status_code, 200)
        for name in (
            "forwardsync_out_of_scope_devices",
            "forwardsync_present_backfilled_devices",
        ):
            self.assertContains(
                response,
                reverse(f"plugins:forward_netbox:{name}", kwargs={"pk": self.sync.pk}),
            )
