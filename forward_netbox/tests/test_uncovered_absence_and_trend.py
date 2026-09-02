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
from extras.models import Tag

from forward_netbox.choices import ForwardSyncStatusChoices
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
            responses.append(census_rows)
        client.run_nqe_query = Mock(side_effect=responses)
        with (
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            patch.object(ForwardSource, "get_client", return_value=client),
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

        # Scope query + ONE census, however many sets it classified.
        self.assertEqual(client.run_nqe_query.call_count, 2)
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
        self.assertEqual(client.run_nqe_query.call_count, 2)
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


class UncoveredListPagesTest(_Fixture):
    """Both halves listable in full, from the stored report."""

    def _client(self):
        user = get_user_model().objects.create_user(username="admin-why", password="x")
        user.is_superuser = True
        user.is_staff = True
        user.save()
        client = Client()
        client.force_login(user)
        return client

    def _stored_report(self):
        owned = self._device("owned-gone")
        self._own(owned)
        self._device("someone-elses")
        fwd_client = Mock()
        fwd_client.run_nqe_query = Mock(
            side_effect=[
                [{"name": "in-scope", "completed": True}],
                [{"name": "in-scope", "vendor": "Vendor.CISCO"}],
            ]
        )
        with (
            patch.object(ForwardSource, "get_client", return_value=fwd_client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
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
