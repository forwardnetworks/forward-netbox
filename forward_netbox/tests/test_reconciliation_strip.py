from datetime import timedelta
from unittest import mock

from dcim.models import Device
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils import timezone

from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.tests.test_uncovered_device_cleanup import (
    _Fixture as _ReportFixture,
)
from forward_netbox.utilities.reconciliation_strip import KEYS_READ
from forward_netbox.utilities.reconciliation_strip import reconciliation_strip

REPORT = {
    "include_tags": ["Prod"],
    "exclude_tags": ["Lab"],
    # 4,128 distinct names, but 4,130 named devices: two devices share a name.
    "netbox_device_count": 4128,
    "netbox_named_device_total": 4130,
    "forward_in_scope_completed": 3800,
    "forward_in_scope_endpoints": 62,
    "forward_tagged_backfilled": 5,
    "forward_missing_in_netbox": 10,
    "netbox_out_of_scope": 45,
    "unmanaged": {"untagged_total": 257, "owned_untagged": 45, "unclaimed": 212},
    # Names in the stored report must never reach the strip.
    "out_of_scope_sample": ["zq-secret-host-1"],
}


class StripFixture(SubinterfaceFixture):
    def report(self, payload=REPORT, generated_at=None, error=""):
        patcher = mock.patch(
            "forward_netbox.utilities.scope_reconciliation.latest_scope_report",
            return_value=(None, payload, generated_at or timezone.now(), error),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def steps(self, strip):
        return {step["label"]: step for step in strip["steps"]}

    def counts(self, strip):
        return {label: step["count"] for label, step in self.steps(strip).items()}


class ReconciliationStripTest(StripFixture):
    def test_each_step_carries_the_reports_own_number_under_the_pages_own_label(self):
        self.report()

        counts = self.counts(reconciliation_strip(self.sync))

        self.assertEqual(counts["In scope (collected)"], 3800)
        self.assertEqual(counts["In scope (SNMP endpoints)"], 62)
        self.assertEqual(counts["In scope, missing from NetBox"], 10)
        self.assertEqual(counts["Tagged but backfilled"], 5)
        self.assertEqual(counts["Carry no include tag"], 257)
        self.assertEqual(counts["created by this sync"], 45)
        self.assertEqual(counts["not claimed by this sync"], 212)

    def test_collected_and_endpoints_are_not_summed_into_one_claim(self):
        self.report()

        counts = self.counts(reconciliation_strip(self.sync))

        self.assertNotIn(3862, counts.values())

    def test_backfilled_devices_are_a_separate_group_not_part_of_the_in_scope_count(
        self,
    ):
        self.report()

        steps = self.steps(reconciliation_strip(self.sync))

        self.assertEqual(steps["Tagged but backfilled"]["indent"], 0)
        self.assertIn(
            "not in the numbers above", steps["Tagged but backfilled"]["meaning"]
        )
        self.assertEqual(steps["In scope, missing from NetBox"]["indent"], 1)

    def test_matched_is_the_named_device_total_minus_the_ones_carrying_no_tag(self):
        self.report()

        counts = self.counts(reconciliation_strip(self.sync))

        # Both taken over devices, so they can be subtracted; the distinct-name
        # count (4,128) is not used for it.
        self.assertEqual(counts["NetBox devices (with a name)"], 4130)
        self.assertEqual(counts["matched to a tagged Forward device"], 4130 - 257)

    def test_an_older_report_without_the_device_total_does_not_guess_a_matched_count(
        self,
    ):
        payload = {k: v for k, v in REPORT.items() if k != "netbox_named_device_total"}
        self.report(payload=payload)

        strip = reconciliation_strip(self.sync)
        counts = self.counts(strip)

        self.assertNotIn("matched to a tagged Forward device", counts)
        self.assertEqual(counts["NetBox devices"], 4128)
        meaning = self.steps(strip)["NetBox devices"]["meaning"]
        self.assertIn("run Scope Reconciliation again", meaning)

    def test_the_two_groups_without_a_tag_add_up_to_the_ones_without_a_tag(self):
        self.report()

        counts = self.counts(reconciliation_strip(self.sync))

        self.assertEqual(
            counts["created by this sync"] + counts["not claimed by this sync"],
            counts["Carry no include tag"],
        )

    def test_orphans_are_their_own_line_and_make_no_claim_about_being_the_same_set(
        self,
    ):
        self.report(payload={**REPORT, "netbox_out_of_scope": 40})

        steps = self.steps(reconciliation_strip(self.sync))

        orphans = steps["of those, also listed as out-of-scope orphans"]
        self.assertEqual(orphans["count"], 40)
        self.assertEqual(steps["created by this sync"]["count"], 45)
        self.assertIn("normally matches", orphans["meaning"])
        self.assertIn("can differ", orphans["meaning"])

    def test_nothing_says_what_happens_to_an_uncovered_device(self):
        self.report()

        for step in reconciliation_strip(self.sync)["steps"]:
            for claim in ("keeps them", "never removed", "operator action", "Prune"):
                self.assertNotIn(claim, step["meaning"], step["label"])

    def test_the_tags_are_kept_out_of_the_descriptions(self):
        self.report()

        strip = reconciliation_strip(self.sync)

        self.assertEqual(strip["include_tags"], ["Prod"])
        self.assertEqual(strip["exclude_tags"], ["Lab"])
        for step in strip["steps"]:
            self.assertNotIn("Prod", step["meaning"])
            self.assertNotIn("Lab", step["meaning"])

    def test_the_first_step_says_why_it_is_smaller_than_the_whole_network(self):
        self.report()

        first = reconciliation_strip(self.sync)["steps"][0]

        self.assertIn("smaller than your whole Forward network", first["meaning"])

    def test_no_device_name_from_the_report_reaches_the_strip(self):
        self.report()

        self.assertNotIn("zq-secret-host-1", str(reconciliation_strip(self.sync)))

    def test_every_step_says_what_it_counts(self):
        self.report()

        for step in reconciliation_strip(self.sync)["steps"]:
            self.assertTrue(step["meaning"].strip(), step["label"])

    def test_a_missing_report_is_said_so_with_a_way_to_make_one(self):
        self.report(payload=None)

        strip = reconciliation_strip(self.sync)

        self.assertFalse(strip["available"])
        self.assertIn("Run Scope Reconciliation", strip["message"])
        self.assertIn("scope-reconciliation", strip["url"])

    def test_a_run_that_produced_no_report_is_not_described_as_missing(self):
        self.report(payload={}, error="boom")

        strip = reconciliation_strip(self.sync)

        self.assertFalse(strip["available"])
        self.assertIn("did not produce a report", strip["message"])

    def test_an_old_report_is_flagged_and_a_fresh_one_is_not(self):
        self.report(generated_at=timezone.now() - timedelta(days=30))
        self.assertTrue(reconciliation_strip(self.sync)["old"])

    def test_a_fresh_report_is_not_flagged(self):
        self.report()
        self.assertFalse(reconciliation_strip(self.sync)["old"])

    def test_a_report_with_no_timestamp_is_shown_without_an_age(self):
        patcher = mock.patch(
            "forward_netbox.utilities.scope_reconciliation.latest_scope_report",
            return_value=(None, REPORT, None, ""),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        strip = reconciliation_strip(self.sync)

        self.assertTrue(strip["available"])
        self.assertIsNone(strip["age_days"])
        self.assertFalse(strip["old"])


class AgainstTheRealReportTest(_ReportFixture):
    """The strip against a report the real producer made, not one written by hand."""

    def build(self):
        from dcim.models import Site

        self.tagged = self._device("in-scope")
        other_site = Site.objects.create(name="SiteC2", slug="site-c2")
        # Two devices share a name, so distinct names and devices differ.
        self.dup_a = self._device("dup")
        self.dup_b = Device.objects.create(
            name="dup", device_type=self.dt, role=self.role, site=other_site
        )
        self.gone = self._own(self._device("gone"))
        self.stranger = self._device("stranger")

    def stored(self):
        from forward_netbox.utilities.scope_reconciliation import public_scope_report

        report = self._report(
            [{"name": "in-scope", "completed": True, "tagNames": ["Prod_Core"]}]
        )
        return public_scope_report(report)

    def strip_of(self, payload):
        patcher = mock.patch(
            "forward_netbox.utilities.scope_reconciliation.latest_scope_report",
            return_value=(None, payload, timezone.now(), ""),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return reconciliation_strip(self.sync)

    def test_every_key_the_strip_reads_is_in_the_real_report(self):
        self.build()
        payload = self.stored()

        for key in KEYS_READ:
            present = key in payload or key in (payload.get("unmanaged") or {})
            self.assertTrue(present, key)

    def test_the_real_counts_obey_the_identities_the_strip_relies_on(self):
        self.build()
        payload = self.stored()
        unmanaged = payload["unmanaged"]

        # Devices, not names: two devices share the name "dup".
        named_devices = Device.objects.exclude(name="").count()
        self.assertEqual(payload["netbox_named_device_total"], named_devices)
        self.assertGreater(
            payload["netbox_named_device_total"], payload["netbox_device_count"]
        )
        self.assertEqual(
            unmanaged["owned_untagged"] + unmanaged["unclaimed"],
            unmanaged["untagged_total"],
        )
        counts = {
            step["label"]: step["count"] for step in self.strip_of(payload)["steps"]
        }
        self.assertEqual(
            counts["matched to a tagged Forward device"],
            payload["netbox_named_device_total"] - unmanaged["untagged_total"],
        )
        # Only the one tagged device is matched; the rest carry no include tag.
        self.assertEqual(counts["matched to a tagged Forward device"], 1)
        # untagged: the two "dup" devices, "gone" (ours) and "stranger".
        self.assertEqual(counts["Carry no include tag"], 4)
        self.assertEqual(counts["created by this sync"], 1)
        self.assertEqual(counts["not claimed by this sync"], 3)

    def test_after_the_tag_step_records_absence_the_orphans_track_the_uncovered(self):
        from forward_netbox.utilities.scope_reconciliation import record_device_absence

        self.build()
        record_device_absence(self.sync, (), uncovered_pks=[self.gone.pk])

        payload = self.stored()

        # The wording says "normally matches": this is the case it rests on.
        self.assertEqual(payload["netbox_out_of_scope"], 1)
        self.assertEqual(payload["unmanaged"]["owned_untagged"], 1)


class HealthShowsTheStripTest(StripFixture):
    def setUp(self):
        super().setUp()
        user = get_user_model().objects.create_superuser(
            "root-r", "r@example.com", "pw"
        )
        self.client.force_login(user)
        self.health = reverse(
            "plugins:forward_netbox:forwardsync_health", args=[self.sync.pk]
        )

    def test_the_health_page_renders_the_flow_and_the_scope_tags(self):
        self.report()

        response = self.client.get(self.health)

        self.assertContains(response, "How the device numbers fit together")
        self.assertContains(response, "In scope (collected)")
        self.assertContains(response, "3800")
        self.assertContains(response, "<code>Prod</code>")
        self.assertContains(response, "Report from")

    def test_the_light_summary_does_not_build_it(self):
        from forward_netbox.utilities.health import sync_health_summary

        self.assertIsNone(sync_health_summary(self.sync)["reconciliation"])

    def test_a_failure_building_it_gives_nothing_not_a_broken_page(self):
        from forward_netbox.utilities.health import _reconciliation

        with mock.patch(
            "forward_netbox.utilities.reconciliation_strip.reconciliation_strip",
            side_effect=RuntimeError("boom"),
        ):
            self.assertIsNone(_reconciliation(self.sync))
