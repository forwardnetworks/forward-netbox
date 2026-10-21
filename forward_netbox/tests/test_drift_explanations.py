from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase
from django.urls import reverse

from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.utilities.drift_explanations import COLUMN_HELP
from forward_netbox.utilities.drift_explanations import DEVICE_DENOMINATOR
from forward_netbox.utilities.drift_explanations import explain_drift_row
from forward_netbox.utilities.drift_report import compute_drift_report


def row(**extra):
    base = {
        "model": "ipam.vlan",
        "comparison_available": True,
        "comparison_unavailable_reason": "",
        "in_sync": False,
        "pending_changes": 0,
        "pending_removes": 0,
        "already_removed": 0,
    }
    base.update(extra)
    return base


class ExplainDriftRowTest(SimpleTestCase):
    def test_a_row_in_sync_says_nothing(self):
        self.assertEqual(
            explain_drift_row(row(in_sync=True)), {"meaning": "", "action": ""}
        )

    def test_a_map_fetched_in_full_by_design_is_not_a_fault(self):
        text = explain_drift_row(
            row(
                comparison_available=False,
                comparison_unavailable_reason="full_only_by_design",
                in_sync=None,
            )
        )

        self.assertIn("No row-by-row comparison is made", text["meaning"])
        self.assertIn("another row for the same model", text["meaning"])
        self.assertIn("not a fault", text["meaning"])
        self.assertEqual(text["action"], "Nothing to do.")

    def test_a_failed_comparison_points_at_preview_dependencies_not_a_log(self):
        text = explain_drift_row(
            row(
                comparison_available=False,
                comparison_unavailable_reason="error",
                in_sync=None,
            )
        )

        self.assertIn("other models were still attempted", text["meaning"])
        # The drift report only re-renders the cached preview, so re-running it does
        # nothing; the preview is what produces new figures.
        self.assertIn("Run Preview Dependencies again", text["action"])
        self.assertNotIn("log", text["action"])

    def test_no_exact_comparison_is_an_upper_bound_and_may_be_a_sibling_row(self):
        text = explain_drift_row(
            row(
                comparison_available=False,
                comparison_unavailable_reason="no_exact_comparison",
                in_sync=None,
            )
        )

        self.assertIn("an estimate, not a measured difference", text["meaning"])
        self.assertIn("another row for the same model", text["meaning"])
        # Re-running the preview fixes only one of the situations that read this way.
        self.assertIn("If every row reads this way", text["action"])

    def test_changes_and_removals_are_counted_with_the_right_grammar(self):
        one = explain_drift_row(row(pending_changes=1, pending_removes=1))
        many = explain_drift_row(row(pending_changes=128, pending_removes=2))

        self.assertIn("1 row is counted as new or different", one["meaning"])
        self.assertIn("1 removal is counted", one["meaning"])
        self.assertIn("128 rows are counted as new or different", many["meaning"])
        self.assertIn("2 removals are counted", many["meaning"])

    def test_nothing_claims_what_the_figures_do_not_establish(self):
        text = explain_drift_row(
            row(
                model="dcim.device",
                pending_changes=3,
                pending_removes=29,
                already_removed=12,
            )
        )
        said = text["meaning"] + " " + text["action"]

        # A counted removal is not a row confirmed to be in NetBox, and the next sync
        # does not necessarily create or update every counted row.
        for claim in (
            "NetBox holds",
            "no longer reports",
            "will be removed",
            "creates or updates them",
            "apply them",
            "mostly",
            "disabled",
            "nothing is left",
            "compared in full",
            "applied on an earlier",
            "upper bound",
            "stages the rows",
            "the most",
            "at least",
        ):
            self.assertNotIn(claim, said, claim)

    def test_a_non_device_removal_says_it_follows_the_removal_rules(self):
        text = explain_drift_row(row(pending_removes=5))

        self.assertIn("follows the sync's removal rules", text["action"])

    def test_already_removed_names_all_three_reasons_not_just_applied_earlier(self):
        none = explain_drift_row(row(pending_changes=3))
        some = explain_drift_row(row(pending_changes=3, already_removed=12))

        self.assertNotIn("more than this run stages", none["meaning"])
        self.assertIn("12 removals", some["meaning"])
        self.assertIn("more than this run stages", some["meaning"])
        for reason in ("an earlier removal", "never imported", "protection"):
            self.assertIn(reason, some["meaning"])

    def test_the_device_row_always_says_what_forward_rows_counts(self):
        for in_sync in (True, False):
            text = explain_drift_row(
                row(model="dcim.device", in_sync=in_sync, pending_removes=29)
            )
            self.assertIn(DEVICE_DENOMINATOR, text["meaning"])
        self.assertIn("bundled device maps", DEVICE_DENOMINATOR)
        self.assertIn("may be smaller", DEVICE_DENOMINATOR)

    def test_device_removals_point_at_scope_reconciliation(self):
        text = explain_drift_row(row(model="dcim.device", pending_removes=29))

        self.assertIn("29 removals are counted", text["meaning"])
        self.assertIn("not checked row by row against NetBox", text["meaning"])
        self.assertIn("Scope Reconciliation", text["action"])

    def test_every_column_has_a_description(self):
        columns = [column for column, _meaning in COLUMN_HELP]

        self.assertEqual(
            columns,
            [
                "Forward rows",
                "Upsert/change candidates",
                "Pending removals",
                "Estimated apply work",
                "Drift",
                "In sync",
            ],
        )
        for _column, meaning in COLUMN_HELP:
            self.assertTrue(meaning.strip())


class ReportCarriesTheExplanationTest(SimpleTestCase):
    def payload(self):
        return {
            "comparison_coverage": {"measured_models": 2, "total_models": 3},
            "model_results": [
                {
                    "model": "dcim.device",
                    "row_count": 3862,
                    "estimated_changes": 29,
                    "delete_count": 29,
                    "already_removed_count": 12,
                    "change_estimate_kind": "exact_comparison",
                },
                {
                    "model": "dcim.site",
                    "row_count": 99,
                    "estimated_changes": 0,
                    "delete_count": 0,
                    "change_estimate_kind": "exact_comparison",
                },
                {
                    "model": "ipam.vlan",
                    "row_count": 5,
                    "estimated_changes": 5,
                    "delete_count": 0,
                    "change_estimate_kind": "workload_upper_bound",
                },
            ],
        }

    def test_every_row_has_both_fields_and_no_figure_changed(self):
        report = compute_drift_report(self.payload())

        by_model = {r["model"]: r for r in report["models"]}
        for r in report["models"]:
            self.assertIn("meaning", r)
            self.assertIn("action", r)
        device = by_model["dcim.device"]
        self.assertEqual(device["forward_rows"], 3862)
        self.assertEqual(device["pending_removes"], 29)
        self.assertEqual(device["already_removed"], 12)
        self.assertEqual(by_model["dcim.site"]["meaning"], "")

    def test_the_customers_device_row_reads_in_plain_words(self):
        report = compute_drift_report(self.payload())

        device = [r for r in report["models"] if r["model"] == "dcim.device"][0]

        self.assertIn("29 removals are counted", device["meaning"])
        self.assertIn("12 removals", device["meaning"])
        self.assertIn(
            "may be smaller than your whole Forward network", device["meaning"]
        )


class DriftPageShowsItTest(SubinterfaceFixture):
    def test_the_page_has_the_note_and_the_column(self):
        user = get_user_model().objects.create_superuser(
            "root-d", "d@example.com", "pw"
        )
        self.client.force_login(user)
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id="123e4567-e89b-12d3-a456-426614174099",
            data={
                "generated_at": "t",
                "comparison_coverage": {"measured_models": 1, "total_models": 1},
                "model_results": [
                    {
                        "model": "dcim.device",
                        "row_count": 3862,
                        "estimated_changes": 29,
                        "delete_count": 29,
                        "already_removed_count": 12,
                        "change_estimate_kind": "exact_comparison",
                    }
                ],
            },
        )

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_drift_report",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "How to read this table")
        self.assertContains(response, "What it means")
        self.assertContains(response, "29 removals are counted")
        self.assertContains(response, "3862")
        # The old, contradicted wording must not be on the page anywhere.
        for gone in (
            "compared in full on every sync",
            "already applied on an earlier sync",
        ):
            self.assertNotContains(response, gone)
        # The note defines the same columns the table has.
        for column, _meaning in COLUMN_HELP:
            self.assertContains(response, f"<th>{column}</th>")
            self.assertContains(response, f"<dt>{column}</dt>")
