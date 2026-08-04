"""A failing contract preflight must say what to do about it.

A customer whose 32 enabled maps all reported `unresolved_full_commit` saw one
line — "correct the reported type-only contract issues" — against 32 rows that
said the same thing, with no indication of which maps, what was wrong, or what
would fix it. The sync applied nothing and the message explained none of it.
"""

from types import SimpleNamespace

from django.test import SimpleTestCase

from forward_netbox.utilities.health import _fetch_failure_check
from forward_netbox.utilities.health import _persisted_query_contract_preflight
from forward_netbox.utilities.health import _query_contract_preflight_message
from forward_netbox.utilities.health import _query_contract_preflight_status


def _issues(issue_type, *models):
    return [
        {"map_id": index, "model": model, "type": issue_type}
        for index, model in enumerate(models, start=1)
    ]


class ContractPreflightMessageTest(SimpleTestCase):
    def test_a_missing_stored_commit_is_not_reported_as_a_failure(self):
        """Proven by a customer bundle, not by reasoning.

        32 of 32 maps reported `unresolved_full_commit` both while the sync
        applied nothing AND after the same sync applied 24,748 changes. A signal
        identical when broken and when healthy cannot gate a sync. Reporting it
        as a failure is what sent the investigation down the wrong path.
        """
        issues = _issues("unresolved_full_commit", "dcim.site")
        self.assertEqual(_query_contract_preflight_status(issues), "info")
        self.assertIn("informational", _query_contract_preflight_message(issues))

    def test_a_genuinely_blocking_issue_still_fails(self):
        issues = _issues("missing_diff_source_hash", "dcim.site")
        self.assertEqual(_query_contract_preflight_status(issues), "fail")
        self.assertIn("skip those models", _query_contract_preflight_message(issues))

    def test_one_blocking_issue_among_many_benign_ones_still_fails(self):
        issues = _issues(
            "unresolved_full_commit", *[f"a.m{i}" for i in range(30)]
        ) + _issues("identical_full_diff_commit", "dcim.site")
        self.assertEqual(_query_contract_preflight_status(issues), "fail")

    def test_it_names_the_affected_models(self):
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site", "dcim.device")
        )
        self.assertIn("dcim.site", message)
        self.assertIn("dcim.device", message)

    def test_it_summarises_rather_than_listing_every_model(self):
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", *[f"app.model{i}" for i in range(32)])
        )
        self.assertIn("32 map(s)", message)
        self.assertIn("+29 more", message)

    def test_a_missing_stored_commit_explains_why_it_is_normal(self):
        # It must not tell an operator to go fix something that is not broken.
        # Only path-bound maps reach this issue now; an ID-bound map needs no
        # commit at all and is never listed.
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site")
        )
        self.assertIn("at sync time", message)
        self.assertIn("path-bound", message.lower())
        self.assertNotIn("Publish Bundled Queries", message)

    def test_a_blocking_message_states_the_consequence_and_the_remedy(self):
        message = _query_contract_preflight_message(
            _issues("missing_diff_source_hash", "dcim.site")
        )
        self.assertIn("will not sync", message)
        self.assertIn("Refresh Query IDs", message)
        self.assertIn("org query repository", message)

    def test_distinct_problems_are_reported_separately(self):
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site")
            + _issues("missing_diff_source_hash", "dcim.device")
        )
        self.assertIn("source is attested", message)

    def test_an_identical_full_and_diff_commit_is_explained(self):
        message = _query_contract_preflight_message(
            _issues("identical_full_diff_commit", "dcim.site")
        )
        self.assertIn("re-execute the full query", message)

    def test_an_unknown_issue_type_still_produces_guidance(self):
        message = _query_contract_preflight_message(
            [{"map_id": 1, "model": "dcim.site", "type": "something_new"}]
        )
        self.assertIn("dcim.site", message)
        self.assertIn("persisted execution contract", message)

    def test_a_missing_model_name_does_not_break_the_summary(self):
        message = _query_contract_preflight_message(
            [{"map_id": 1, "model": "", "type": "unresolved_full_commit"}]
        )
        self.assertIn("1 map(s)", message)


def _map(pk, model_string, *, query_id="", query_path="", commit_id=""):
    return SimpleNamespace(
        pk=pk,
        model_string=model_string,
        query_id=query_id,
        query_path=query_path,
        commit_id=commit_id,
        diff_commit_id="",
        diff_source_sha256="",
        execution_mode=(
            "query_id" if query_id else ("query_path" if query_path else "query")
        ),
    )


class PersistedPreflightScopeTest(SimpleTestCase):
    """A query-ID map needs no commit, so an empty one is not an issue."""

    def test_query_id_maps_without_a_commit_are_not_reported(self):
        maps = [_map(index, f"app.model{index}", query_id="Q") for index in range(32)]

        preflight = _persisted_query_contract_preflight(maps)

        self.assertEqual(preflight["issues"], [])
        self.assertTrue(preflight["consistent"])

    def test_path_bound_maps_without_a_commit_are_still_reported(self):
        preflight = _persisted_query_contract_preflight(
            [_map(1, "dcim.site", query_path="/Forward/sites")]
        )

        self.assertEqual(
            [issue["type"] for issue in preflight["issues"]],
            ["unresolved_full_commit"],
        )


class FetchFailureCheckTest(SimpleTestCase):
    """Severity follows what the run did, not what the maps look like at rest.

    The health page reported a run that fetched nothing as one `warn` row
    saying "1 issue(s)" next to an "informational" contract line, so five dead
    runs read as healthy.
    """

    @staticmethod
    def _ingestion(rows):
        return SimpleNamespace(pk=7, model_results=rows)

    def test_every_model_failing_is_blocking_and_names_the_consequence(self):
        rows = [
            {"model": f"app.model{index}", "failure_count": 1} for index in range(32)
        ]

        check = _fetch_failure_check(self._ingestion(rows))

        self.assertEqual(check["status"], "fail")
        self.assertIn("applied nothing", check["message"])
        self.assertIn("nothing will sync", check["message"])
        self.assertIn("Refresh Query IDs", check["message"])
        self.assertIn("unresolved_full_commit", check["message"])
        self.assertIn("unsupported_full_parameters", check["message"])

    def test_a_partial_failure_warns_and_names_the_models(self):
        rows = [
            {"model": "dcim.site", "failure_count": 1},
            {"model": "dcim.device", "failure_count": 0},
            {"model": "dcim.interface", "failure_count": 0},
        ]

        check = _fetch_failure_check(self._ingestion(rows))

        self.assertEqual(check["status"], "warn")
        self.assertIn("dcim.site", check["message"])
        self.assertIn("1 of 3", check["message"])

    def test_a_clean_run_reports_nothing_at_all(self):
        rows = [
            {"model": "dcim.site", "failure_count": 0},
            {"model": "dcim.device", "failure_count": 0},
        ]

        self.assertIsNone(_fetch_failure_check(self._ingestion(rows)))

    def test_no_ingestion_and_no_rows_report_nothing(self):
        self.assertIsNone(_fetch_failure_check(None))
        self.assertIsNone(_fetch_failure_check(self._ingestion([])))
