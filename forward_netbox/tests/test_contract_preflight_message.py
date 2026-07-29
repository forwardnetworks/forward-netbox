"""A failing contract preflight must say what to do about it.

A customer whose 32 enabled maps all reported `unresolved_full_commit` saw one
line — "correct the reported type-only contract issues" — against 32 rows that
said the same thing, with no indication of which maps, what was wrong, or what
would fix it. The sync applied nothing and the message explained none of it.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.health import _query_contract_preflight_message


def _issues(issue_type, *models):
    return [
        {"map_id": index, "model": model, "type": issue_type}
        for index, model in enumerate(models, start=1)
    ]


class ContractPreflightMessageTest(SimpleTestCase):
    def test_it_names_the_consequence(self):
        # The old message never said the models would be skipped, which is the
        # part that mattered: the sync looked like it ran.
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site")
        )
        self.assertIn("skipped", message)

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

    def test_it_gives_the_remediation_for_an_unresolved_commit(self):
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site")
        )
        self.assertIn("Publish Bundled Queries", message)
        # The folder caveat matters: publishing to the wrong directory creates
        # duplicates instead of fixing the binding.
        self.assertIn("folder", message)

    def test_distinct_problems_are_reported_separately(self):
        message = _query_contract_preflight_message(
            _issues("unresolved_full_commit", "dcim.site")
            + _issues("missing_diff_source_hash", "dcim.device")
        )
        self.assertIn("Publish Bundled Queries", message)
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
