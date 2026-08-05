"""A query ID with no commit is a complete binding; we must not resolve one.

Forward's full-execution endpoint takes `queryId` and resolves the latest commit
server-side. The plugin used to read a commit first anyway, by walking
`/nqe/queries/{id}/history` and matching committed source. Every way that walk
could fail - a repository reorganisation, a permissions gap, one lookup that did
not answer - produced `unresolved_full_commit`, and a single ineligible map makes
`_build_workload_jobs` plan zero jobs for the whole sync. Observed live: 32 of 32
enabled maps ID-bound with no commit, and five consecutive dead syncs.

These tests pin the decision itself: if a commit is not specified, it is not
required, and nothing goes looking for one.
"""

from unittest.mock import Mock

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.forward_api_impl import ForwardClient
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_fetch_execution import ForwardQueryFetcher
from forward_netbox.utilities.query_registry import QuerySpec
from forward_netbox.utilities.query_registry import resolve_query_specs_for_client

BUNDLED_SOURCE = """
@query
f(scope: List<String>) =
foreach value in scope
select {name: value, slug: value}
"""
DIFF_SOURCE = """
@query
f() =
foreach value in ["site"]
select {name: value, slug: value}
"""
QUERY_ID = "Q_site_query"
PINNED_COMMIT = "c" * 40


def _builtin_spec(**overrides):
    values = {
        "model_string": "dcim.site",
        "query_name": "Forward Sites",
        "query_id": QUERY_ID,
        "query_repository": "org",
        "resolved_query_path": "/CustomerRoot/forward_locations",
        "commit_id": None,
        "map_id": 7,
        "built_in": True,
        "contract_key": "forward_locations",
        "full_query_source": BUNDLED_SOURCE,
        "full_source_sha256": query_source_sha256(BUNDLED_SOURCE),
        "parameters": {"scope": []},
        "coalesce_fields": (("slug",), ("name",)),
    }
    values.update(overrides)
    return QuerySpec(**values)


def _customer_spec(**overrides):
    """A map the operator bound to their own query ID, storing no source."""

    values = {
        "model_string": "dcim.site",
        "query_name": "Operator Sites",
        "query_id": QUERY_ID,
        "query_repository": "org",
        "resolved_query_path": "/CustomerRoot/their_sites",
        "commit_id": None,
        "map_id": 9,
        "built_in": False,
        "parameters": {},
        "coalesce_fields": (("slug",), ("name",)),
    }
    values.update(overrides)
    return QuerySpec(**values)


def _client(*, committed_source=BUNDLED_SOURCE):
    client = Mock()
    client.get_nqe_query_history.return_value = [
        {"id": PINNED_COMMIT, "path": "/CustomerRoot/forward_locations"}
    ]

    def committed(**kwargs):
        commit_id = kwargs.get("commit_id")
        source = DIFF_SOURCE if commit_id == "diff-commit" else committed_source
        return {
            "queryId": QUERY_ID,
            "commitId": commit_id,
            "sourceCode": source,
        }

    client.get_committed_nqe_query.side_effect = committed
    return client


class QueryIdWithoutCommitTest(SimpleTestCase):
    def test_no_commit_is_read_for_an_id_bound_map(self):
        client = _client()

        resolved = resolve_query_specs_for_client([_builtin_spec()], client)

        self.assertEqual(len(resolved), 1)
        self.assertIsNone(resolved[0].commit_id)
        client.get_nqe_query_history.assert_not_called()
        client.get_committed_nqe_query.assert_not_called()
        client.get_nqe_repository_query_index.assert_not_called()

    def test_head_is_treated_as_no_commit_rather_than_a_pin(self):
        client = _client()

        resolved = resolve_query_specs_for_client(
            [_builtin_spec(commit_id="head")],
            client,
        )

        self.assertIsNone(resolved[0].commit_id)
        client.get_nqe_query_history.assert_not_called()

    def test_the_contract_runs_it_and_says_it_is_at_head(self):
        contract = resolve_execution_contract(
            _builtin_spec(),
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "eligible")
        self.assertTrue(contract.full_unpinned_head)
        self.assertEqual(contract.full_revision.commit_id, "")

    def test_bundled_source_still_verifies_declarations_and_parameters(self):
        # The protection that survives without Forward's history: a built-in map
        # ships its `.nqe`, so its declaration is parsed and matched locally.
        self.assertTrue(
            resolve_execution_contract(
                _builtin_spec(),
                effective_parameters={"scope": ["device-a"]},
            ).full_revision.source_verified
        )

        refused = resolve_execution_contract(
            _builtin_spec(),
            effective_parameters={"scope": [], "device_tag_include_match": "any"},
        )

        self.assertFalse(refused.full_eligible)
        self.assertEqual(refused.full_reason_code, "unsupported_full_parameters")

    def test_the_execution_payload_omits_commit_id_entirely(self):
        payload = ForwardClient._nqe_async_execution_payload(
            Mock(),
            query_id=QUERY_ID,
            commit_id=None,
            parameters={"scope": []},
        )

        self.assertNotIn("commitId", payload)
        self.assertEqual(payload["queryId"], QUERY_ID)

    def test_a_stored_commit_is_still_honoured_exactly(self):
        client = _client()

        resolved = resolve_query_specs_for_client(
            [_builtin_spec(commit_id=PINNED_COMMIT)],
            client,
        )

        self.assertEqual(resolved[0].commit_id, PINNED_COMMIT)
        contract = resolve_execution_contract(
            resolved[0],
            effective_parameters={"scope": ["device-a"]},
        )
        self.assertFalse(contract.full_unpinned_head)
        payload = ForwardClient._nqe_async_execution_payload(
            Mock(),
            query_id=QUERY_ID,
            commit_id=contract.full_revision.commit_id,
            parameters={"scope": []},
        )
        self.assertEqual(payload["commitId"], PINNED_COMMIT)

    def test_a_path_bound_builtin_map_still_resolves_a_verified_commit(self):
        # A path is not an identity: the folder may now hold a different query,
        # so this binding still has to verify a revision before executing it.
        client = _client()
        client.get_nqe_repository_query_index.return_value = {
            "by_path": {
                "/CustomerRoot/forward_locations": {
                    "queryId": QUERY_ID,
                    "path": "/CustomerRoot/forward_locations",
                }
            }
        }
        path_bound = _builtin_spec(
            query_id=None,
            query_path="/CustomerRoot/forward_locations",
            resolved_query_path=None,
        )

        resolved = resolve_query_specs_for_client([path_bound], client)

        client.get_nqe_query_history.assert_called_once_with(QUERY_ID)
        self.assertEqual(resolved[0].commit_id, PINNED_COMMIT)


class CustomerQueryWithoutLocalSourceTest(SimpleTestCase):
    """An ID-bound map whose query this plugin does not hold a copy of."""

    def test_it_runs_and_is_reported_as_executed_on_trust(self):
        client = _client()

        resolved = resolve_query_specs_for_client([_customer_spec()], client)
        contract = resolve_execution_contract(
            resolved[0],
            effective_parameters={},
        )

        client.get_committed_nqe_query.assert_not_called()
        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "remote_source_only")

    def test_the_preflight_says_out_loud_that_it_runs_unverified(self):
        contract = resolve_execution_contract(
            _customer_spec(),
            effective_parameters={},
        )
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=None, client=None, logger_=logger)

        fetcher._report_contract_compatibility_issues("dcim.site", [contract])

        self.assertTrue(
            any(
                "holds no copy of" in str(call) and "Operator Sites" in str(call)
                for call in logger.log_info.call_args_list
            ),
            logger.log_info.call_args_list,
        )
        # Said, not blamed: it must not read as a rejected map.
        self.assertFalse(
            any(
                "rejected map(s)" in str(call)
                for call in logger.log_warning.call_args_list
            )
        )

    def test_a_map_that_stores_its_own_query_is_still_verified(self):
        contract = resolve_execution_contract(
            _customer_spec(
                full_query_source=BUNDLED_SOURCE,
                full_source_sha256=query_source_sha256("something else"),
                parameters={"scope": []},
            ),
            effective_parameters={"scope": []},
        )

        self.assertFalse(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "unverified_full_source")


class DiffExecutionIsUntouchedTest(SimpleTestCase):
    """A diff names its own revisions, so nothing about it is relaxed."""

    def _diff_spec(self, **overrides):
        values = {
            "diff_commit_id": "diff-commit",
            "diff_source_sha256": query_source_sha256(DIFF_SOURCE),
        }
        values.update(overrides)
        return _builtin_spec(**values)

    def test_the_diff_side_is_still_hydrated_when_the_full_side_is_unpinned(self):
        client = _client()

        resolved = resolve_query_specs_for_client([self._diff_spec()], client)

        self.assertIsNone(resolved[0].commit_id)
        self.assertEqual(resolved[0].diff_query_source, DIFF_SOURCE)
        client.get_committed_nqe_query.assert_called_once()
        self.assertEqual(
            client.get_committed_nqe_query.call_args.kwargs["commit_id"],
            "diff-commit",
        )

    def test_an_unpinned_full_side_still_permits_a_diff(self):
        client = _client()
        resolved = resolve_query_specs_for_client([self._diff_spec()], client)

        contract = resolve_execution_contract(
            resolved[0],
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertEqual(contract.diff_reason_code, "eligible")
        self.assertTrue(contract.diff_eligible)

    def test_a_diff_without_a_commit_is_still_refused(self):
        contract = resolve_execution_contract(
            _builtin_spec(),
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertEqual(contract.diff_reason_code, "missing_diff_commit")
        self.assertFalse(contract.diff_eligible)

    def test_a_diff_revision_that_declares_parameters_is_still_refused(self):
        client = _client(committed_source=BUNDLED_SOURCE)
        client.get_committed_nqe_query.side_effect = lambda **kwargs: {
            "queryId": QUERY_ID,
            "commitId": kwargs.get("commit_id"),
            "sourceCode": BUNDLED_SOURCE,
        }
        spec = self._diff_spec(diff_source_sha256=query_source_sha256(BUNDLED_SOURCE))

        resolved = resolve_query_specs_for_client([spec], client)
        contract = resolve_execution_contract(
            resolved[0],
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertEqual(contract.diff_reason_code, "nonempty_diff_declarations")
        self.assertFalse(contract.diff_eligible)


class DriftedQueryFailsPerModelTest(SimpleTestCase):
    """Drift now surfaces at execution. It has to name the map when it does."""

    def _fetcher(self):
        return ForwardQueryFetcher(sync=None, client=None, logger_=None)

    def test_a_missing_field_names_the_map_and_the_revision(self):
        contract = resolve_execution_contract(
            _builtin_spec(),
            effective_parameters={"scope": ["device-a"]},
        )

        with self.assertRaises(ForwardQueryError) as raised:
            self._fetcher().validate_rows(
                "dcim.site",
                [{"name": "site-a"}],
                [],
                [["slug"]],
                contract=contract,
            )

        message = str(raised.exception)
        self.assertIn("Forward Sites", message)
        self.assertIn("[7]", message)
        self.assertIn("unpinned head", message)

    def test_a_parameter_rejection_names_the_map_and_says_what_to_do(self):
        exc = Exception(
            "Forward API request failed with HTTP 400: NQE_RUNTIME_ERROR - "
            "Provided argument, 'device_tag_include_match' is not a parameter "
            "to the given query."
        )

        message = self._fetcher()._failure_message(
            "dcim.site",
            _builtin_spec(),
            exc,
        )

        self.assertIn("Forward Sites", message)
        self.assertIn(QUERY_ID, message)
        self.assertIn("Forward's latest commit", message)
        self.assertIn("no longer declares them", message)

    def test_a_pinned_map_is_described_as_pinned(self):
        message = self._fetcher()._failure_message(
            "dcim.site",
            _builtin_spec(commit_id=PINNED_COMMIT),
            Exception("boom"),
        )

        self.assertIn("a pinned commit", message)

    def test_a_raw_query_map_gets_no_binding_sentence(self):
        raw_spec = QuerySpec(
            model_string="dcim.site",
            query_name="Raw Sites",
            query=BUNDLED_SOURCE,
            parameters={"scope": []},
        )

        message = self._fetcher()._failure_message("dcim.site", raw_spec, Exception())

        self.assertEqual(message, "Exception.")
