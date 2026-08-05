from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.exceptions import ForwardFetchBudgetExceededError
from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.branch_budget import build_branch_plan
from forward_netbox.utilities.query_execution_contract import canonical_sha256
from forward_netbox.utilities.query_execution_contract import (
    compatible_baseline_evidence,
)
from forward_netbox.utilities.query_execution_contract import declared_query_parameters
from forward_netbox.utilities.query_execution_contract import diff_artifact_key
from forward_netbox.utilities.query_execution_contract import DiffArtifact
from forward_netbox.utilities.query_execution_contract import DiffArtifactKey
from forward_netbox.utilities.query_execution_contract import DiffArtifactStore
from forward_netbox.utilities.query_execution_contract import normalized_row_multiset
from forward_netbox.utilities.query_execution_contract import normalized_row_sets_match
from forward_netbox.utilities.query_execution_contract import query_source_sha256
from forward_netbox.utilities.query_execution_contract import resolve_execution_contract
from forward_netbox.utilities.query_execution_contract import (
    resolve_model_execution_contract,
)
from forward_netbox.utilities.query_execution_contract import (
    scope_membership_fingerprint,
)
from forward_netbox.utilities.query_fetch import ForwardQueryContext
from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
from forward_netbox.utilities.query_fetch_execution import DIFF_BUDGET_FALLBACK_REASON
from forward_netbox.utilities.query_fetch_execution import (
    DIFF_CIRCUIT_OPEN_FALLBACK_REASON,
)
from forward_netbox.utilities.query_registry import QuerySpec

FULL_SOURCE = """
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


def _context(**overrides):
    values = {
        "network_id": "network",
        "snapshot_selector": "latestProcessed",
        "snapshot_id": "snapshot-after",
        "device_tag_include_tags": ["Include"],
        "device_tag_exclude_tags": ["Exclude"],
        "device_tag_include_match": "all",
        "device_tag_prune_out_of_scope": False,
        "sync_device_tags": ["Feature"],
        "apply_device_scope_tags": False,
        "sync_endpoints": False,
        "sync_generic_endpoints": False,
        "scope_endpoints_by_include_tags": False,
        "scoped_device_names": {"device-a"},
        "scoped_site_names": {"site-a"},
        "scoped_matched_tags": {"device-a": ["Include"]},
    }
    values.update(overrides)
    return ForwardQueryContext(**values)


def _eligible_spec(**overrides):
    values = {
        "model_string": "dcim.site",
        "query_name": "Forward Sites",
        "query_id": "Q_sites",
        "resolved_query_path": "/Forward/forward_locations",
        "commit_id": "full-commit",
        "map_id": 1,
        "map_weight": 100,
        "built_in": True,
        "contract_key": "forward_locations",
        "full_query_source": FULL_SOURCE,
        "full_source_sha256": query_source_sha256(FULL_SOURCE),
        "diff_commit_id": "diff-commit",
        "diff_query_source": DIFF_SOURCE,
        "diff_source_sha256": query_source_sha256(DIFF_SOURCE),
        "parameters": {"scope": []},
        "coalesce_fields": (("slug",), ("name",)),
    }
    values.update(overrides)
    return QuerySpec(**values)


def _path_bound_spec(**overrides):
    """The binding that still has to resolve and verify a commit before it runs.

    A repository path is not an identity - the folder can come to hold a
    different query - so a commit reached through a path is only executed after
    its source is verified. A direct query ID is the opposite case: it names the
    query, so Forward resolving its latest commit is the whole of the binding.
    """

    values = {
        "query_id": None,
        "resolved_query_id": "Q_sites",
        "query_path": "/Forward/forward_locations",
        "query_repository": "org",
        "resolved_query_path": "/Forward/forward_locations",
    }
    values.update(overrides)
    return replace(_eligible_spec(), **values)


def _contract(spec=None, *, effective_parameters=None):
    return resolve_execution_contract(
        spec or _eligible_spec(),
        effective_parameters=(
            {"scope": ["device-a"]}
            if effective_parameters is None
            else effective_parameters
        ),
    )


def _model_contract(specs=None, *, context=None, parameters=None):
    specs = list(specs or [_eligible_spec()])
    contracts = [
        _contract(
            spec,
            effective_parameters=(
                parameters[index] if isinstance(parameters, list) else parameters
            ),
        )
        for index, spec in enumerate(specs)
    ]
    return resolve_model_execution_contract(
        specs[0].model_string,
        contracts,
        context=context or _context(),
    )


def _baseline(model_contract, *, snapshot_id="snapshot-before"):
    row = {
        "model": model_contract.model_string,
        "execution_contract_fingerprint": (
            model_contract.execution_contract_fingerprint
        ),
        "map_set_fingerprint": model_contract.map_set_fingerprint,
        "scope_config_fingerprint": model_contract.scope_config_fingerprint,
        "scope_membership_fingerprint": canonical_sha256(
            {"snapshot": snapshot_id, "members": ["device-before"]}
        ),
    }
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        model_results=[dict(row) for _contract in model_contract.maps],
    )


class ResolvedExecutionContractTest(SimpleTestCase):
    def test_implicit_primary_key_query_is_verified_parameterless(self):
        source = """
helper(device: Device) =
  foreach interface in device.interfaces
  select {device: device.name, interface: interface.name};

@primaryKey(device, interface)
foreach device in network.devices
foreach row in helper(device)
select row
"""

        self.assertEqual(declared_query_parameters(source), ())

    def test_implicit_query_without_primary_key_is_parameterless(self):
        self.assertEqual(
            declared_query_parameters(
                """
foreach device in network.devices
select {device: device.name}
"""
            ),
            (),
        )
        self.assertIsNone(
            declared_query_parameters(
                """
@primaryKey(device)
foreach device in network.devices
select {device: device.name}
@primaryKey(endpoint)
foreach endpoint in network.endpoints
select {endpoint: endpoint.name}
"""
            )
        )

    def test_normalized_parity_fixture_is_order_independent_and_duplicate_sensitive(
        self,
    ):
        expected = [
            {"slug": "alpha", "nested": {"enabled": True, "weight": 1}},
            {"slug": "beta", "nested": {"enabled": False, "weight": 2}},
        ]
        reordered = [
            {"nested": {"weight": 2, "enabled": False}, "slug": "beta"},
            {"nested": {"weight": 1, "enabled": True}, "slug": "alpha"},
        ]

        self.assertTrue(normalized_row_sets_match(expected, reordered))
        self.assertEqual(
            normalized_row_multiset(expected),
            normalized_row_multiset(reordered),
        )
        self.assertFalse(
            normalized_row_sets_match(expected, [*reordered, reordered[0]])
        )
        self.assertFalse(
            normalized_row_sets_match(
                expected,
                [
                    *reordered[:-1],
                    {"nested": {"weight": 3, "enabled": True}, "slug": "alpha"},
                ],
            )
        )

    def test_parameterized_full_and_parameterless_diff_are_separate_revisions(self):
        contract = _contract()

        self.assertTrue(contract.diff_eligible)
        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.reason_code, "eligible")
        self.assertEqual(contract.full_reason_code, "eligible")
        self.assertEqual(contract.diff_reason_code, "eligible")
        self.assertEqual(contract.full_revision.query_id, "Q_sites")
        self.assertEqual(contract.full_revision.commit_id, "full-commit")
        self.assertEqual(
            [
                parameter.name
                for parameter in contract.full_revision.declared_parameters
            ],
            ["scope"],
        )
        self.assertEqual(
            dict(contract.full_effective_parameters),
            {"scope": ["device-a"]},
        )
        self.assertEqual(contract.diff_revision.query_id, "Q_sites")
        self.assertEqual(contract.diff_revision.commit_id, "diff-commit")
        self.assertEqual(contract.diff_revision.declared_parameters, ())

    def test_fail_closed_contract_matrix(self):
        cases = {
            "custom": (
                replace(
                    _eligible_spec(),
                    built_in=False,
                    full_source_sha256="",
                ),
                "unverified_full_source",
                False,
            ),
            "raw": (
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Raw Sites",
                    query=FULL_SOURCE,
                    parameters={"scope": []},
                ),
                "raw_query",
                True,
            ),
            "unresolved_id": (
                QuerySpec(
                    model_string="dcim.site",
                    query_name="Path Sites",
                    query_repository="org",
                    query_path="/Forward/sites",
                    commit_id="full-commit",
                    built_in=True,
                    contract_key="forward_locations",
                    full_query_source=FULL_SOURCE,
                    full_source_sha256=query_source_sha256(FULL_SOURCE),
                    diff_commit_id="diff-commit",
                    diff_query_source=DIFF_SOURCE,
                    diff_source_sha256=query_source_sha256(DIFF_SOURCE),
                    parameters={"scope": []},
                ),
                "unresolved_query_id",
                False,
            ),
            "unsafe_contributor_ownership": (
                replace(
                    _eligible_spec(),
                    diff_ownership_mode="unsafe_contributor_reduction",
                ),
                "unsupported_diff_ownership",
                True,
            ),
            "unpinned_full_path_binding": (
                _path_bound_spec(commit_id=None),
                "unresolved_full_commit",
                False,
            ),
            "legacy_pinned_parameterized": (
                replace(_eligible_spec(), diff_commit_id=None),
                "missing_diff_commit",
                True,
            ),
            "same_commit": (
                replace(_eligible_spec(), diff_commit_id="full-commit"),
                "identical_full_diff_commit",
                True,
            ),
            "full_hash_mismatch": (
                replace(_eligible_spec(), full_source_sha256="0" * 64),
                "unverified_full_source",
                False,
            ),
            "unverified_full_declarations": (
                replace(
                    _eligible_spec(),
                    full_query_source="""\
@query
f(x: String)
select {name: x}
""",
                    full_source_sha256=query_source_sha256(
                        """
@query
f(x: String)
select {name: x}
"""
                    ),
                ),
                "unverified_full_declarations",
                False,
            ),
            "diff_hash_mismatch": (
                replace(_eligible_spec(), diff_source_sha256="0" * 64),
                "unverified_diff_source",
                True,
            ),
            "nonempty_diff_declarations": (
                replace(
                    _eligible_spec(),
                    diff_query_source=FULL_SOURCE,
                    diff_source_sha256=query_source_sha256(FULL_SOURCE),
                ),
                "nonempty_diff_declarations",
                True,
            ),
            "unsupported_parameter": (
                _eligible_spec(),
                "unsupported_full_parameters",
                False,
            ),
            "oversupplied_parameters": (
                _eligible_spec(),
                "unsupported_full_parameters",
                False,
            ),
        }
        for name, (spec, reason, full_eligible) in cases.items():
            effective_parameters = (
                {"undeclared": True}
                if name == "unsupported_parameter"
                else (
                    {"scope": ["device-a"], "extra": "value"}
                    if name == "oversupplied_parameters"
                    else {"scope": []} if "scope" in spec.parameters else {}
                )
            )
            with self.subTest(name=name):
                contract = _contract(
                    spec,
                    effective_parameters=effective_parameters,
                )
                self.assertEqual(contract.full_eligible, full_eligible)
                self.assertFalse(contract.diff_eligible)
                self.assertEqual(contract.reason_code, reason)

    def test_full_contract_unverified_full_source_is_closed(self):
        contract = _contract(
            replace(_eligible_spec(), full_source_sha256="0" * 64),
            effective_parameters={"scope": []},
        )

        self.assertEqual(contract.full_reason_code, "unverified_full_source")
        self.assertFalse(contract.full_eligible)

    def test_raw_full_query_rejects_supplied_vs_declared_mismatch(self):
        spec = QuerySpec(
            model_string="dcim.site",
            query_name="Raw Sites",
            query=FULL_SOURCE,
            parameters={"scope": []},
        )

        contract = _contract(
            spec,
            effective_parameters={"scope": [], "extra": True},
        )

        self.assertFalse(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "unsupported_full_parameters")

    def test_path_bound_map_without_full_commit_is_unresolved_full(self):
        contract = _contract(
            _path_bound_spec(commit_id=""),
            effective_parameters={"scope": []},
        )

        self.assertEqual(contract.full_reason_code, "unresolved_full_commit")
        self.assertFalse(contract.full_eligible)

    def test_id_bound_map_without_full_commit_runs_at_forward_latest(self):
        # If a commit is not specified it is not required: Forward resolves the
        # latest commit for a query ID server-side. Demanding one refused every
        # such map, and one refused map plans zero jobs for the whole sync.
        contract = _contract(
            replace(_eligible_spec(), commit_id=""),
            effective_parameters={"scope": []},
        )

        self.assertEqual(contract.full_reason_code, "eligible")
        self.assertTrue(contract.full_eligible)
        self.assertTrue(contract.full_unpinned_head)

    def test_full_parameter_set_must_match_exactly(self):
        contract = _contract(
            _eligible_spec(),
            effective_parameters={"scope": ["device-a"], "extra": "value"},
        )

        self.assertEqual(contract.full_reason_code, "unsupported_full_parameters")
        self.assertEqual(contract.reason_code, "unsupported_full_parameters")
        self.assertEqual(contract.diff_reason_code, "eligible")

        contract = _contract(
            _eligible_spec(),
            effective_parameters={},
        )

        self.assertEqual(contract.full_reason_code, "unsupported_full_parameters")
        self.assertEqual(contract.reason_code, "unsupported_full_parameters")

    def test_full_contract_parameter_mismatch_closes_full_and_diff(self):
        contract = _contract(
            _eligible_spec(),
            effective_parameters={"scope": ["device-a"], "extra": "value"},
        )

        self.assertFalse(contract.full_eligible)
        self.assertEqual(contract.full_reason_code, "unsupported_full_parameters")
        self.assertFalse(contract.diff_eligible)
        self.assertEqual(contract.reason_code, "unsupported_full_parameters")
        self.assertIsNotNone(contract.diff_revision)

    def test_alias_and_rules_variants_require_data_file_hashes(self):
        for variant, data_file in (
            ("aliases", "netbox_device_type_aliases"),
            ("rules", "netbox_feature_tag_rules"),
        ):
            with self.subTest(variant=variant):
                spec = replace(
                    _eligible_spec(),
                    variant=variant,
                    required_data_files=(data_file,),
                )
                self.assertEqual(
                    _contract(spec).reason_code,
                    "missing_variant_data_hash",
                )
                verified = replace(
                    spec,
                    data_file_hashes={data_file: "a" * 64},
                )
                self.assertTrue(_contract(verified).diff_eligible)

    def test_parameterized_diff_revision_is_not_diff_eligible(self):
        contract = _contract(
            replace(
                _eligible_spec(),
                diff_query_source=FULL_SOURCE,
                diff_source_sha256=query_source_sha256(FULL_SOURCE),
            ),
            effective_parameters={"scope": ["device-a"]},
        )

        self.assertTrue(contract.full_eligible)
        self.assertEqual(contract.diff_reason_code, "nonempty_diff_declarations")
        self.assertFalse(contract.diff_eligible)
        self.assertEqual(contract.reason_code, "nonempty_diff_declarations")

    def test_parameterless_full_revision_fails_closed_before_forward_http(self):
        parameterless_full = replace(
            _eligible_spec(),
            full_query_source=DIFF_SOURCE,
            full_source_sha256=query_source_sha256(DIFF_SOURCE),
            diff_commit_id=None,
            diff_query_source=None,
            diff_source_sha256="",
        )
        contract = _contract(
            parameterless_full,
            effective_parameters={"scope": ["device-a"]},
        )
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(
                pk=1,
                parameters={},
                source=SimpleNamespace(parameters={}),
                latest_baseline_ingestion=Mock(return_value=None),
            ),
            client=client,
            logger_=Mock(),
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Full execution is not allowed.*unsupported_full_parameters",
        ):
            fetcher._fetch_spec_rows(
                "dcim.site",
                contract,
                baseline=None,
                context=_context(),
                coalesce_fields=[["slug"], ["name"]],
            )

        client.run_nqe_query.assert_not_called()
        client.run_nqe_diff.assert_not_called()

    def test_parameter_declaring_diff_revision_falls_closed_to_full(self):
        parameterized_diff = replace(
            _eligible_spec(),
            diff_query_source=FULL_SOURCE,
            diff_source_sha256=query_source_sha256(FULL_SOURCE),
        )
        contract = _contract(
            parameterized_diff,
            effective_parameters={"scope": ["device-a"]},
        )
        client = Mock()
        client.run_nqe_query.return_value = [{"name": "site-a", "slug": "site-a"}]
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(
                pk=1,
                parameters={},
                source=SimpleNamespace(parameters={}),
                latest_baseline_ingestion=Mock(return_value=None),
            ),
            client=client,
            logger_=Mock(),
        )

        rows, deletes, sync_mode, metadata = fetcher._fetch_spec_rows(
            "dcim.site",
            contract,
            baseline=SimpleNamespace(snapshot_id="snapshot-before"),
            context=_context(),
            coalesce_fields=[["slug"], ["name"]],
            return_fetch_meta=True,
        )

        self.assertEqual(rows, [{"name": "site-a", "slug": "site-a"}])
        self.assertEqual(deletes, [])
        self.assertEqual(sync_mode, "full")
        self.assertEqual(
            metadata["fetch_parameters"]["fallback_reason"],
            "nonempty_diff_declarations",
        )
        client.run_nqe_diff.assert_not_called()
        client.run_nqe_query.assert_called_once()

    def test_direct_diff_helper_rejects_unverified_contract_before_http(self):
        contract = _contract(
            replace(
                _eligible_spec(),
                diff_query_source=FULL_SOURCE,
                diff_source_sha256=query_source_sha256(FULL_SOURCE),
            ),
            effective_parameters={"scope": ["device-a"]},
        )
        client = Mock()
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(pk=1),
            client=client,
            logger_=Mock(),
        )

        with self.assertRaisesRegex(
            ForwardQueryError,
            "Diff execution is not allowed.*nonempty_diff_declarations",
        ):
            fetcher._run_nqe_diff(
                spec=contract.spec,
                contract=contract,
                context=_context(),
                before_snapshot_id="snapshot-before",
            )

        client.run_nqe_diff.assert_not_called()

    def test_unpinned_path_binding_blocks_all_workload_jobs_in_preflight(self):
        unpinned = _path_bound_spec(commit_id="")
        eligible_other = replace(
            _eligible_spec(),
            model_string="dcim.location",
            query_name="Forward Locations",
            query_id="Q_locations",
            map_id=2,
            contract_key="forward_locations_secondary",
        )
        sync = SimpleNamespace(
            pk=1,
            parameters={},
            source=SimpleNamespace(parameters={}),
        )
        client = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=client, logger_=Mock())
        fetcher._drop_unavailable_integration_models = Mock(
            return_value=["dcim.site", "dcim.location"]
        )
        fetcher._resolve_specs_for_models = Mock(
            return_value=(
                {
                    "dcim.site": [unpinned],
                    "dcim.location": [eligible_other],
                },
                {},
            )
        )
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)

        jobs = fetcher._build_workload_jobs(
            _context(maps=[]),
            model_strings=["dcim.site", "dcim.location"],
        )

        self.assertEqual(jobs, [])
        self.assertIn("dcim.site", fetcher._failed_model_results)
        self.assertEqual(
            fetcher._failed_model_results["dcim.site"].sync_mode,
            "planning",
        )
        client.run_nqe_query.assert_not_called()
        client.run_nqe_diff.assert_not_called()

    def test_an_unpinned_id_binding_does_not_block_the_run(self):
        # The same shape that used to empty an entire sync. One map bound to a
        # query ID with no commit must plan its job, not refuse the run.
        sync = SimpleNamespace(
            pk=1,
            parameters={},
            source=SimpleNamespace(parameters={}),
        )
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=client, logger_=logger)
        fetcher._drop_unavailable_integration_models = Mock(return_value=["dcim.site"])
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.site": [replace(_eligible_spec(), commit_id="")]}, {})
        )
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._incremental_baseline_for_specs = Mock(return_value=None)
        fetcher._query_parameters_for_scope = Mock(return_value={"scope": []})

        jobs = fetcher._build_workload_jobs(
            _context(maps=[]),
            model_strings=["dcim.site"],
        )

        self.assertEqual(len(jobs), 1)
        self.assertEqual(fetcher._failed_model_results, {})
        self.assertFalse(
            any(
                "blocked all Forward workload execution" in str(call)
                for call in logger.log_warning.call_args_list
            )
        )

    def test_preflight_surfaces_each_unsafe_full_contract_before_forward_http(self):
        cases = {
            "parameterless_full": (
                replace(
                    _eligible_spec(),
                    full_query_source=DIFF_SOURCE,
                    full_source_sha256=query_source_sha256(DIFF_SOURCE),
                ),
                {"scope": []},
                "unsupported_full_parameters",
            ),
            "supplied_vs_declared_mismatch": (
                _eligible_spec(),
                {"scope": [], "extra": True},
                "unsupported_full_parameters",
            ),
            "path_binding_without_pinned_full_commit": (
                _path_bound_spec(commit_id=""),
                {"scope": []},
                "unresolved_full_commit",
            ),
        }

        for name, (spec, effective_parameters, reason) in cases.items():
            with self.subTest(name=name):
                sync = SimpleNamespace(
                    pk=1,
                    parameters={},
                    source=SimpleNamespace(parameters={}),
                )
                client = Mock()
                logger = Mock()
                fetcher = ForwardQueryFetcher(
                    sync=sync,
                    client=client,
                    logger_=logger,
                )
                fetcher._drop_unavailable_integration_models = Mock(
                    return_value=["dcim.site"]
                )
                fetcher._resolve_specs_for_models = Mock(
                    return_value=({"dcim.site": [spec]}, {})
                )
                fetcher._scope_for_spec = Mock(return_value=None)
                fetcher._query_parameters_for_scope = Mock(
                    return_value=effective_parameters
                )

                jobs = fetcher._build_workload_jobs(
                    _context(maps=[]),
                    model_strings=["dcim.site"],
                )

                self.assertEqual(jobs, [])
                self.assertIn("dcim.site", fetcher._failed_model_results)
                self.assertTrue(
                    any(
                        "Execution contract preflight found" in str(call)
                        and f"full:{reason}" in str(call)
                        for call in logger.log_warning.call_args_list
                    )
                )
                self.assertTrue(
                    any(
                        "blocked all Forward workload execution" in str(call)
                        for call in logger.log_warning.call_args_list
                    )
                )
                client.run_nqe_query.assert_not_called()
                client.run_nqe_diff.assert_not_called()

    def test_preflight_surfaces_parameter_declaring_diff_and_schedules_full_only(self):
        spec = replace(
            _eligible_spec(),
            diff_query_source=FULL_SOURCE,
            diff_source_sha256=query_source_sha256(FULL_SOURCE),
        )
        sync = SimpleNamespace(
            pk=1,
            parameters={},
            source=SimpleNamespace(parameters={}),
        )
        client = Mock()
        logger = Mock()
        fetcher = ForwardQueryFetcher(sync=sync, client=client, logger_=logger)
        fetcher._drop_unavailable_integration_models = Mock(return_value=["dcim.site"])
        fetcher._resolve_specs_for_models = Mock(
            return_value=({"dcim.site": [spec]}, {})
        )
        fetcher._scope_for_spec = Mock(return_value=None)
        fetcher._query_parameters_for_scope = Mock(return_value={"scope": []})

        jobs = fetcher._build_workload_jobs(
            _context(maps=[]),
            model_strings=["dcim.site"],
        )

        self.assertEqual(len(jobs), 1)
        self.assertIsNone(jobs[0][2])
        self.assertTrue(
            any(
                "diff:nonempty_diff_declarations" in str(call)
                for call in logger.log_warning.call_args_list
            )
        )
        client.run_nqe_query.assert_not_called()
        client.run_nqe_diff.assert_not_called()

    def test_snapshot_data_file_hash_hydration_reaches_execution_contract(self):
        data_file = "netbox_feature_tag_rules"
        content_hash = "md5:" + ("a" * 32)
        client = Mock()
        client.get_snapshot_data_file_hashes.return_value = {
            data_file: content_hash,
        }
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(pk=1),
            client=client,
            logger_=Mock(),
        )
        spec = replace(
            _eligible_spec(),
            variant="rules",
            required_data_files=(data_file,),
        )

        (hydrated,) = fetcher._hydrate_snapshot_data_file_hashes(
            [spec],
            _context(),
        )
        contract = resolve_execution_contract(
            hydrated,
            effective_parameters=hydrated.parameters,
        )

        self.assertTrue(contract.diff_eligible)
        self.assertEqual(contract.data_file_hashes, ((data_file, content_hash),))
        client.get_snapshot_data_file_hashes.assert_called_once_with(
            "network",
            "snapshot-after",
        )

    def test_missing_or_conflicting_baseline_provenance_forces_full(self):
        model_contract = _model_contract()
        self.assertIsNone(
            compatible_baseline_evidence(
                SimpleNamespace(model_results=[]),
                model_contract,
            )
        )

        baseline = _baseline(model_contract)
        self.assertIsNotNone(compatible_baseline_evidence(baseline, model_contract))
        baseline.model_results[0]["scope_config_fingerprint"] = "changed"
        self.assertIsNone(compatible_baseline_evidence(baseline, model_contract))


class ExecutionFingerprintTest(SimpleTestCase):
    def _fingerprint(
        self,
        *,
        spec=None,
        context=None,
        before_snapshot_id="snapshot-before",
        after_snapshot_id=None,
        before_membership="before-membership",
        effective_parameters=None,
    ):
        context = context or _context()
        model_contract = _model_contract(
            [spec or _eligible_spec()],
            context=context,
            parameters=effective_parameters,
        )
        return diff_artifact_key(
            model_contract,
            before_snapshot_id=before_snapshot_id,
            after_snapshot_id=after_snapshot_id or context.snapshot_id,
            before_scope_membership_fingerprint=before_membership,
        ).fingerprint

    def test_every_semantic_input_invalidates_artifact(self):
        baseline = self._fingerprint()
        full_changed = FULL_SOURCE.replace("slug: value", "slug: toLowerCase(value)")
        diff_changed = DIFF_SOURCE.replace('"site"', '"site-2"')
        cases = {
            "before_snapshot": {
                "before_snapshot_id": "snapshot-before-2",
            },
            "after_snapshot": {
                "after_snapshot_id": "snapshot-after-2",
            },
            "model": {
                "spec": replace(_eligible_spec(), model_string="dcim.location"),
            },
            "query_id": {
                "spec": replace(_eligible_spec(), query_id="Q_sites_2"),
            },
            "query_path": {
                "spec": replace(
                    _eligible_spec(),
                    resolved_query_path="/Forward/renamed_locations",
                ),
            },
            "full_commit": {
                "spec": replace(_eligible_spec(), commit_id="full-commit-2"),
            },
            "diff_commit": {
                "spec": replace(_eligible_spec(), diff_commit_id="diff-commit-2"),
            },
            "full_source_hash": {
                "spec": replace(
                    _eligible_spec(),
                    full_query_source=full_changed,
                    full_source_sha256=query_source_sha256(full_changed),
                ),
            },
            "diff_source_hash": {
                "spec": replace(
                    _eligible_spec(),
                    diff_query_source=diff_changed,
                    diff_source_sha256=query_source_sha256(diff_changed),
                ),
            },
            "variant": {
                "spec": replace(_eligible_spec(), variant="alternate"),
            },
            "data_file_hash": {
                "spec": replace(
                    _eligible_spec(),
                    data_file_hashes={"netbox_data": "b" * 64},
                ),
            },
            "effective_parameters": {
                "effective_parameters": {"scope": ["device-b"]},
            },
            "include_tags": {
                "context": _context(device_tag_include_tags=["Other"]),
            },
            "exclude_tags": {
                "context": _context(device_tag_exclude_tags=["Other"]),
            },
            "include_match": {
                "context": _context(device_tag_include_match="any"),
            },
            "prune": {
                "context": _context(device_tag_prune_out_of_scope=True),
            },
            "sync_device_tags": {
                "context": _context(sync_device_tags=["Other"]),
            },
            "apply_device_scope_tags": {
                "context": _context(apply_device_scope_tags=True),
            },
            "endpoint_toggle": {
                "context": _context(sync_endpoints=True),
            },
            "generic_endpoint_toggle": {
                "context": _context(sync_generic_endpoints=True),
            },
            "endpoint_scope_toggle": {
                "context": _context(scope_endpoints_by_include_tags=True),
            },
            "before_membership": {
                "before_membership": "other-before-membership",
            },
            "after_device_membership": {
                "context": _context(scoped_device_names={"device-b"}),
            },
            "after_site_membership": {
                "context": _context(scoped_site_names={"site-b"}),
            },
            "after_matched_tags": {
                "context": _context(
                    scoped_matched_tags={"device-a": ["Include", "Other"]}
                ),
            },
            "coalesce_identity": {
                "spec": replace(_eligible_spec(), coalesce_fields=(("name",),)),
            },
            "normalization_version": {
                "spec": replace(_eligible_spec(), normalization_version=2),
            },
            "identity_version": {
                "spec": replace(_eligible_spec(), identity_version=2),
            },
            "reducer": {
                "spec": replace(
                    _eligible_spec(),
                    reducer_id="locations",
                    reducer_version=2,
                ),
            },
            "diff_ownership": {
                "spec": replace(
                    _eligible_spec(),
                    diff_ownership_mode="device",
                ),
            },
            "query_contract_version": {
                "spec": replace(_eligible_spec(), query_contract_version=2),
            },
        }
        for name, changes in cases.items():
            with self.subTest(name=name):
                self.assertNotEqual(baseline, self._fingerprint(**changes))


class Tier2OwnershipReducerTest(SimpleTestCase):
    DEVICE_OWNED_MODELS = (
        "dcim.interface",
        "dcim.inventoryitem",
        "dcim.module",
        "netbox_routing.bgppeer",
        "netbox_routing.bgpaddressfamily",
        "netbox_routing.bgppeeraddressfamily",
        "netbox_routing.ospfinstance",
        "netbox_routing.ospfinterface",
    )

    def _fetcher(self, *, client=None, source_parameters=None):
        return ForwardQueryFetcher(
            sync=SimpleNamespace(
                pk=1,
                parameters={},
                source=SimpleNamespace(parameters=dict(source_parameters or {})),
            ),
            client=client or Mock(),
            logger_=Mock(),
        )

    def test_every_device_owned_map_uses_after_upserts_and_before_deletes(self):
        context = _context(
            scoped_device_names={"device-in"},
            scoped_site_names=set(),
            scoped_matched_tags={"device-in": ["Include"]},
        )
        diff_rows = [
            {
                "type": "MODIFIED",
                "before": {"device": "device-in", "name": "leaving"},
                "after": {"device": "device-out", "name": "leaving"},
            },
            {
                "type": "MODIFIED",
                "before": {"device": "device-out", "name": "entering"},
                "after": {"device": "device-in", "name": "entering"},
            },
            {
                "type": "DELETED",
                "before": {"device": "device-in", "name": "deleted"},
                "after": None,
            },
            {
                "type": "ADDED",
                "before": None,
                "after": {"device": "device-out", "name": "ignored"},
            },
        ]

        for model_string in self.DEVICE_OWNED_MODELS:
            with self.subTest(model_string=model_string):
                reduced = self._fetcher()._reduce_tier2_diff_rows_to_scope(
                    model_string=model_string,
                    diff_rows=diff_rows,
                    ownership_mode="device",
                    before_scoped_devices={"device-in"},
                    context=context,
                )
                self.assertEqual(
                    [
                        (row["type"], (row.get("before") or row.get("after"))["name"])
                        for row in reduced
                    ],
                    [
                        ("DELETED", "leaving"),
                        ("ADDED", "entering"),
                        ("DELETED", "deleted"),
                    ],
                )

    def test_silent_missed_delete_fixture_uses_before_side_owner(self):
        context = _context(
            scoped_device_names={"device-in"},
            scoped_site_names=set(),
            scoped_matched_tags={"device-in": ["Include"]},
        )
        before = {"device": "device-in", "name": "Ethernet1"}
        after = {"device": "device-out", "name": "Ethernet1"}

        reduced = self._fetcher()._reduce_tier2_diff_rows_to_scope(
            model_string="dcim.interface",
            diff_rows=[{"type": "MODIFIED", "before": before, "after": after}],
            ownership_mode="device",
            before_scoped_devices={"device-in"},
            context=context,
        )

        self.assertEqual(
            reduced,
            [{"type": "DELETED", "before": before, "after": None}],
        )

    def test_cable_scope_retains_either_endpoint_on_each_side(self):
        context = _context(
            scoped_device_names={"device-after"},
            scoped_site_names=set(),
            scoped_matched_tags={"device-after": ["Include"]},
        )
        before = {
            "device": "device-before",
            "interface": "Ethernet1",
            "remote_device": "outside-before",
            "remote_interface": "Ethernet2",
        }
        after = {
            "device": "outside-after",
            "interface": "Ethernet2",
            "remote_device": "device-after",
            "remote_interface": "Ethernet1",
        }

        reduced = self._fetcher()._reduce_tier2_diff_rows_to_scope(
            model_string="dcim.cable",
            diff_rows=[{"type": "MODIFIED", "before": before, "after": after}],
            ownership_mode="cable_either_endpoint",
            before_scoped_devices={"device-before"},
            context=context,
        )

        self.assertEqual(
            reduced,
            [{"type": "MODIFIED", "before": before, "after": after}],
        )

    def test_before_scope_is_verified_and_membership_change_blocks_diff(self):
        fetcher = self._fetcher()
        context = _context(
            scoped_device_names={"device-a"},
            scoped_site_names=set(),
            scoped_matched_tags={"device-a": ["Include"]},
        )
        before_context = replace(context, snapshot_id="snapshot-before")
        baseline = SimpleNamespace(
            snapshot_id="snapshot-before",
            model_results=[
                {
                    "model": "dcim.interface",
                    "scope_membership_fingerprint": scope_membership_fingerprint(
                        before_context
                    ),
                }
            ],
        )
        fetcher._resolve_scoped_tag_scope = Mock(
            return_value=(
                {"device-a"},
                set(),
                {"device-a": ["Include"]},
                False,
            )
        )

        before_devices, reason = fetcher._verified_tier2_before_scope(
            model_string="dcim.interface",
            baseline=baseline,
            context=context,
        )

        self.assertEqual(before_devices, {"device-a"})
        self.assertEqual(reason, "")

        changed_fetcher = self._fetcher()
        changed_fetcher._resolve_scoped_tag_scope = Mock(
            return_value=(
                {"device-before-only"},
                set(),
                {"device-before-only": ["Include"]},
                False,
            )
        )
        changed_before = replace(
            context,
            snapshot_id="snapshot-before",
            scoped_device_names={"device-before-only"},
            scoped_matched_tags={"device-before-only": ["Include"]},
        )
        baseline.model_results[0]["scope_membership_fingerprint"] = (
            scope_membership_fingerprint(changed_before)
        )

        before_devices, reason = changed_fetcher._verified_tier2_before_scope(
            model_string="dcim.interface",
            baseline=baseline,
            context=context,
        )

        self.assertIsNone(before_devices)
        self.assertEqual(reason, "scope_membership_changed")

    def test_membership_change_runs_full_without_attempting_diff(self):
        context = _context(
            scoped_device_names={"device-after"},
            scoped_site_names=set(),
            scoped_matched_tags={"device-after": ["Include"]},
        )
        before_context = replace(
            context,
            snapshot_id="snapshot-before",
            scoped_device_names={"device-before"},
            scoped_matched_tags={"device-before": ["Include"]},
        )
        baseline = SimpleNamespace(
            snapshot_id="snapshot-before",
            model_results=[
                {
                    "model": "dcim.interface",
                    "scope_membership_fingerprint": scope_membership_fingerprint(
                        before_context
                    ),
                }
            ],
        )
        spec = _eligible_spec(
            model_string="dcim.interface",
            contract_key="forward_interfaces",
            coalesce_fields=(("device", "name"),),
            diff_ownership_mode="device",
            reducer_id="tier2_side_local_device",
            reducer_version=2,
        )
        contract = _contract(spec, effective_parameters={"scope": []})
        client = Mock()
        client.run_nqe_query.return_value = [
            {"device": "device-after", "name": "Ethernet1"}
        ]
        fetcher = ForwardQueryFetcher(
            sync=SimpleNamespace(
                parameters={},
                source=SimpleNamespace(parameters={}),
            ),
            client=client,
            logger_=Mock(),
        )
        fetcher._resolve_scoped_tag_scope = Mock(
            return_value=(
                {"device-before"},
                set(),
                {"device-before": ["Include"]},
                False,
            )
        )

        rows, deletes, sync_mode, metadata = fetcher._fetch_spec_rows(
            "dcim.interface",
            contract,
            baseline,
            context,
            [["device", "name"]],
            return_fetch_meta=True,
        )

        self.assertEqual(rows, [{"device": "device-after", "name": "Ethernet1"}])
        self.assertEqual(deletes, [])
        self.assertEqual(sync_mode, "full")
        self.assertEqual(
            metadata["fetch_parameters"]["fallback_reason"],
            "scope_membership_changed",
        )
        client.run_nqe_diff.assert_not_called()
        client.run_nqe_query.assert_called_once()

    def test_diff_budget_exceeded_falls_back_to_full_with_type_only_reason(self):
        context = _context()
        baseline = SimpleNamespace(snapshot_id="snapshot-before")
        contract = _contract(effective_parameters={"scope": []})
        client = Mock()
        client.run_nqe_diff.side_effect = ForwardFetchBudgetExceededError(
            "customer-derived backend detail"
        )
        client.run_nqe_query.return_value = [{"name": "site-a", "slug": "site-a"}]
        fetcher = self._fetcher(client=client)

        rows, deletes, sync_mode, metadata = fetcher._fetch_spec_rows(
            "dcim.site",
            contract,
            baseline,
            context,
            [["slug"], ["name"]],
            return_fetch_meta=True,
        )

        self.assertEqual(rows, [{"name": "site-a", "slug": "site-a"}])
        self.assertEqual(deletes, [])
        self.assertEqual(sync_mode, "full")
        self.assertEqual(
            metadata["fetch_parameters"]["fallback_reason"],
            DIFF_BUDGET_FALLBACK_REASON,
        )
        self.assertNotIn(
            "customer-derived",
            metadata["fetch_parameters"]["fallback_reason"],
        )
        client.run_nqe_diff.assert_called_once()
        self.assertIsNotNone(client.run_nqe_diff.call_args.kwargs["deadline"])
        client.run_nqe_query.assert_called_once()

    def test_diff_timeout_circuit_opens_after_configured_threshold(self):
        context = _context()
        baseline = SimpleNamespace(snapshot_id="snapshot-before")
        contract = _contract(effective_parameters={"scope": []})
        client = Mock()
        client.run_nqe_diff.side_effect = [
            ForwardFetchBudgetExceededError("first"),
            ForwardFetchBudgetExceededError("second"),
        ]
        client.run_nqe_query.return_value = [{"name": "site-a", "slug": "site-a"}]
        fetcher = self._fetcher(
            client=client,
            source_parameters={"diff_timeout_circuit_breaker_threshold": 2},
        )

        metadata = None
        for _index in range(3):
            _rows, _deletes, sync_mode, metadata = fetcher._fetch_spec_rows(
                "dcim.site",
                contract,
                baseline,
                context,
                [["slug"], ["name"]],
                return_fetch_meta=True,
            )
            self.assertEqual(sync_mode, "full")

        self.assertEqual(client.run_nqe_diff.call_count, 2)
        self.assertEqual(client.run_nqe_query.call_count, 3)
        self.assertEqual(
            metadata["fetch_parameters"]["fallback_reason"],
            DIFF_CIRCUIT_OPEN_FALLBACK_REASON,
        )

    def test_fast_diff_is_unaffected_by_bounded_policy(self):
        context = _context()
        baseline = SimpleNamespace(snapshot_id="snapshot-before")
        contract = _contract(effective_parameters={"scope": []})
        client = Mock()
        client.run_nqe_diff.return_value = [
            {
                "type": "ADDED",
                "before": None,
                "after": {"name": "site-a", "slug": "site-a"},
            }
        ]
        fetcher = self._fetcher(client=client)

        rows, deletes, sync_mode, metadata = fetcher._fetch_spec_rows(
            "dcim.site",
            contract,
            baseline,
            context,
            [["slug"], ["name"]],
            return_fetch_meta=True,
        )

        self.assertEqual(rows, [{"name": "site-a", "slug": "site-a"}])
        self.assertEqual(deletes, [])
        self.assertEqual(sync_mode, "diff")
        self.assertNotIn("fallback_reason", metadata["fetch_parameters"])
        client.run_nqe_query.assert_not_called()

    def test_map_set_is_sorted_but_add_remove_or_weight_change_invalidates(self):
        first = _eligible_spec()
        second = _eligible_spec(
            query_name="Forward Sites Secondary",
            query_id="Q_sites_secondary",
            map_id=2,
            map_weight=200,
            contract_key="forward_locations_secondary",
            commit_id="full-secondary",
            diff_commit_id="diff-secondary",
        )
        context = _context()
        forward = _model_contract([first, second], context=context)
        reverse = _model_contract([second, first], context=context)
        self.assertEqual(
            forward.map_set_fingerprint,
            reverse.map_set_fingerprint,
        )
        self.assertNotEqual(
            forward.map_set_fingerprint,
            _model_contract([first], context=context).map_set_fingerprint,
        )
        changed_weight = _model_contract(
            [first, replace(second, map_weight=50)],
            context=context,
        )
        self.assertNotEqual(
            forward.map_set_fingerprint,
            changed_weight.map_set_fingerprint,
        )


class DiffArtifactStoreTest(SimpleTestCase):
    def test_concurrent_consumers_build_once(self):
        store = DiffArtifactStore()
        key = DiffArtifactKey("dcim.site", "before", "after", "maps", "scope")
        builder = Mock(return_value=DiffArtifact(key=key, map_results=("result",)))

        with ThreadPoolExecutor(max_workers=12) as executor:
            artifacts = list(
                executor.map(
                    lambda _index: store.get_or_build(key, builder),
                    range(40),
                )
            )

        self.assertEqual(builder.call_count, 1)
        self.assertTrue(all(artifact is artifacts[0] for artifact in artifacts))

    def test_many_local_shards_execute_one_diff_per_unique_query_map(self):
        first = _eligible_spec()
        second = _eligible_spec(
            query_name="Forward Sites Secondary",
            query_id="Q_sites_secondary",
            map_id=2,
            map_weight=200,
            contract_key="forward_locations_secondary",
            commit_id="full-secondary",
            diff_commit_id="diff-secondary",
        )
        context = _context(
            device_tag_include_tags=[],
            device_tag_exclude_tags=[],
            scoped_device_names=set(),
            scoped_site_names=set(),
            scoped_matched_tags={},
        )
        contracts = [
            _contract(first, effective_parameters={"scope": []}),
            _contract(second, effective_parameters={"scope": []}),
        ]
        model_contract = resolve_model_execution_contract(
            "dcim.site",
            contracts,
            context=context,
        )
        baseline = _baseline(model_contract)
        client = Mock()

        def diff_rows(**kwargs):
            prefix = kwargs["query_id"]
            return [
                {
                    "type": "ADDED",
                    "before": None,
                    "after": {
                        "name": f"{prefix}-{index}",
                        "slug": f"{prefix}-{index}",
                    },
                }
                for index in range(30)
            ]

        client.run_nqe_diff.side_effect = diff_rows
        sync = SimpleNamespace(
            pk=1,
            parameters={},
            source=SimpleNamespace(
                parameters={
                    "workload_fetch_retry_attempts": 0,
                    "workload_fetch_retry_backoff_seconds": 0,
                }
            ),
        )
        fetcher = ForwardQueryFetcher(sync=sync, client=client, logger_=Mock())
        decision = SimpleNamespace(
            selected_engine="adapter",
            reason="test",
            as_dict=lambda: {"selected_engine": "adapter"},
        )
        jobs = [
            (
                "dcim.site",
                contract,
                baseline,
                [["slug"], ["name"]],
                None,
                model_contract,
            )
            for contract in contracts
        ]

        with patch(
            "forward_netbox.utilities.query_fetch_execution.apply_engine_decision_for",
            return_value=decision,
        ):
            results = [fetcher._run_workload_job((context, False, job)) for job in jobs]
            # Repeated consumers of the same model artifact must remain cache hits.
            results.extend(
                fetcher._run_workload_job((context, False, job)) for job in jobs
            )

        workloads = [workload for _result, workload in results[:2]]
        plan = build_branch_plan(
            workloads,
            max_changes_per_staging_item=1,
        )
        self.assertGreater(len(plan), 50)
        call_counts = Counter(
            call.kwargs["query_id"] for call in client.run_nqe_diff.call_args_list
        )
        self.assertEqual(
            call_counts,
            Counter({"Q_sites": 1, "Q_sites_secondary": 1}),
        )
        self.assertEqual(client.run_nqe_diff.call_count, len(contracts))
        self.assertEqual(
            {
                (call.kwargs["query_id"], call.kwargs["commit_id"])
                for call in client.run_nqe_diff.call_args_list
            },
            {
                ("Q_sites", "diff-commit"),
                ("Q_sites_secondary", "diff-secondary"),
            },
        )
