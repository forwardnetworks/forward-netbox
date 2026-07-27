import os
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import DatabaseError
from django.db import transaction
from django.test import TestCase
from django.utils import timezone

from forward_netbox.models import ForwardContributorBaseline
from forward_netbox.models import ForwardContributorRelation
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.contributor_baseline import canonical_contributor_identity
from forward_netbox.utilities.contributor_baseline import compatible_current_relation
from forward_netbox.utilities.contributor_baseline import contributor_storage_summary
from forward_netbox.utilities.contributor_baseline import ContributorBaselineExpectation
from forward_netbox.utilities.contributor_baseline import (
    ContributorBaselinePromotionError,
)
from forward_netbox.utilities.contributor_baseline import ContributorBaselineUnavailable
from forward_netbox.utilities.contributor_baseline import ContributorRelationContract
from forward_netbox.utilities.contributor_baseline import ContributorRelationSeed
from forward_netbox.utilities.contributor_baseline import ContributorWorkRelation
from forward_netbox.utilities.contributor_baseline import iter_relation_entries
from forward_netbox.utilities.contributor_baseline import (
    promote_contributor_baselines_locked,
)
from forward_netbox.utilities.contributor_baseline import stage_contributor_baseline
from forward_netbox.utilities.workload_state import (
    stage_and_promote_noop_workload_states,
)


class ContributorBaselineCacheTest(TestCase):
    def setUp(self):
        user = get_user_model().objects.create_user(username="contributor-cache-owner")
        self.source = ForwardSource.objects.create(
            name="contributor-cache-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "network"},
        )
        self.sync = ForwardSync.objects.create(
            name="contributor-cache-sync",
            source=self.source,
            user=user,
            parameters={"dcim.macaddress": True},
        )
        self.contract = ContributorRelationContract(
            model_string="dcim.macaddress",
            map_id=None,
            contract_key="forward_mac_addresses",
            query_path="/netbox/forward_mac_addresses",
            query_id="Q_mac",
            full_commit_id="full-commit",
            full_source_sha256="1" * 64,
            diff_query_id="Q_mac",
            diff_commit_id="diff-commit",
            diff_source_sha256="2" * 64,
            contract_fingerprint="3" * 64,
            reducer_id="mac_representative",
            reducer_version=1,
            normalization_version=1,
            identity_version=1,
        )
        self.rows = [
            {
                "device": "device-a",
                "interface": "Ethernet1",
                "mac_address": "00:11:22:33:44:55",
                "contributor_tags": ["Blue", "Blue"],
            },
            {
                "device": "device-b",
                "interface": "Ethernet2",
                "mac_address": "00:11:22:33:44:55",
                "contributor_tags": ["Blue"],
            },
        ]

    def _ingestion(self, snapshot_id):
        return ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id=snapshot_id,
        )

    def _stage(self, ingestion, *, rows=None, contract=None):
        return stage_contributor_baseline(
            ingestion,
            [
                ContributorRelationSeed(
                    contract=contract or self.contract,
                    rows=list(self.rows if rows is None else rows),
                    target_key=lambda row: row["mac_address"],
                )
            ],
            network_fingerprint="4" * 64,
            map_set_fingerprint="5" * 64,
            scope_config_fingerprint="6" * 64,
            scope_membership_fingerprint="7" * 64,
            scope_state={
                "scoped_device_names": ["device-a", "device-b"],
                "matched_tags": {"device-a": ["Blue"]},
            },
        )

    def _promote(self, ingestion):
        ingestion.merge_applied_at = timezone.now()
        ingestion.save(update_fields=["merge_applied_at"])
        with transaction.atomic():
            return promote_contributor_baselines_locked(ingestion)

    def _expectation(self, **overrides):
        values = {
            "before_snapshot_id": "snapshot-1",
            "network_fingerprint": "4" * 64,
            "map_set_fingerprint": "5" * 64,
            "scope_config_fingerprint": "6" * 64,
            "scope_membership_fingerprint": "7" * 64,
            "contract": self.contract,
        }
        values.update(overrides)
        return ContributorBaselineExpectation(**values)

    def test_chunked_relation_round_trip_and_size_summary(self):
        baseline = self._stage(self._ingestion("snapshot-1"))
        relation = baseline.relations.get()

        decoded = list(iter_relation_entries(relation))

        self.assertEqual(len(decoded), 2)
        self.assertEqual(
            decoded[0][0],
            canonical_contributor_identity(self.rows[0]),
        )
        self.assertEqual(
            decoded[0][2]["contributor_tags"],
            ["Blue"],
        )
        summary = contributor_storage_summary(baseline)
        self.assertEqual(summary["row_count"], 2)
        self.assertGreater(summary["uncompressed_bytes"], 0)
        self.assertGreater(summary["compressed_bytes"], 0)

    def test_pending_and_failed_run_never_advance_current_baseline(self):
        ingestion = self._ingestion("snapshot-1")
        pending = self._stage(ingestion)

        self.assertFalse(pending.is_current)
        self.assertFalse(
            ForwardContributorBaseline.objects.filter(is_current=True).exists()
        )
        with (
            self.assertRaises(ContributorBaselinePromotionError),
            transaction.atomic(),
        ):
            promote_contributor_baselines_locked(ingestion)
        pending.refresh_from_db()
        self.assertFalse(pending.is_current)

    def test_successful_merge_evidence_promotes_once_and_resume_is_idempotent(self):
        ingestion = self._ingestion("snapshot-1")
        pending = self._stage(ingestion)

        self.assertEqual(self._promote(ingestion), 1)
        pending.refresh_from_db()
        self.assertTrue(pending.is_current)
        self.assertEqual(pending.status, ForwardContributorBaseline.Status.CURRENT)
        with transaction.atomic():
            self.assertEqual(promote_contributor_baselines_locked(ingestion), 1)
        self.assertEqual(
            ForwardContributorBaseline.objects.filter(is_current=True).count(),
            1,
        )

    def test_noop_finalizer_is_an_explicit_promotion_boundary(self):
        ingestion = self._ingestion("snapshot-1")
        pending = self._stage(ingestion)

        stage_and_promote_noop_workload_states(ingestion, [])

        ingestion.refresh_from_db()
        pending.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertIsNotNone(ingestion.merge_applied_at)
        self.assertIsNotNone(ingestion.merge_finalized_at)
        self.assertTrue(pending.is_current)

    def test_contract_commit_scope_and_cache_miss_invalidate(self):
        ingestion = self._ingestion("snapshot-1")
        self._stage(ingestion)
        self._promote(ingestion)

        relation, reason = compatible_current_relation(
            self.sync,
            self._expectation(),
        )
        self.assertIsNotNone(relation)
        self.assertEqual(reason, "")

        cases = {
            "baseline_snapshot_changed": self._expectation(
                before_snapshot_id="snapshot-other"
            ),
            "network_scope_changed": self._expectation(network_fingerprint="8" * 64),
            "query_commit_changed": self._expectation(
                contract=replace(self.contract, diff_commit_id="different")
            ),
            "query_identity_changed": self._expectation(
                contract=replace(self.contract, query_path="/different")
            ),
            "contract_fingerprint_changed": self._expectation(
                contract=replace(
                    self.contract,
                    contract_fingerprint="9" * 64,
                )
            ),
            "query_map_changed": self._expectation(
                contract=replace(self.contract, map_id=99)
            ),
            "scope_config_changed": self._expectation(
                scope_config_fingerprint="a" * 64
            ),
            "scope_membership_changed": self._expectation(
                scope_membership_fingerprint="b" * 64
            ),
            "map_set_changed": self._expectation(map_set_fingerprint="c" * 64),
        }
        for expected_reason, expectation in cases.items():
            with self.subTest(expected_reason=expected_reason):
                relation, reason = compatible_current_relation(
                    self.sync,
                    expectation,
                )
                self.assertIsNone(relation)
                self.assertEqual(reason, expected_reason)

        second_sync = ForwardSync.objects.create(
            name="contributor-cache-empty-sync",
            source=self.source,
            user=self.sync.user,
            parameters={"dcim.macaddress": True},
        )
        relation, reason = compatible_current_relation(
            second_sync,
            self._expectation(),
        )
        self.assertIsNone(relation)
        self.assertEqual(reason, "cache_miss")

    def test_cache_corruption_and_database_failure_fail_closed(self):
        ingestion = self._ingestion("snapshot-1")
        baseline = self._stage(ingestion)
        self._promote(ingestion)
        relation = baseline.relations.get()
        chunk = relation.chunks.get()
        chunk.payload = bytes(chunk.payload) + b"corrupt"
        chunk.save(update_fields=["payload"])

        loaded, reason = compatible_current_relation(
            self.sync,
            self._expectation(),
        )

        self.assertIsNone(loaded)
        self.assertEqual(reason, "cache_corrupt")
        with patch.object(
            ForwardContributorBaseline.objects,
            "filter",
            side_effect=DatabaseError("database unavailable"),
        ):
            loaded, reason = compatible_current_relation(
                self.sync,
                self._expectation(),
            )
        self.assertIsNone(loaded)
        self.assertEqual(reason, "cache_database_error")

    def test_generation_guards_fail_atomically_without_truncation(self):
        row_limited_ingestion = self._ingestion("snapshot-row-limit")
        with self.assertRaises(ContributorBaselineUnavailable):
            stage_contributor_baseline(
                row_limited_ingestion,
                [
                    ContributorRelationSeed(
                        contract=self.contract,
                        rows=self.rows,
                        target_key=lambda row: row["mac_address"],
                    )
                ],
                network_fingerprint="4" * 64,
                map_set_fingerprint="5" * 64,
                scope_config_fingerprint="6" * 64,
                scope_membership_fingerprint="7" * 64,
                max_rows=1,
            )
        self.assertFalse(
            ForwardContributorBaseline.objects.filter(
                ingestion=row_limited_ingestion
            ).exists()
        )

        byte_limited_ingestion = self._ingestion("snapshot-byte-limit")
        with self.assertRaises(ContributorBaselineUnavailable):
            stage_contributor_baseline(
                byte_limited_ingestion,
                [
                    ContributorRelationSeed(
                        contract=self.contract,
                        rows=self.rows,
                        target_key=lambda row: row["mac_address"],
                    )
                ],
                network_fingerprint="4" * 64,
                map_set_fingerprint="5" * 64,
                scope_config_fingerprint="6" * 64,
                scope_membership_fingerprint="7" * 64,
                max_compressed_bytes=1,
            )
        self.assertFalse(
            ForwardContributorBaseline.objects.filter(
                ingestion=byte_limited_ingestion
            ).exists()
        )

    def test_corrupt_pending_generation_finalizes_without_cache_advancement(self):
        ingestion = self._ingestion("snapshot-corrupt-pending")
        pending = self._stage(ingestion)
        chunk = pending.relations.get().chunks.get()
        chunk.payload = bytes(chunk.payload) + b"corrupt"
        chunk.save(update_fields=["payload"])

        stage_and_promote_noop_workload_states(ingestion, [])

        ingestion.refresh_from_db()
        pending.refresh_from_db()
        self.assertIsNotNone(ingestion.merge_applied_at)
        self.assertIsNotNone(ingestion.merge_finalized_at)
        self.assertTrue(ingestion.baseline_ready)
        self.assertFalse(pending.is_current)
        self.assertEqual(pending.status, ForwardContributorBaseline.Status.PENDING)
        loaded, reason = compatible_current_relation(
            self.sync,
            self._expectation(before_snapshot_id=ingestion.snapshot_id),
        )
        self.assertIsNone(loaded)
        self.assertEqual(reason, "cache_miss")

    def test_cache_database_error_during_finalization_fails_closed(self):
        ingestion = self._ingestion("snapshot-database-failure")
        pending = self._stage(ingestion)

        with patch(
            "forward_netbox.utilities.contributor_baseline.promote_contributor_baselines_locked",
            side_effect=DatabaseError("database unavailable"),
        ):
            stage_and_promote_noop_workload_states(ingestion, [])

        ingestion.refresh_from_db()
        pending.refresh_from_db()
        self.assertTrue(ingestion.baseline_ready)
        self.assertIsNotNone(ingestion.merge_applied_at)
        self.assertFalse(pending.is_current)

    def test_stale_concurrent_generation_cannot_replace_new_current(self):
        first_ingestion = self._ingestion("snapshot-1")
        first = self._stage(first_ingestion)
        self._promote(first_ingestion)

        second_ingestion = self._ingestion("snapshot-2")
        second = self._stage(second_ingestion)
        third_ingestion = self._ingestion("snapshot-3")
        third = self._stage(third_ingestion)
        self.assertEqual(second.parent_baseline_id, first.pk)
        self.assertEqual(third.parent_baseline_id, first.pk)

        self._promote(second_ingestion)
        with self.assertRaises(ContributorBaselinePromotionError):
            self._promote(third_ingestion)

        current = ForwardContributorBaseline.objects.get(is_current=True)
        self.assertEqual(current.pk, second.pk)
        third.refresh_from_db()
        self.assertFalse(third.is_current)

    def test_selected_mac_deletion_uses_unchanged_alternate_contributor(self):
        ingestion = self._ingestion("snapshot-1")
        baseline = self._stage(ingestion)
        self._promote(ingestion)
        relation = ForwardContributorRelation.objects.get(baseline=baseline)

        with ContributorWorkRelation(relation) as work_relation:
            before = work_relation.reduce_mac_addresses()
            work_relation.apply_diff(
                [
                    {
                        "type": "DELETED",
                        "before": self.rows[0],
                        "after": None,
                    }
                ],
                target_key=lambda row: row["mac_address"],
            )
            after = work_relation.reduce_mac_addresses()

        self.assertEqual(before[0]["device"], "device-a")
        self.assertEqual(after[0]["device"], "device-b")
        self.assertEqual(after[0]["mac_address"], "00:11:22:33:44:55")
        self.assertNotEqual(before, after)

    def test_work_relation_cleans_private_directory_when_sqlite_open_fails(self):
        ingestion = self._ingestion("snapshot-temp-cleanup")
        relation = self._stage(ingestion).relations.get()
        directory = tempfile.mkdtemp(prefix="forward-contributor-test-")

        with (
            patch(
                "forward_netbox.utilities.contributor_baseline.tempfile.mkdtemp",
                return_value=directory,
            ),
            patch(
                "forward_netbox.utilities.contributor_baseline.sqlite3.connect",
                side_effect=sqlite3.OperationalError("unavailable"),
            ),
            self.assertRaises(sqlite3.OperationalError),
        ):
            ContributorWorkRelation(relation)

        self.assertFalse(os.path.exists(directory))

    def test_work_relation_allows_serialized_worker_to_coordinator_handoff(self):
        relation = self._stage(
            self._ingestion("snapshot-thread-handoff")
        ).relations.get()
        with ContributorWorkRelation(relation) as work_relation:
            with ThreadPoolExecutor(max_workers=1) as executor:
                worker_rows = executor.submit(
                    lambda: list(work_relation.iter_rows())
                ).result()
            coordinator_rows = list(work_relation.iter_rows())

        self.assertEqual(worker_rows, coordinator_rows)
        self.assertEqual(len(coordinator_rows), 2)
