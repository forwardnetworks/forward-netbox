"""A deterministic row failure must not be a permanent dead end.

When any row fails, the merge sets the branch back to READY and never attests,
so `merge_applied_at` stays null — which also means
`resume_post_merge_bookkeeping` cannot recover it, because that requires durable
merge-applied evidence. The only exit is a retry with zero failures.

When the failure is deterministic — a unique-constraint clash on a row the
source keeps re-sending, a validation error on data the operator cannot change —
that retry never arrives. A customer sat with 5 failures out of 24,748 blocking
the baseline, and with it drift measurement and diff-based syncs, indefinitely.

Accepting the failures must stay an explicit operator act. Nothing in the sync
or retry path may set it, the failures stay recorded, and the acceptance is
written into durable evidence — otherwise this becomes the 2.6.4 defect in a new
costume, where a run that did not fully apply looks like one that did.
"""

from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.models import Branch

from forward_netbox.exceptions import ForwardPartialMergeError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.forward_api import LATEST_PROCESSED_SNAPSHOT
from forward_netbox.utilities.merge import ACCEPTED_MERGE_FAILURES_KEY
from forward_netbox.utilities.merge import merge_branch


class AcceptedMergeFailureTest(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="accept-owner")
        self.source = ForwardSource.objects.create(
            name="accept-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="accept-sync",
            source=self.source,
            user=self.user,
            parameters={"snapshot_id": LATEST_PROCESSED_SNAPSHOT},
        )
        self.branch = Branch.objects.create(
            name=f"accept-{uuid4().hex[:10]}",
            schema_id=f"accept_{uuid4().hex[:10]}",
            status=BranchStatusChoices.READY,
        )
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            branch=self.branch,
            snapshot_selector=LATEST_PROCESSED_SNAPSHOT,
            snapshot_id="snapshot-accept",
        )

    def _merge(self, *, applied, failed, accept):
        changes = self._changes(applied + failed)

        def bulk(*_args, result_metadata=None, **_kwargs):
            result_metadata.update(
                logical_total=applied + failed,
                logical_action_counts={"create": applied},
            )
            return applied, failed, set()

        with (
            patch.object(Branch, "get_unmerged_changes", return_value=changes),
            patch(
                "forward_netbox.utilities.merge.bulk_merge_changes", side_effect=bulk
            ),
        ):
            merge_branch(
                self.ingestion,
                user=self.user,
                accept_reported_failures=accept,
            )

    @staticmethod
    def _changes(total):
        from unittest.mock import MagicMock

        changes = MagicMock()
        changes.order_by.return_value = changes
        changes.exists.return_value = total > 0
        changes.values.return_value.distinct.return_value.count.return_value = total
        changes.values_list.return_value.distinct.return_value = []
        changes.annotate.return_value = []
        return changes

    def test_a_failed_row_still_blocks_by_default(self):
        # The guarantee that must not regress: silence is never the default.
        with self.assertRaises(ForwardPartialMergeError):
            self._merge(applied=10, failed=1, accept=False)
        self.ingestion.refresh_from_db()
        self.assertIsNone(self.ingestion.merge_applied_at)

    def test_accepting_completes_the_merge(self):
        self._merge(applied=10, failed=1, accept=True)
        self.ingestion.refresh_from_db()
        self.assertIsNotNone(
            self.ingestion.merge_applied_at,
            "accepting must attest the merge, or bookkeeping can never complete",
        )

    def test_acceptance_is_recorded_as_durable_evidence(self):
        self._merge(applied=10, failed=2, accept=True)
        self.ingestion.refresh_from_db()
        accepted = (self.ingestion.snapshot_info or {}).get(ACCEPTED_MERGE_FAILURES_KEY)
        self.assertTrue(accepted, "an accepted-over baseline must say so")
        self.assertEqual(accepted[-1]["failed_change_count"], 2)
        self.assertEqual(accepted[-1]["accepted_by"], "accept-owner")
        self.assertTrue(accepted[-1]["accepted_at"])

    def test_the_failure_count_is_still_reported(self):
        # Accepting must not erase the failures; it only stops them blocking.
        self._merge(applied=10, failed=3, accept=True)
        self.ingestion.refresh_from_db()
        self.assertEqual(self.ingestion.failed_change_count, 3)

    def test_a_clean_merge_records_no_acceptance(self):
        self._merge(applied=10, failed=0, accept=True)
        self.ingestion.refresh_from_db()
        self.assertNotIn(
            ACCEPTED_MERGE_FAILURES_KEY, self.ingestion.snapshot_info or {}
        )

    def test_acceptance_defaults_to_off_in_the_sync_path(self):
        # Nothing automatic may reach it. This is the whole safety argument.
        import inspect

        from forward_netbox.utilities.ingestion_merge import sync_merge_ingestion

        signature = inspect.signature(sync_merge_ingestion)
        self.assertIs(signature.parameters["accept_reported_failures"].default, False)
        signature = inspect.signature(merge_branch)
        self.assertIs(signature.parameters["accept_reported_failures"].default, False)
