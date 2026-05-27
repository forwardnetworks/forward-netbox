import json
from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from forward_netbox.choices import ForwardExecutionRunStatusChoices
from forward_netbox.choices import ForwardSourceStatusChoices
from forward_netbox.models import ForwardExecutionRun
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.branch_budget import BRANCH_RUN_STATE_PARAMETER


class ForwardPruneCompatibilityCacheCommandTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = ForwardSource.objects.create(
            name="prune-source",
            type="saas",
            url="https://fwd.app",
            status=ForwardSourceStatusChoices.READY,
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "network_id": "network-1",
            },
        )

    def _set_branch_run_payload(self, sync, payload):
        parameters = dict(sync.parameters or {})
        parameters[BRANCH_RUN_STATE_PARAMETER] = payload
        ForwardSync.objects.filter(pk=sync.pk).update(parameters=parameters)
        sync.refresh_from_db()

    def test_dry_run_reports_stale_payload_without_writing(self):
        sync = ForwardSync.objects.create(
            name="prune-sync-dry-run",
            source=self.source,
            parameters={},
        )
        ForwardExecutionRun.objects.create(
            sync=sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-1",
        )
        self._set_branch_run_payload(sync, {"phase": "planning", "next_step_index": 2})

        stream = StringIO()
        call_command(
            "forward_prune_compatibility_cache",
            "--sync-name",
            sync.name,
            "--dry-run",
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["inspected_syncs"], 1)
        self.assertEqual(payload["stale_payload_syncs"], 1)
        self.assertEqual(payload["pruned_syncs"], 0)
        self.assertTrue(payload["rows"][0]["stale_payload"])
        sync.refresh_from_db()
        self.assertIn(BRANCH_RUN_STATE_PARAMETER, sync.parameters)

    def test_prunes_stale_payload_when_not_dry_run(self):
        sync = ForwardSync.objects.create(
            name="prune-sync-write",
            source=self.source,
            parameters={},
        )
        ForwardExecutionRun.objects.create(
            sync=sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.COMPLETED,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-2",
        )
        self._set_branch_run_payload(
            sync, {"phase": "merge_wait", "awaiting_merge": False}
        )

        stream = StringIO()
        call_command(
            "forward_prune_compatibility_cache",
            "--sync-name",
            sync.name,
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["stale_payload_syncs"], 1)
        self.assertEqual(payload["pruned_syncs"], 1)
        self.assertTrue(payload["rows"][0]["pruned"])
        sync.refresh_from_db()
        self.assertNotIn(BRANCH_RUN_STATE_PARAMETER, sync.parameters)

    def test_does_not_prune_when_active_run_exists(self):
        sync = ForwardSync.objects.create(
            name="prune-sync-active-run",
            source=self.source,
            parameters={},
        )
        ForwardExecutionRun.objects.create(
            sync=sync,
            source=self.source,
            status=ForwardExecutionRunStatusChoices.RUNNING,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot-3",
        )
        self._set_branch_run_payload(sync, {"phase": "running", "next_step_index": 3})

        stream = StringIO()
        call_command(
            "forward_prune_compatibility_cache",
            "--sync-name",
            sync.name,
            stdout=stream,
        )
        payload = json.loads(stream.getvalue())
        self.assertEqual(payload["stale_payload_syncs"], 0)
        self.assertEqual(payload["pruned_syncs"], 0)
        self.assertFalse(payload["rows"][0]["stale_payload"])
        sync.refresh_from_db()
        self.assertIn(BRANCH_RUN_STATE_PARAMETER, sync.parameters)
