# The scope audit CLI printed the same 25-name sample the panel shows, so a
# customer with 552 uncovered devices had no way, in the UI or on a shell, to
# list them. `--full` prints every name in every bucket, to the console only.
import json
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


def _report():
    return {
        "sync_id": 1,
        "netbox_out_of_scope": 2,
        "forward_missing_in_netbox": 1,
        "out_of_scope_sample": ["orphan-a"],
        "unmanaged": {"owned_untagged_sample": ["gone-a"]},
        "out_of_scope_absence": {"available": False},
        "_out_of_scope": {"orphan-a", "orphan-b"},
        "_tagged_names": {"in-scope"},
        "_device_tagged_names": {"in-scope"},
        "_present_backfilled": {"backfilled-a"},
        "_owned_untagged": {"gone-a", "gone-b", "gone-c"},
        "_missing_in_netbox": {"missing-a"},
        "_out_of_scope_pks": [],
    }


class ScopeAuditFullTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="audit-full-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(name="audit-full-sync", source=source)

    def _run(self, *arguments):
        out = StringIO()
        with patch(
            "forward_netbox.management.commands.forward_device_scope_reconciliation_audit.compute_scope_reconciliation",
            return_value=_report(),
        ):
            call_command(
                "forward_device_scope_reconciliation_audit",
                f"--sync-id={self.sync.pk}",
                *arguments,
                stdout=out,
            )
        return json.loads(out.getvalue())

    def test_the_default_output_stays_sampled(self):
        payload = self._run()
        self.assertNotIn("full", payload)
        self.assertNotIn("_owned_untagged", payload)

    def test_full_prints_every_bucket_in_full(self):
        payload = self._run("--full")
        self.assertEqual(payload["full"]["out_of_scope"], ["orphan-a", "orphan-b"])
        self.assertEqual(payload["full"]["tagged_but_backfilled"], ["backfilled-a"])
        self.assertEqual(payload["full"]["owned_uncovered"], ["gone-a", "gone-b", "gone-c"])
        self.assertEqual(payload["full"]["in_scope_missing_from_netbox"], ["missing-a"])
