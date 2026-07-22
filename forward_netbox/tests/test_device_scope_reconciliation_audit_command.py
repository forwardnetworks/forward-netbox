import json
from io import StringIO
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.core.management import call_command
from django.db import connection
from django.test import TestCase
from django.test import TransactionTestCase
from extras.models import Tag

from forward_netbox.models import ForwardDeviceTagClaim
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardPreservedDeviceTagAssignment
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.ownership import reconcile_sync_scope_tag_claims


class ForwardDeviceScopeReconciliationAuditCommandTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="recon-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
                "device_tag_include_tags": ["Prod_Core"],
                "device_tag_include_match": "any",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="recon-sync",
            source=self.source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        mfr = Manufacturer.objects.create(name="MfrR", slug="mfr-r")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-r", slug="dt-r")
        self.role = DeviceRole.objects.create(name="RoleR", slug="role-r")
        self.site = Site.objects.create(name="SiteR", slug="site-r")

    def _make_devices(self, *names):
        for name in names:
            Device.objects.create(
                name=name, device_type=self.dt, role=self.role, site=self.site
            )

    def _claim_scope(self, *names):
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_id="prior-snapshot",
            baseline_ready=True,
        )
        reconcile_sync_scope_tag_claims(
            self.sync,
            {name: ["Prod_Core"] for name in names},
            generation=ingestion.pk,
            snapshot_id=ingestion.snapshot_id,
        )

    def _run(self, rows, **kwargs):
        client = Mock()
        client.run_nqe_query = Mock(return_value=rows)
        out = StringIO()
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            call_command(
                "forward_device_scope_reconciliation_audit",
                "--sync-name",
                "recon-sync",
                stdout=out,
                stderr=StringIO(),
                **kwargs,
            )
        return json.loads(out.getvalue())

    def test_clean_when_netbox_matches_scope(self):
        self._make_devices("dev-a", "dev-b")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-b", "completed": True},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["netbox_device_count"], 2)
        self.assertEqual(payload["forward_in_scope_completed"], 2)
        self.assertEqual(payload["netbox_out_of_scope"], 0)
        self.assertEqual(payload["remediation"], "")

    def test_unowned_netbox_device_is_not_classified_by_this_sync(self):
        self._make_devices("dev-a", "owned-by-another-source")

        payload = self._run([{"name": "dev-a", "completed": True}])

        self.assertEqual(payload["netbox_out_of_scope"], 0)
        self.assertEqual(payload["out_of_scope_sample"], [])
        self.assertTrue(Device.objects.filter(name="owned-by-another-source").exists())

    def test_reports_out_of_scope_and_backfilled(self):
        # dev-a/dev-b completed in scope, dev-c tagged but backfilled, dev-d stale.
        self._make_devices("dev-a", "dev-b", "dev-c", "dev-d")
        self._claim_scope("dev-d")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-b", "completed": True},
            {"name": "dev-c", "completed": False},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["forward_in_scope_completed"], 2)
        self.assertEqual(payload["forward_tagged_backfilled"], 1)
        self.assertEqual(payload["netbox_present_backfilled"], 1)
        self.assertEqual(payload["netbox_out_of_scope"], 1)
        self.assertEqual(payload["out_of_scope_sample"], ["dev-d"])
        self.assertEqual(payload["present_backfilled_sample"], ["dev-c"])
        self.assertIn("device_tag_prune_out_of_scope", payload["remediation"])

    def test_scope_claim_input_excludes_absent_backfilled_devices(self):
        self.source.parameters = {
            **self.source.parameters,
            "apply_device_scope_tags": True,
        }
        self.source.save(update_fields=["parameters"])
        self._make_devices("dev-present")
        rows = [
            {
                "name": "dev-present",
                "completed": True,
                "tagNames": ["Prod_Core"],
            },
            {
                "name": "dev-absent-backfilled",
                "completed": False,
                "tagNames": ["Prod_Core"],
            },
        ]
        client = Mock()
        client.run_nqe_query = Mock(return_value=rows)
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            from forward_netbox.utilities.scope_reconciliation import (
                compute_scope_reconciliation,
            )

            report = compute_scope_reconciliation(self.sync)

        self.assertEqual(report["scope_tag_targets_missing_in_netbox"], 1)
        self.assertEqual(
            report["scope_tag_targets_missing_sample"],
            ["dev-absent-backfilled"],
        )
        self.assertEqual(
            report["_matched_include_tags_by_name"],
            {"dev-present": ["Prod_Core"]},
        )

    def test_fail_on_drift_exits_nonzero(self):
        self._make_devices("dev-a", "dev-stale")
        self._claim_scope("dev-stale")
        rows = [{"name": "dev-a", "completed": True}]
        with self.assertRaises(SystemExit):
            self._run(rows, **{"fail_on_drift": True})

    def test_backfill_reason_breakdown_and_staleness(self):
        # Two in-scope devices fail collection with different reasons; the audit
        # must surface the per-reason breakdown and a per-device stale age so an
        # operator never needs a manual Forward probe to diagnose the gap.
        self._make_devices("dev-ok", "dev-auth", "dev-timeout")
        rows = [
            {"name": "dev-ok", "completed": True},
            {
                "name": "dev-auth",
                "completed": False,
                "reason": "DeviceSnapshotResult.collectionFailed"
                "(DeviceCollectionError.AUTHENTICATION_FAILED)",
                "backfillTime": "2020-01-01T00:00:00Z",
            },
            {
                "name": "dev-timeout",
                "completed": False,
                "reason": "DeviceSnapshotResult.collectionFailed"
                "(DeviceCollectionError.CONNECTION_TIMEOUT)",
                "backfillTime": "2020-01-01T00:00:00Z",
            },
        ]
        payload = self._run(rows)
        self.assertEqual(
            payload["backfilled_reason_breakdown"],
            {"AUTHENTICATION_FAILED": 1, "CONNECTION_TIMEOUT": 1},
        )
        detail = {d["name"]: d for d in payload["present_backfilled_detail_sample"]}
        self.assertEqual(detail["dev-auth"]["reason"], "AUTHENTICATION_FAILED")
        self.assertEqual(detail["dev-timeout"]["reason"], "CONNECTION_TIMEOUT")
        self.assertGreater(detail["dev-auth"]["stale_days"], 0)

    def test_backfill_reason_defaults_unknown_without_reason(self):
        # Older payloads (no reason/backfillTime) must not break: reason -> unknown,
        # stale_days -> None.
        self._make_devices("dev-a", "dev-c")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-c", "completed": False},
        ]
        payload = self._run(rows)
        self.assertEqual(payload["backfilled_reason_breakdown"], {"unknown": 1})
        detail = payload["present_backfilled_detail_sample"][0]
        self.assertEqual(detail["reason"], "unknown")
        self.assertIsNone(detail["stale_days"])

    def test_prune_orphans_dry_run_keeps_devices(self):
        # dev-a completed in scope, dev-c tagged-but-backfilled, dev-d stale.
        self._make_devices("dev-a", "dev-c", "dev-d")
        self._claim_scope("dev-d")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-c", "completed": False},
        ]
        payload = self._run(rows, **{"prune_orphans": True})
        self.assertTrue(payload["prune_requested"])
        self.assertFalse(payload["prune_applied"])
        self.assertEqual(payload["prune_candidate_count"], 1)
        self.assertNotIn("pruned_device_count", payload)
        # Nothing deleted in a dry run.
        self.assertTrue(Device.objects.filter(name="dev-d").exists())
        self.assertTrue(Device.objects.filter(name="dev-c").exists())

    def test_prune_orphans_refuses_on_empty_forward_scope(self):
        # Forward returned 0 scoped devices (failed/empty query) — pruning would
        # treat every NetBox device as an orphan, so it must refuse and delete
        # nothing.
        self._make_devices("dev-a", "dev-b")
        with self.assertRaises(SystemExit):
            self._run([], **{"prune_orphans": True, "apply": True})
        self.assertEqual(Device.objects.count(), 2)

    def test_prune_orphans_apply_deletes_only_out_of_scope(self):
        self._make_devices("dev-a", "dev-c", "dev-d")
        self._claim_scope("dev-d")
        rows = [
            {"name": "dev-a", "completed": True},
            {"name": "dev-c", "completed": False},
        ]
        payload = self._run(rows, **{"prune_orphans": True, "apply": True})
        self.assertTrue(payload["prune_applied"])
        self.assertEqual(payload["pruned_device_count"], 1)
        # Only the untagged stale device is gone; in-scope and backfilled remain.
        self.assertFalse(Device.objects.filter(name="dev-d").exists())
        self.assertTrue(Device.objects.filter(name="dev-a").exists())
        self.assertTrue(Device.objects.filter(name="dev-c").exists())

    def test_prune_blocks_device_still_claimed_by_another_sync(self):
        from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices

        self._make_devices("dev-live", "dev-shared")
        self._claim_scope("dev-shared")
        other_source = ForwardSource.objects.create(
            name="other-recon-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-2",
                "device_tag_include_tags": ["Prod_Core"],
            },
        )
        other_sync = ForwardSync.objects.create(
            name="other-recon-sync",
            source=other_source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        other_ingestion = ForwardIngestion.objects.create(
            sync=other_sync,
            snapshot_id="other-snapshot",
            baseline_ready=True,
        )
        reconcile_sync_scope_tag_claims(
            other_sync,
            {"dev-shared": ["Prod_Core"]},
            generation=other_ingestion.pk,
            snapshot_id=other_ingestion.snapshot_id,
        )
        shared = Device.objects.get(name="dev-shared")
        report = {
            "_out_of_scope": {"dev-shared"},
            "_out_of_scope_pks": [shared.pk],
            "_device_tagged_names": {"dev-live"},
            "_tagged_names": {"dev-live"},
        }

        result = prune_orphan_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["ownership_blocked_device_count"], 1)
        self.assertTrue(Device.objects.filter(pk=shared.pk).exists())
        self.assertEqual(
            ForwardDeviceTagClaim.objects.filter(device=shared).count(),
            2,
        )

    def test_out_of_scope_pks_track_device_identity(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self._make_devices("dev-a", "dev-d")
        self._claim_scope("dev-d")
        rows = [{"name": "dev-a", "completed": True}]
        client = Mock()
        client.run_nqe_query = Mock(return_value=rows)
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            report = compute_scope_reconciliation(self.sync)

        dev_d = Device.objects.get(name="dev-d")
        self.assertEqual(report["_out_of_scope"], {"dev-d"})
        # Identity-aware: the out-of-scope set is resolved to the exact device PK.
        self.assertEqual(list(report["_out_of_scope_pks"]), [dev_d.pk])

    def test_endpoint_import_protects_endpoint_devices_from_orphan_set(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self.source.parameters = {
            **self.source.parameters,
            "sync_endpoints": True,
            "device_tag_exclude_tags": ["Blocked"],
            "scope_endpoints_by_include_tags": False,
        }
        self.source.save(update_fields=["parameters"])
        self._make_devices("dev-a", "endpoint-a", "dev-stale")
        self._claim_scope("dev-stale")
        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-a", "completed": True}],
            [{"name": "endpoint-a"}],
        ]
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            report = compute_scope_reconciliation(self.sync)

        self.assertEqual(report["forward_in_scope_completed"], 1)
        self.assertEqual(report["forward_in_scope_endpoints"], 1)
        self.assertEqual(report["_tagged_names"], {"dev-a", "endpoint-a"})
        self.assertEqual(report["_device_tagged_names"], {"dev-a"})
        self.assertEqual(report["_out_of_scope"], {"dev-stale"})
        endpoint_query = client.run_nqe_query.call_args_list[1].kwargs["query"]
        self.assertIn("foreach endpoint in network.endpoints", endpoint_query)
        self.assertIn("where !isEmpty(endpoint.snmpOutputs)", endpoint_query)
        self.assertIn('where !("Blocked" in endpoint.tagNames)', endpoint_query)
        self.assertNotIn('"Prod_Core" in endpoint.tagNames', endpoint_query)
        self.assertIn("endpoint.profileName", endpoint_query)
        self.assertIn("where !isCimc", endpoint_query)

    def test_endpoint_import_can_require_include_scope_tags(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self.source.parameters = {
            **self.source.parameters,
            "sync_endpoints": True,
            "scope_endpoints_by_include_tags": True,
        }
        self.source.save(update_fields=["parameters"])
        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-a", "completed": True}],
            [{"name": "endpoint-a"}],
        ]
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            report = compute_scope_reconciliation(self.sync)

        self.assertEqual(report["forward_in_scope_endpoints"], 1)
        endpoint_query = client.run_nqe_query.call_args_list[1].kwargs["query"]
        self.assertIn('where ("Prod_Core" in endpoint.tagNames)', endpoint_query)

    def test_endpoint_scope_probe_failure_aborts_reconciliation(self):
        from forward_netbox.exceptions import ForwardQueryError
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self.source.parameters = {**self.source.parameters, "sync_endpoints": True}
        self.source.save(update_fields=["parameters"])
        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-a", "completed": True}],
            ForwardQueryError("endpoint probe failed"),
        ]
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
            self.assertRaisesRegex(ForwardQueryError, "endpoint probe failed"),
        ):
            compute_scope_reconciliation(self.sync)

    def test_endpoint_only_scope_does_not_bypass_prune_guard(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )
        from forward_netbox.utilities.scope_reconciliation import EmptyForwardScopeError
        from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices

        self.source.parameters = {**self.source.parameters, "sync_endpoints": True}
        self.source.save(update_fields=["parameters"])
        self._make_devices("endpoint-a", "dev-stale")
        self._claim_scope("dev-stale")
        client = Mock()
        client.run_nqe_query.side_effect = [[], [{"name": "endpoint-a"}]]
        with (
            patch.object(ForwardSource, "get_client", return_value=client),
            patch.object(ForwardSync, "resolve_snapshot_id", return_value="snap-1"),
        ):
            report = compute_scope_reconciliation(self.sync)

        self.assertEqual(report["_tagged_names"], {"endpoint-a"})
        self.assertEqual(report["_device_tagged_names"], set())
        self.assertEqual(report["_out_of_scope"], {"dev-stale"})
        with self.assertRaises(EmptyForwardScopeError):
            prune_orphan_devices(self.sync, report=report)
        self.assertTrue(Device.objects.filter(name="dev-stale").exists())

    def test_endpoint_only_scope_does_not_bypass_command_prune_guard(self):
        self._make_devices("endpoint-a", "dev-stale")
        report = {
            "netbox_out_of_scope": 1,
            "out_of_scope_sample": ["dev-stale"],
            "_tagged_names": {"endpoint-a"},
            "_device_tagged_names": set(),
            "_out_of_scope": {"dev-stale"},
        }
        with (
            patch(
                "forward_netbox.management.commands."
                "forward_device_scope_reconciliation_audit."
                "compute_scope_reconciliation",
                return_value=report,
            ),
            self.assertRaises(SystemExit),
        ):
            call_command(
                "forward_device_scope_reconciliation_audit",
                "--sync-name",
                "recon-sync",
                "--prune-orphans",
                "--apply",
                stdout=StringIO(),
                stderr=StringIO(),
            )

        self.assertTrue(Device.objects.filter(name="dev-stale").exists())

    def test_prune_preserves_device_with_customer_owned_tag_assignment(self):
        from forward_netbox.utilities.scope_reconciliation import prune_orphan_devices

        self._make_devices("dev-live", "dev-preserved")
        self._claim_scope("dev-preserved")
        preserved = Device.objects.get(name="dev-preserved")
        tag = Tag.objects.create(
            name="Customer Scope",
            slug="customer-scope",
            color="9e9e9e",
        )
        preserved.tags.add(tag)
        ForwardPreservedDeviceTagAssignment.objects.create(
            device=preserved,
            tag=tag,
        )
        report = {
            "_out_of_scope": {preserved.name},
            "_out_of_scope_pks": [preserved.pk],
            "_device_tagged_names": {"dev-live"},
            "_tagged_names": {"dev-live"},
        }

        result = prune_orphan_devices(self.sync, report=report)

        self.assertEqual(result["pruned_device_count"], 0)
        self.assertEqual(result["ownership_blocked_device_count"], 1)
        self.assertEqual(result["protected_device_count"], 0)
        self.assertNotIn("protected_by_model", result)
        self.assertTrue(Device.objects.filter(pk=preserved.pk).exists())
        self.assertTrue(
            ForwardDeviceTagClaim.objects.filter(
                sync=self.sync,
                device=preserved,
                claim_type="scope",
            ).exists()
        )


class RoutingDanglingAuditCommandTest(TransactionTestCase):
    """Exercise the read-only audit with and without the routing plugin."""

    def test_skips_cleanly_without_plugin(self):
        out = StringIO()
        with patch(
            "forward_netbox.management.commands."
            "forward_routing_dangling_audit.apps.is_installed",
            return_value=False,
        ):
            call_command("forward_routing_dangling_audit", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["skipped"], "netbox_routing is not installed")

    def test_reports_danglers_with_plugin(self):
        from django.apps import apps as django_apps

        if not django_apps.is_installed("netbox_routing"):
            self.skipTest("netbox_routing is not installed")
        from django.contrib.contenttypes.models import ContentType

        from dcim.models import Device
        from ipam.models import ASN, RIR

        from forward_netbox.utilities.bulk_delete import (
            DEVICE_GENERIC_RELATION_GUARD_TRIGGER,
        )

        BGPRouter = django_apps.get_model("netbox_routing", "bgprouter")
        device_ct = ContentType.objects.get_for_model(Device)
        asn = ASN.objects.create(
            asn=64513,
            rir=RIR.objects.create(name="Routing Audit Test"),
        )
        table_name = connection.ops.quote_name(BGPRouter._meta.db_table)
        trigger_name = connection.ops.quote_name(DEVICE_GENERIC_RELATION_GUARD_TRIGGER)
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER {trigger_name}")
        try:
            # Seed a pre-0042 legacy row; current writes are guarded by the trigger.
            BGPRouter.objects.create(
                asn=asn,
                assigned_object_type=device_ct,
                assigned_object_id=999999,
            )
        finally:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"ALTER TABLE {table_name} ENABLE TRIGGER {trigger_name}"
                )
        out = StringIO()
        call_command("forward_routing_dangling_audit", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertGreaterEqual(payload["dangling"]["bgprouter"], 1)
