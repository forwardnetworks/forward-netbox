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
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


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

    def test_reports_out_of_scope_and_backfilled(self):
        # dev-a/dev-b completed in scope, dev-c tagged but backfilled, dev-d stale.
        self._make_devices("dev-a", "dev-b", "dev-c", "dev-d")
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

    def test_fail_on_drift_exits_nonzero(self):
        self._make_devices("dev-a", "dev-stale")
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

    def test_out_of_scope_pks_track_device_identity(self):
        from forward_netbox.utilities.scope_reconciliation import (
            compute_scope_reconciliation,
        )

        self._make_devices("dev-a", "dev-d")
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
            "scope_endpoints_by_include_tags_configured": True,
        }
        self.source.save(update_fields=["parameters"])
        self._make_devices("dev-a", "endpoint-a", "dev-stale")
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


class PruneProtectorSweepTest(TestCase):
    """Pruning devices must sweep PROTECT-ing optional-plugin rows instead of
    failing wholesale (field report: netbox_routing BGP peers protect the
    interface IPs of pruned devices, and the single-transaction prune rolled
    back everything with ProtectedError).
    """

    def setUp(self):
        mfr = Manufacturer.objects.create(name="MfrP", slug="mfr-p")
        self.dt = DeviceType.objects.create(manufacturer=mfr, model="dt-p", slug="dt-p")
        self.role = DeviceRole.objects.create(name="RoleP", slug="role-p")
        self.site = Site.objects.create(name="SiteP", slug="site-p")

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.dt, role=self.role, site=self.site
        )

    def test_grouping_orders_children_before_parents_and_unknown_last(self):
        from forward_netbox.utilities.scope_reconciliation import (
            _group_protected_objects_by_rank,
        )

        peer = Mock()
        peer._meta.label_lower = "netbox_routing.bgppeer"
        session = Mock()
        session._meta.label_lower = "netbox_peering_manager.peeringsession"
        stranger = Mock()
        stranger._meta.label_lower = "someplugin.unknownmodel"
        groups = _group_protected_objects_by_rank([peer, stranger, session])
        labels = [label for label, _objects in groups]
        # Delete order: peering sessions before the BGP peers they protect;
        # models without a known rank go last.
        self.assertEqual(
            labels,
            [
                "netbox_peering_manager.peeringsession",
                "netbox_routing.bgppeer",
                "someplugin.unknownmodel",
            ],
        )

    def test_sweep_deletes_blockers_and_retries(self):
        from django.db.models.deletion import ProtectedError

        from forward_netbox.utilities import scope_reconciliation

        # Two real rows stand in for PROTECT-ing plugin objects (the sweep only
        # needs __class__ + pk, so any deletable model works without plugins).
        blocker_a = self._device("blocker-a")
        blocker_b = self._device("blocker-b")
        target = self._device("prune-me")

        real_delete = Device.objects.filter(pk__in=[target.pk]).delete
        calls = {"n": 0}

        class FakeManagerProxy:
            def filter(self, **kwargs):
                proxy = Mock()

                def _delete():
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise ProtectedError(
                            "Cannot delete some instances of model 'Device'",
                            {blocker_a, blocker_b},
                        )
                    return real_delete()

                proxy.delete = _delete
                return proxy

        fake_device = Mock()
        fake_device.objects = FakeManagerProxy()
        with patch.object(scope_reconciliation, "Device", fake_device):
            deleted, tally = scope_reconciliation._delete_devices_sweeping_protectors(
                [target.pk]
            )

        # Blockers swept via their real model manager, then the retry succeeded.
        self.assertFalse(Device.objects.filter(name="blocker-a").exists())
        self.assertFalse(Device.objects.filter(name="blocker-b").exists())
        self.assertFalse(Device.objects.filter(name="prune-me").exists())
        self.assertEqual(deleted, 1)
        self.assertEqual(tally, {"dcim.device": 2})

    def test_sweep_cap_reraises_with_tally(self):
        from django.db.models.deletion import ProtectedError

        from forward_netbox.utilities import scope_reconciliation

        class AlwaysProtected:
            def filter(self, **kwargs):
                proxy = Mock()

                def _delete():
                    # Empty protected set: nothing sweepable, so the loop can
                    # never make progress and must hit the cap.
                    raise ProtectedError("still protected", set())

                proxy.delete = _delete
                return proxy

        fake_device = Mock()
        fake_device.objects = AlwaysProtected()
        with patch.object(scope_reconciliation, "Device", fake_device):
            with self.assertRaises(ProtectedError) as ctx:
                scope_reconciliation._delete_devices_sweeping_protectors([1])
        self.assertIn("sweep passes", str(ctx.exception.args[0]))


class DanglingRoutingCleanupTest(TestCase):
    """GenericFK routing rows (BGPRouter -> scopes -> AFs/peers -> settings)
    never block a device delete, so they dangle after a prune. The sweep is
    scoped to THIS run's pruned device pks and is plugin-absent safe.
    CI has no netbox_routing, so the plugin path is exercised via mocks.
    """

    def test_absent_plugin_returns_empty(self):
        from forward_netbox.utilities.scope_reconciliation import (
            _cleanup_dangling_routing_objects,
        )

        # netbox_routing genuinely absent in CI: early return, no model access.
        self.assertEqual(_cleanup_dangling_routing_objects([1, 2]), {})

    def test_empty_pks_short_circuits(self):
        from forward_netbox.utilities.scope_reconciliation import (
            _cleanup_dangling_routing_objects,
        )

        with patch("django.apps.apps.is_installed") as is_installed:
            self.assertEqual(_cleanup_dangling_routing_objects([]), {})
        is_installed.assert_not_called()

    def test_sweep_order_and_tally_with_mocked_plugin(self):
        from forward_netbox.utilities import scope_reconciliation

        deletions = []

        def make_model(label, list_pks=(), delete_count=1):
            model = Mock()
            model._meta.label_lower = label
            manager = Mock()

            def _filter(**kwargs):
                qs = Mock()
                qs.values_list = Mock(return_value=list(list_pks))

                def _delete():
                    deletions.append((label, kwargs))
                    return (delete_count, {})

                qs.delete = _delete
                return qs

            manager.filter = _filter
            model.objects = manager
            return model

        models = {
            "BGPRouter": make_model("netbox_routing.bgprouter", [10]),
            "BGPScope": make_model("netbox_routing.bgpscope", [20]),
            "BGPAddressFamily": make_model("netbox_routing.bgpaddressfamily", [30]),
            "BGPPeer": make_model("netbox_routing.bgppeer", [40]),
            "BGPSetting": make_model("netbox_routing.bgpsetting"),
            "BGPPeerAddressFamily": make_model("netbox_routing.bgppeeraddressfamily"),
        }

        with (
            patch("django.apps.apps.is_installed", return_value=True),
            patch(
                "django.apps.apps.get_model",
                side_effect=lambda app, name: models[name],
            ),
            patch(
                "forward_netbox.utilities.scope_reconciliation.transaction.atomic",
            ),
            patch(
                "django.contrib.contenttypes.models.ContentType.objects"
            ) as ct_objects,
        ):
            ct_objects.get_for_model = Mock(side_effect=lambda m: f"ct:{m}")
            tally = scope_reconciliation._cleanup_dangling_routing_objects([1, 2])

        deleted_labels = [label for label, _kwargs in deletions]
        # Children first: peer-AFs -> settings (per parent CT) -> peers -> AFs
        # -> scopes -> routers.
        self.assertEqual(deleted_labels[0], "netbox_routing.bgppeeraddressfamily")
        self.assertEqual(
            deleted_labels[-4:],
            [
                "netbox_routing.bgppeer",
                "netbox_routing.bgpaddressfamily",
                "netbox_routing.bgpscope",
                "netbox_routing.bgprouter",
            ],
        )
        self.assertTrue(
            all(label == "netbox_routing.bgpsetting" for label in deleted_labels[1:-4])
        )
        # Tally keyed by label_lower; every mocked delete returned 1.
        self.assertEqual(tally["netbox_routing.bgprouter"], 1)
        self.assertEqual(tally["netbox_routing.bgppeer"], 1)

    def test_prune_result_carries_dangling_tally(self):
        from forward_netbox.utilities import scope_reconciliation

        mfr = Manufacturer.objects.create(name="MfrD", slug="mfr-d")
        dt = DeviceType.objects.create(manufacturer=mfr, model="dt-d", slug="dt-d")
        role = DeviceRole.objects.create(name="RoleD", slug="role-d")
        site = Site.objects.create(name="SiteD", slug="site-d")
        orphan = Device.objects.create(
            name="dangle-orphan", device_type=dt, role=role, site=site
        )
        report = {
            "_out_of_scope": {"dangle-orphan"},
            "_tagged_names": {"kept-device"},
            "_out_of_scope_pks": [orphan.pk],
        }
        with patch.object(
            scope_reconciliation,
            "_cleanup_dangling_routing_objects",
            return_value={"netbox_routing.bgprouter": 2},
        ):
            result = scope_reconciliation.prune_orphan_devices(Mock(), report=report)
        self.assertEqual(
            result["pruned_dangling_rows"], {"netbox_routing.bgprouter": 2}
        )
        self.assertFalse(Device.objects.filter(name="dangle-orphan").exists())


class DanglingRoutingCleanupRealSchemaTest(TestCase):
    """Real-schema exercise of the dangler sweep. netbox_routing does not
    support the NetBox 4.6 baseline yet, so this is skip-gated: it starts
    running (and closes the mock-only coverage gap) the moment the plugin
    becomes installable in CI."""

    def test_sweep_against_real_models(self):
        from django.apps import apps as django_apps

        if not django_apps.is_installed("netbox_routing"):
            self.skipTest("netbox_routing is not installed")
        from django.contrib.contenttypes.models import ContentType

        from dcim.models import Device
        from forward_netbox.utilities.scope_reconciliation import (
            _cleanup_dangling_routing_objects,
        )

        BGPRouter = django_apps.get_model("netbox_routing", "bgprouter")
        device_ct = ContentType.objects.get_for_model(Device)
        # A dangling router: its GenericFK points at a device pk that no
        # longer exists.
        router = BGPRouter.objects.create(
            assigned_object_type=device_ct, assigned_object_id=999999
        )
        tally = _cleanup_dangling_routing_objects({999999})
        self.assertFalse(BGPRouter.objects.filter(pk=router.pk).exists())
        self.assertGreaterEqual(sum(tally.values()), 1)


class RoutingDanglingAuditCommandTest(TestCase):
    """Read-only dangling audit: clean skip without the plugin; real-schema
    path is skip-gated until netbox_routing supports the 4.6 baseline."""

    def test_skips_cleanly_without_plugin(self):
        from django.apps import apps as django_apps

        if django_apps.is_installed("netbox_routing"):
            self.skipTest("netbox_routing installed; covered by the real test")
        out = StringIO()
        call_command("forward_routing_dangling_audit", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["skipped"], "netbox_routing is not installed")

    def test_reports_danglers_with_plugin(self):
        from django.apps import apps as django_apps

        if not django_apps.is_installed("netbox_routing"):
            self.skipTest("netbox_routing is not installed")
        from django.contrib.contenttypes.models import ContentType

        from dcim.models import Device

        BGPRouter = django_apps.get_model("netbox_routing", "bgprouter")
        device_ct = ContentType.objects.get_for_model(Device)
        BGPRouter.objects.create(
            assigned_object_type=device_ct, assigned_object_id=999999
        )
        out = StringIO()
        call_command("forward_routing_dangling_audit", stdout=out)
        payload = json.loads(out.getvalue())
        self.assertGreaterEqual(payload["dangling"]["bgprouter"], 1)
