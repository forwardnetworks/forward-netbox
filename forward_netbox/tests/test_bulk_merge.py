# Integration tests for the bulk branch merge (large-dataset ingest redesign).
#
# Provisions a real netbox_branching branch, stages changes into it, and merges
# via bulk_merge_changes — proving net creates apply in a single bulk_create
# while MPTT/tree models fall back to per-object apply, and re-merge is
# idempotent.
import logging
import uuid
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Region
from dcim.models import Site
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.test import RequestFactory
from django.test import TestCase
from django.test import TransactionTestCase
from django.urls import reverse
from netbox.context_managers import event_tracking
from netbox_branching.models import Branch
from netbox_branching.utilities import activate_branch

from forward_netbox.utilities.bulk_merge import bulk_merge_changes


def provision_branch(*, user, name="Test Branch", **kwargs):
    branch = Branch(name=name, **kwargs)
    branch.save(provision=False)
    branch.provision(user=user)
    branch.refresh_from_db()
    return branch


class BulkMergeIntegrationTest(TransactionTestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(username="bulk-merge-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.bulk_merge")

    def _real_apply_one(self, branch):
        def apply_one(collapsed):
            dummy = collapsed.generate_object_change()
            last = collapsed.last_change
            try:
                with transaction.atomic():
                    with event_tracking(self.request):
                        self.request.id = getattr(last, "request_id", None)
                        self.request.user = self.user
                        dummy.apply(branch, using=DEFAULT_DB_ALIAS, logger=self.logger)
                return True
            except Exception:
                return False

        return Mock(side_effect=apply_one)

    def test_bulk_merge_creates_sites_in_bulk_and_mptt_per_object(self):
        branch = provision_branch(user=self.user, name="Bulk Merge")

        # Stage net-new creates into the branch: 30 Sites (bulk-safe) + 1 Region
        # (MPTT, must apply per object).
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            for i in range(30):
                Site.objects.create(name=f"Bulk Site {i}", slug=f"bulk-site-{i}")
            Region.objects.create(name="Bulk Region", slug="bulk-region")

        self.assertEqual(Site.objects.filter(slug__startswith="bulk-site-").count(), 0)

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = self._real_apply_one(branch)
        bulk_create_calls = []
        original_bulk_create = Site.objects.bulk_create

        def spy_bulk_create(objs, *a, **k):
            objs = list(objs)
            bulk_create_calls.append(len(objs))
            return original_bulk_create(objs, *a, **k)

        Site.objects.bulk_create = spy_bulk_create
        try:
            applied, failed, models = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
            )
        finally:
            Site.objects.bulk_create = original_bulk_create

        # All 31 net creates applied to main.
        self.assertEqual(failed, 0)
        self.assertEqual(applied, 31)
        self.assertEqual(Site.objects.filter(slug__startswith="bulk-site-").count(), 30)
        self.assertTrue(Region.objects.filter(slug="bulk-region").exists())
        # Sites went through ONE bulk_create of 30; Region (MPTT) went per-object.
        self.assertEqual(bulk_create_calls, [30])
        self.assertEqual(apply_one.call_count, 1)

    def test_bulk_merge_is_idempotent_on_re_merge(self):
        branch = provision_branch(user=self.user, name="Bulk Merge Idem")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            for i in range(10):
                Site.objects.create(name=f"Idem Site {i}", slug=f"idem-site-{i}")

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = self._real_apply_one(branch)

        bulk_merge_changes(
            branch, changes, self.request, self.user, self.logger, apply_one=apply_one
        )
        self.assertEqual(Site.objects.filter(slug__startswith="idem-site-").count(), 10)

        # Re-run the same merge (simulating a crash-resume): existing pks are
        # skipped, no duplicate-pk error, no per-object fallback noise.
        apply_one2 = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch, changes, self.request, self.user, self.logger, apply_one=apply_one2
        )
        self.assertEqual(failed, 0)
        self.assertEqual(applied, 10)
        self.assertEqual(apply_one2.call_count, 0)
        self.assertEqual(Site.objects.filter(slug__startswith="idem-site-").count(), 10)


class SingleBranchExecutorTest(TransactionTestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        from forward_netbox.models import ForwardSource
        from forward_netbox.models import ForwardSync

        self.user = get_user_model().objects.create_user(username="sbe-user")
        self.source = ForwardSource.objects.create(
            name="sbe-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "u@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="sbe-sync",
            source=self.source,
            auto_merge=True,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _context(self):
        from forward_netbox.utilities.query_fetch_execution import ForwardQueryContext

        return ForwardQueryContext(
            network_id="net-1",
            snapshot_selector="latestProcessed",
            snapshot_id="snap-1",
        )

    def _site_workloads(self):
        from forward_netbox.utilities.branch_budget import BranchWorkload

        return [
            BranchWorkload(
                model_string="dcim.site",
                label="dcim.site | Forward Locations",
                upsert_rows=[
                    {"name": f"SBE Site {i}", "slug": f"sbe-site-{i}"}
                    for i in range(15)
                ],
                coalesce_fields=[["slug"], ["name"]],
                query_name="Forward Locations",
            )
        ]

    def _run_executor(self):
        from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
        from forward_netbox.utilities.single_branch_executor import (
            ForwardSingleBranchExecutor,
        )
        from forward_netbox.utilities.validation import ForwardValidationRunner

        context = self._context()
        workloads = self._site_workloads()
        logger = Mock()
        self.sync.logger = logger
        provision_calls = []
        original_provision = Branch.provision

        def spy_provision(branch_self, *a, **k):
            provision_calls.append(branch_self.pk)
            return original_provision(branch_self, *a, **k)

        with (
            patch.object(ForwardQueryFetcher, "resolve_context", return_value=context),
            patch.object(ForwardQueryFetcher, "run_preflight", return_value=None),
            patch.object(
                ForwardQueryFetcher, "fetch_workloads", return_value=workloads
            ),
            patch.object(
                ForwardValidationRunner, "record_plan_validation", return_value=None
            ),
            patch.object(Branch, "provision", spy_provision),
        ):
            executor = ForwardSingleBranchExecutor(
                self.sync, Mock(), logger, user=self.user
            )
            ingestions = executor.run()
        return ingestions, provision_calls

    def test_single_branch_auto_merge_lands_in_main(self):
        ingestions, provision_calls = self._run_executor()
        # Exactly ONE branch provisioned for the whole sync (not one per shard).
        self.assertEqual(len(provision_calls), 1)
        # All 15 sites merged into main.
        self.assertEqual(Site.objects.filter(slug__startswith="sbe-site-").count(), 15)
        self.assertTrue(ingestions[0].baseline_ready)


class OrderingComplexityTest(TransactionTestCase):
    # Guards the O((V+E) log V) topological sort in bulk_merge against a
    # regression back to the framework's O(V^2) order (which hangs on a single
    # large model batch even with no edges). Pure-algorithm test: stub collapsed
    # changes + patch the framework graph build so only the sort under test runs.
    def _stub_changes(self, n, *, chain):
        from types import SimpleNamespace
        from netbox_branching.merge_strategies.squash import ActionType

        objs = {}
        for i in range(n):
            objs[i] = SimpleNamespace(
                final_action=ActionType.CREATE,
                last_change=SimpleNamespace(time=i),
                depends_on=set(),
                idx=i,
            )
        self._chain = chain
        return objs

    def _fake_build(self, deletes, updates, creates, logger):
        # Populate depends_on to form a single deep chain (i depends on i-1) when
        # self._chain, else leave edge-free (the "O(V^2) even with no edges"
        # case). Keys equal idx, matching the dict keys in _order.
        if not self._chain:
            return
        allobjs = sorted(
            list(deletes) + list(updates) + list(creates), key=lambda o: o.idx
        )
        for o in allobjs:
            if o.idx > 0:
                o.depends_on = {o.idx - 1}

    def _run_order(self, n, *, chain):
        import time
        from unittest.mock import patch
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy
        from forward_netbox.utilities import bulk_merge

        objs = self._stub_changes(n, chain=chain)
        logger = logging.getLogger("forward_netbox.tests.ordering")
        with (
            patch.object(
                SquashMergeStrategy, "_split_bidirectional_cycles", create=True
            ),
            patch.object(
                SquashMergeStrategy,
                "_build_fk_dependency_graph",
                side_effect=self._fake_build,
            ),
            patch.object(bulk_merge.squash_dependency_graph_built, "send"),
        ):
            t0 = time.monotonic()
            ordered = bulk_merge._order_collapsed_changes_fast(objs, logger, "merge")
            elapsed = time.monotonic() - t0
        return ordered, elapsed

    def test_deep_chain_orders_topologically_and_fast(self):
        # 50k-deep chain: the framework's per-layer rescan would be O(V^2) (depth
        # == V), i.e. minutes-to-hours; this must finish in seconds with a valid
        # topological order (each node after its single dependency).
        n = 50_000
        ordered, elapsed = self._run_order(n, chain=True)
        self.assertEqual(len(ordered), n)
        positions = {o.idx: p for p, o in enumerate(ordered)}
        # Chain 0<-1<-2<-...: every node must appear after its predecessor.
        self.assertTrue(all(positions[i] > positions[i - 1] for i in range(1, n)))
        # Generous ceiling: O(V^2) at 50k (2.5e9 ops) cannot meet this; O(V log V)
        # finishes well under it. Catches a complexity regression without flaking.
        self.assertLess(elapsed, 20.0, f"ordering took {elapsed:.2f}s (O(V^2)?)")

    def test_no_edge_batch_orders_fast(self):
        # The probe's headline: the framework sort is O(V^2) EVEN WITH NO EDGES
        # (blanket discard across all remaining nodes per processed key). 50k
        # independent creates (one big model batch) must order in seconds.
        n = 50_000
        ordered, elapsed = self._run_order(n, chain=False)
        self.assertEqual(len(ordered), n)
        self.assertLess(elapsed, 20.0, f"ordering took {elapsed:.2f}s (O(V^2)?)")

    def test_unbreakable_cycle_raises(self):
        # If a real cycle survives (split patched off), the sort must detect it
        # (ordered < input) and raise rather than silently drop nodes.
        from types import SimpleNamespace
        from unittest.mock import patch
        from netbox_branching.merge_strategies.squash import ActionType
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy
        from forward_netbox.utilities import bulk_merge

        a = SimpleNamespace(
            final_action=ActionType.CREATE,
            last_change=SimpleNamespace(time=0),
            depends_on={1},
            idx=0,
        )
        b = SimpleNamespace(
            final_action=ActionType.CREATE,
            last_change=SimpleNamespace(time=1),
            depends_on={0},
            idx=1,
        )
        objs = {0: a, 1: b}
        logger = logging.getLogger("forward_netbox.tests.ordering")
        with (
            patch.object(
                SquashMergeStrategy, "_split_bidirectional_cycles", create=True
            ),
            patch.object(SquashMergeStrategy, "_build_fk_dependency_graph"),
            patch.object(SquashMergeStrategy, "_log_cycle_details", create=True),
            patch.object(bulk_merge.squash_dependency_graph_built, "send"),
        ):
            with self.assertRaises(Exception):
                bulk_merge._order_collapsed_changes_fast(objs, logger, "merge")


class SkipMissingBatchedTest(TransactionTestCase):
    # Validates _skip_updates_missing_in_main_batched: the batched replacement for
    # the framework's one-.exists()-per-UPDATE (N+1) skip-missing pass. Proves it
    # (a) marks UPDATEs whose object is gone from main as SKIP, (b) leaves
    # present-in-main UPDATEs untouched, and (c) does it in O(models) queries, not
    # O(updates) — the regression guard against reintroducing the N+1.
    def test_skip_missing_marks_skip_and_is_batched(self):
        from types import SimpleNamespace
        from netbox_branching.merge_strategies.squash import ActionType
        from forward_netbox.utilities import bulk_merge

        logger = logging.getLogger("forward_netbox.tests.skip_missing")
        sites = [Site.objects.create(name=f"sk {i}", slug=f"sk-{i}") for i in range(10)]
        present_pks = [s.pk for s in sites]
        missing_pks = [max(present_pks) + 1000 + i for i in range(5)]

        collapsed = {}
        for pk in present_pks + missing_pks:
            collapsed[("dcim.site", pk)] = SimpleNamespace(
                final_action=ActionType.UPDATE,
                model_class=Site,
                key=("dcim.site", pk),
            )

        # One model -> exactly one existence query (chunked pk__in), NOT 15.
        with self.assertNumQueries(1):
            bulk_merge._skip_updates_missing_in_main_batched(collapsed, logger)

        for pk in present_pks:
            self.assertEqual(
                collapsed[("dcim.site", pk)].final_action,
                ActionType.UPDATE,
                "present-in-main UPDATE was wrongly skipped",
            )
        for pk in missing_pks:
            self.assertEqual(
                collapsed[("dcim.site", pk)].final_action,
                ActionType.SKIP,
                "missing-in-main UPDATE was not skipped",
            )

    def test_skip_missing_chunks_large_update_sets(self):
        # More UPDATEs than the chunk threshold must still be O(ceil(n/chunk))
        # queries, not O(n). Use a model count spanning multiple chunks.
        from types import SimpleNamespace
        from netbox_branching.merge_strategies.squash import ActionType
        from forward_netbox.utilities import bulk_merge

        logger = logging.getLogger("forward_netbox.tests.skip_missing")
        n = bulk_merge.BULK_MERGE_FLUSH_THRESHOLD * 2 + 17  # spans 3 chunks
        # All missing in main (no rows created) -> all become SKIP.
        collapsed = {
            ("dcim.site", pk): SimpleNamespace(
                final_action=ActionType.UPDATE,
                model_class=Site,
                key=("dcim.site", pk),
            )
            for pk in range(1, n + 1)
        }
        expected_queries = -(-n // bulk_merge.BULK_MERGE_FLUSH_THRESHOLD)  # ceil
        with self.assertNumQueries(expected_queries):
            bulk_merge._skip_updates_missing_in_main_batched(collapsed, logger)
        self.assertTrue(
            all(c.final_action == ActionType.SKIP for c in collapsed.values())
        )


class Phase4BulkStageTest(TransactionTestCase):
    # Phase 4 proof: bulk_create into a branch + emit_branch_object_changes
    # records the core.ObjectChange rows the merge replays from, so bulk-staged
    # rows are NO LONGER silently lost. Drives the real bulk engine under
    # active_branch, then merges, asserting every bulk-staged row lands in main
    # with its branch pk and WITHOUT any per-object fallback.
    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(username="p4-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.phase4")

    def _runner(self):
        return Mock()

    def test_bulk_stage_emits_objectchanges_and_merges(self):
        from django.contrib.contenttypes.models import ContentType
        from netbox.context import current_request
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="P4 Bulk Stage")
        n = 20
        rows = [{"name": f"P4 Site {i}", "slug": f"p4-site-{i}"} for i in range(n)]

        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                ok = bulk_orm_apply_simple_models(runner, "dcim.site", rows)
        finally:
            current_request.reset(token)

        self.assertTrue(ok)
        # Nothing in main until merge.
        self.assertEqual(Site.objects.filter(slug__startswith="p4-site-").count(), 0)

        # KEY: bulk staging synthesized one CREATE ObjectChange per row in the
        # branch (without this, the merge would see nothing and drop every row).
        site_ct = ContentType.objects.get_for_model(Site)
        unmerged = branch.get_unmerged_changes().filter(changed_object_type=site_ct)
        self.assertEqual(unmerged.count(), n, "bulk staging did not record N changes")
        self.assertTrue(all(c.action == "create" for c in unmerged))
        branch_pks = set(unmerged.values_list("changed_object_id", flat=True))

        # Merge: every row lands in main, via the bulk path (no fallback), with
        # the SAME pk it had in the branch.
        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = Mock()
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=apply_one,
        )
        self.assertEqual(failed, 0)
        self.assertEqual(applied, n)
        self.assertEqual(apply_one.call_count, 0, "bulk-staged rows hit the fallback")
        landed = Site.objects.filter(slug__startswith="p4-site-")
        self.assertEqual(landed.count(), n)
        self.assertEqual(set(landed.values_list("pk", flat=True)), branch_pks)


class DeferredSelfRefFKSplitTest(TestCase):
    """The deferred self-referential FK split that breaks the device.primary_ip4
    create-cycle (device -> primary_ip -> assigned interface -> device)."""

    def _device_create(self, pk, data):
        from types import SimpleNamespace

        from dcim.models import Device
        from netbox_branching.merge_strategies.squash import CollapsedChange

        from forward_netbox.utilities.bulk_merge import ActionType

        cc = CollapsedChange(("dcim.device", pk), Device)
        cc.final_action = ActionType.CREATE
        cc.postchange_data = dict(data)
        cc.last_change = SimpleNamespace(time=pk, pk=pk, request_id=uuid.uuid4())
        return cc

    def test_splits_primary_ip_into_trailing_update(self):
        from forward_netbox.utilities.bulk_merge import ActionType
        from forward_netbox.utilities.bulk_merge import (
            _defer_self_referential_create_fks,
        )

        dev = self._device_create(
            7129, {"name": "sw1", "primary_ip4": 29340, "primary_ip6": None}
        )
        to_process = {dev.key: dev}
        _defer_self_referential_create_fks(to_process, logging.getLogger("t"))

        # primary_ip4 is nulled on the CREATE so it forms no create-ordering edge.
        self.assertIsNone(dev.postchange_data["primary_ip4"])
        # a synthetic trailing UPDATE carries the value and depends on the CREATE.
        upd_key = ("dcim.device", 7129, "defer_self_ref_fk")
        self.assertIn(upd_key, to_process)
        upd = to_process[upd_key]
        self.assertEqual(upd.final_action, ActionType.UPDATE)
        self.assertEqual(upd.postchange_data["primary_ip4"], 29340)
        self.assertIn(dev.key, upd.depends_on)

    def test_noop_without_deferred_fk_value(self):
        from forward_netbox.utilities.bulk_merge import (
            _defer_self_referential_create_fks,
        )

        dev = self._device_create(1, {"name": "sw2", "primary_ip4": None})
        to_process = {dev.key: dev}
        _defer_self_referential_create_fks(to_process, logging.getLogger("t"))
        # No deferred value present -> no synthetic update, create untouched.
        self.assertEqual(list(to_process), [("dcim.device", 1)])

    def test_noop_for_non_deferred_model(self):
        from types import SimpleNamespace

        from dcim.models import Interface
        from netbox_branching.merge_strategies.squash import CollapsedChange

        from forward_netbox.utilities.bulk_merge import ActionType
        from forward_netbox.utilities.bulk_merge import (
            _defer_self_referential_create_fks,
        )

        cc = CollapsedChange(("dcim.interface", 5), Interface)
        cc.final_action = ActionType.CREATE
        cc.postchange_data = {"name": "Gi0/0", "device": 7129}
        cc.last_change = SimpleNamespace(time=5, pk=5, request_id=uuid.uuid4())
        to_process = {cc.key: cc}
        _defer_self_referential_create_fks(to_process, logging.getLogger("t"))
        self.assertEqual(list(to_process), [("dcim.interface", 5)])

    def test_splits_interface_lag_into_trailing_update(self):
        # A LAG member interface (lag -> parent interface) must defer its self-ref
        # FK, else its full_clean dereferences a same-batch, not-yet-committed
        # parent during the bulk-merge flush (Interface.DoesNotExist).
        from types import SimpleNamespace

        from dcim.models import Interface
        from netbox_branching.merge_strategies.squash import CollapsedChange

        from forward_netbox.utilities.bulk_merge import ActionType
        from forward_netbox.utilities.bulk_merge import (
            _defer_self_referential_create_fks,
        )

        cc = CollapsedChange(("dcim.interface", 2540), Interface)
        cc.final_action = ActionType.CREATE
        cc.postchange_data = {"name": "Ethernet1", "device": 7, "lag": 99}
        cc.last_change = SimpleNamespace(time=1, pk=1, request_id=uuid.uuid4())
        to_process = {cc.key: cc}
        _defer_self_referential_create_fks(to_process, logging.getLogger("t"))

        self.assertIsNone(cc.postchange_data["lag"])
        upd_key = ("dcim.interface", 2540, "defer_self_ref_fk")
        self.assertIn(upd_key, to_process)
        self.assertEqual(to_process[upd_key].postchange_data["lag"], 99)
        self.assertIn(cc.key, to_process[upd_key].depends_on)


class BulkMergeTagNameCollisionTest(TransactionTestCase):
    """A branch tag CREATE that collides by NAME with a main-side tag skips.

    While a merge applies device UPDATEs (ordered before CREATEs),
    netbox_branching sets device tags by name, get_or_creating the tag on main
    with a new pk. The branch's tag CREATE then violated the unique name
    constraint and surfaced as a ValidationError ingestion issue even though the
    desired end state (tag exists) was already reached.
    """

    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(username="tag-merge-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.bulk_merge")

    def test_tag_create_name_collision_skips_instead_of_failing(self):
        from extras.models import Tag

        branch = provision_branch(user=self.user, name="Tag Collision")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            Tag.objects.create(name="Mgmt_Coll", slug="mgmt-coll")

        # Simulate the mid-merge name-based get_or_create on main: same name,
        # different pk than the branch row.
        Tag.objects.create(name="Mgmt_Coll", slug="mgmt-coll")

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = Mock(return_value=False)  # fallback would record an issue
        applied, failed, _models = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=apply_one,
        )

        self.assertEqual(failed, 0)
        # The colliding create must be skipped in the batch path, never routed
        # to the per-object fallback (which would raise the ValidationError).
        for call in apply_one.call_args_list:
            collapsed = call.args[0]
            self.assertNotEqual(collapsed.key[0], "extras.tag")
        self.assertEqual(Tag.objects.filter(name="Mgmt_Coll").count(), 1)


class BranchingCompatCycleSplitTest(TestCase):
    """Ordering must survive netbox_branching 1.1.1, which removed
    SquashMergeStrategy._split_bidirectional_cycles (sync died with
    AttributeError). The vendored splitter must break a bidirectional CREATE
    pair (Device.virtual_chassis <-> VirtualChassis.master) the same way."""

    def _bidirectional_pair(self):
        from types import SimpleNamespace

        from dcim.models import Device
        from dcim.models import VirtualChassis
        from netbox_branching.merge_strategies.squash import CollapsedChange

        from forward_netbox.utilities.bulk_merge import ActionType

        dev = CollapsedChange(("dcim.device", 1), Device)
        dev.final_action = ActionType.CREATE
        dev.postchange_data = {"name": "vc-member", "virtual_chassis": 5}
        dev.last_change = SimpleNamespace(time=1, pk=1, request_id=uuid.uuid4())

        vc = CollapsedChange(("dcim.virtualchassis", 5), VirtualChassis)
        vc.final_action = ActionType.CREATE
        vc.postchange_data = {"name": "vc-1", "master": 1}
        vc.last_change = SimpleNamespace(time=2, pk=2, request_id=uuid.uuid4())
        return {dev.key: dev, vc.key: vc}

    def test_vendored_splitter_breaks_bidirectional_create_pair(self):
        from forward_netbox.utilities.bulk_merge import (
            _split_bidirectional_create_cycles,
        )

        to_process = self._bidirectional_pair()
        _split_bidirectional_create_cycles(to_process, logging.getLogger("compat-test"))

        synthetic = [k for k in to_process if len(k) == 3]
        self.assertEqual(len(synthetic), 1)
        nulled = [
            c
            for c in to_process.values()
            if c.final_action.value == "create"
            and c.postchange_data.get("virtual_chassis") is None
            or c.final_action.value == "create"
            and c.postchange_data.get("master") is None
        ]
        self.assertTrue(nulled, "one CREATE of the pair must have its FK NULLed")

    def test_ordering_succeeds_when_framework_splitter_is_absent(self):
        # Simulate netbox_branching 1.1.1, which removed the class attribute
        # (delete it for the duration of the ordering call, then restore).
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy

        from forward_netbox.utilities.bulk_merge import (
            _order_collapsed_changes_fast,
        )

        to_process = self._bidirectional_pair()
        original = SquashMergeStrategy.__dict__.get("_split_bidirectional_cycles")
        if original is not None:
            delattr(SquashMergeStrategy, "_split_bidirectional_cycles")
        try:
            self.assertFalse(
                hasattr(SquashMergeStrategy, "_split_bidirectional_cycles")
            )
            ordered = _order_collapsed_changes_fast(
                to_process, logging.getLogger("compat-test"), operation="merge"
            )
        finally:
            if original is not None:
                setattr(SquashMergeStrategy, "_split_bidirectional_cycles", original)
        # The function orders an internal copy: 2 CREATEs plus the synthetic
        # UPDATEs added by the cycle splitter(s). Success = no cycle exception
        # and both original keys present in the ordering.
        ordered_keys = {c.key[:2] for c in ordered}
        for key in to_process:
            self.assertIn(key[:2], ordered_keys)
