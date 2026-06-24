# Scale-validation for the single-branch ingest redesign's merge path.
#
# Heavy sibling of test_bulk_merge.py. Where that file proves the
# bulk_merge_changes contract on ~30 rows, this proves it does NOT silently lose
# rows, stays idempotent, sub-batches its flushes, and produces timing we can
# extrapolate to ~1M, when fed a real branch staged at scale.
#
# Gated behind FORWARD_SCALE_TEST because it is slow (per-object branch staging
# of thousands of rows). Run it explicitly:
#   docker exec forward-netbox-netbox-1 bash -lc \
#     "cd /opt/netbox/netbox && FORWARD_SCALE_TEST=1 FORWARD_SCALE_TEST_ROWS=20000 \
#      python manage.py test --keepdb --noinput \
#      forward_netbox.tests.test_bulk_merge_scale"
#
# It mirrors what ForwardSingleBranchExecutor produces: every row is staged
# per-object inside activate_branch + event_tracking, so each save fires the
# branching post_save -> ObjectChange -> ChangeDiff chain (the forced-adapter
# staging the executor sets via enable_bulk_orm="false"). The sub-batched
# bulk_create + chunked existence check + O((V+E) log V) collapse/order in
# bulk_merge.py are then exercised on a real per-model batch large enough to
# surface lock/RAM/algorithmic blow-ups that 30 rows hide.
import logging
import math
import os
import time
import uuid
from unittest import skipUnless
from unittest.mock import Mock

from dcim.models import Region
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.test import RequestFactory
from django.test import TransactionTestCase
from django.urls import reverse
from netbox.context_managers import event_tracking
from netbox_branching.models import Branch
from netbox_branching.utilities import activate_branch

from forward_netbox.utilities.bulk_merge import bulk_merge_changes
from forward_netbox.utilities.bulk_merge import BULK_MERGE_FLUSH_THRESHOLD

SITE_COUNT = int(os.environ.get("FORWARD_SCALE_TEST_ROWS", "20000"))
REGION_COUNT = 5
# Commit staging in chunks so setup does not hold one giant transaction (setup
# cost is not the measurement target).
STAGE_CHUNK = 2_000


def provision_branch(*, user, name="Test Branch", **kwargs):
    branch = Branch(name=name, **kwargs)
    branch.save(provision=False)
    branch.provision(user=user)
    branch.refresh_from_db()
    return branch


def _rss_mb():
    # Informational only; never asserted (ru_maxrss is KiB on Linux, bytes on
    # macOS, and a memory print must not fail an otherwise-correct merge).
    try:
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:  # pragma: no cover - platform without resource
        return -1.0


@skipUnless(
    os.environ.get("FORWARD_SCALE_TEST"),
    "set FORWARD_SCALE_TEST=1 to run the slow scale validation",
)
class BulkMergeScaleTest(TransactionTestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(
            username="bulk-merge-scale-user"
        )
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.bulk_merge_scale")
        self.logger.setLevel(logging.WARNING)

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

    def _stage_at_scale(self, branch, site_count, region_count):
        t0 = time.monotonic()
        staged = 0
        while staged < site_count:
            n = min(STAGE_CHUNK, site_count - staged)
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                for i in range(staged, staged + n):
                    Site.objects.create(name=f"Scale Site {i}", slug=f"scale-site-{i}")
            staged += n
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            for i in range(region_count):
                Region.objects.create(
                    name=f"Scale Region {i}", slug=f"scale-region-{i}"
                )
        return time.monotonic() - t0

    def test_scale_merge_no_silent_loss_and_idempotent(self):
        branch = provision_branch(user=self.user, name="Bulk Merge Scale")

        stage_secs = self._stage_at_scale(branch, SITE_COUNT, REGION_COUNT)
        total_staged = SITE_COUNT + REGION_COUNT

        self.assertEqual(Site.objects.filter(slug__startswith="scale-site-").count(), 0)
        self.assertEqual(
            Region.objects.filter(slug__startswith="scale-region-").count(), 0
        )

        changes = branch.get_unmerged_changes().order_by("time")
        staged_change_count = changes.count()
        self.assertEqual(
            staged_change_count,
            total_staged,
            f"branch staged {staged_change_count} ObjectChanges, expected "
            f"{total_staged}; staging lost rows",
        )

        # --- First merge ----------------------------------------------------
        apply_one = self._real_apply_one(branch)
        bulk_create_calls = []
        original_bulk_create = Site.objects.bulk_create

        def spy_bulk_create(objs, *a, **k):
            objs = list(objs)
            bulk_create_calls.append(len(objs))
            return original_bulk_create(objs, *a, **k)

        Site.objects.bulk_create = spy_bulk_create
        try:
            t0 = time.monotonic()
            applied, failed, models = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
            )
            merge_secs = time.monotonic() - t0
        finally:
            Site.objects.bulk_create = original_bulk_create

        rss_after_merge = _rss_mb()

        # NO SILENT LOSS.
        self.assertEqual(failed, 0, "merge reported failed rows")
        self.assertEqual(
            applied, total_staged, f"merge applied {applied} of {total_staged} rows"
        )
        landed_sites = Site.objects.filter(slug__startswith="scale-site-").count()
        landed_regions = Region.objects.filter(slug__startswith="scale-region-").count()
        self.assertEqual(landed_sites, SITE_COUNT, "site count mismatch in main")
        self.assertEqual(landed_regions, REGION_COUNT, "region count mismatch in main")
        self.assertEqual(
            Site.objects.filter(slug__startswith="scale-site-")
            .values_list("pk", flat=True)
            .distinct()
            .count(),
            SITE_COUNT,
        )
        self.assertEqual(
            apply_one.call_count,
            REGION_COUNT,
            "apply_one fired for non-region rows (bulk path leaked to fallback)",
        )
        self.assertEqual(sum(bulk_create_calls), SITE_COUNT)
        # Sub-batched: one flush per BULK_MERGE_FLUSH_THRESHOLD rows (not one
        # giant transaction). This is the bounded-RAM/transaction guarantee.
        expected_flushes = math.ceil(SITE_COUNT / BULK_MERGE_FLUSH_THRESHOLD)
        self.assertEqual(
            len(bulk_create_calls),
            expected_flushes,
            f"expected {expected_flushes} sub-batch flushes for {SITE_COUNT} "
            f"sites at threshold {BULK_MERGE_FLUSH_THRESHOLD}, got "
            f"{bulk_create_calls}",
        )
        self.assertTrue(
            all(n <= BULK_MERGE_FLUSH_THRESHOLD for n in bulk_create_calls),
            f"a flush exceeded the threshold: {bulk_create_calls}",
        )

        # --- Re-merge (crash-resume idempotency at scale) -------------------
        # Scoped to dcim.site to isolate the existence-skip contract from the
        # framework's non-idempotent MPTT create-on-existing-pk replay.
        site_ct = ContentType.objects.get_for_model(Site)
        apply_one2 = self._real_apply_one(branch)
        bulk_create_calls2 = []

        def spy_bulk_create2(objs, *a, **k):
            objs = list(objs)
            bulk_create_calls2.append(len(objs))
            return original_bulk_create(objs, *a, **k)

        Site.objects.bulk_create = spy_bulk_create2
        try:
            changes2 = (
                branch.get_unmerged_changes()
                .filter(changed_object_type=site_ct)
                .order_by("time")
            )
            t0 = time.monotonic()
            applied2, failed2, _ = bulk_merge_changes(
                branch,
                changes2,
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one2,
            )
            remerge_secs = time.monotonic() - t0
        finally:
            Site.objects.bulk_create = original_bulk_create

        self.assertEqual(failed2, 0, "re-merge reported failed rows")
        self.assertEqual(applied2, SITE_COUNT, f"re-merge applied {applied2} sites")
        self.assertEqual(
            sum(bulk_create_calls2), 0, "re-merge inserted rows; should skip all"
        )
        self.assertEqual(
            apply_one2.call_count, 0, "re-merge fell back to per-object apply"
        )
        self.assertEqual(
            Site.objects.filter(slug__startswith="scale-site-").count(), SITE_COUNT
        )

        # --- Timing / extrapolation print -----------------------------------
        stage_per_row_ms = (stage_secs / total_staged) * 1000.0
        merge_per_row_ms = (merge_secs / total_staged) * 1000.0
        print("\n==== bulk_merge scale timing ====")
        print(
            f"rows staged          : {total_staged} ({SITE_COUNT} site/{REGION_COUNT} region)"
        )
        print(
            f"staging   wall       : {stage_secs:8.2f}s ({stage_per_row_ms:.3f} ms/row)"
        )
        print(
            f"merge     wall       : {merge_secs:8.2f}s ({merge_per_row_ms:.3f} ms/row)"
        )
        print(f"re-merge  wall       : {remerge_secs:8.2f}s (idempotent skip path)")
        print(
            f"flush sub-batches    : {len(bulk_create_calls)} x <= {BULK_MERGE_FLUSH_THRESHOLD}"
        )
        print(f"peak RSS after merge : {rss_after_merge:.0f} MiB (informational)")
        print(
            f"extrapolated 1M merge: {merge_per_row_ms * 1_000_000 / 1000.0 / 60.0:8.2f} min"
        )
        print(
            f"extrapolated 1M stage: {stage_per_row_ms * 1_000_000 / 1000.0 / 60.0:8.2f} min"
        )
        print("=================================")

    def test_bulk_stage_is_fast_and_lossless(self):
        # Phase 4: stage via the BULK engine (bulk_create + synthesized
        # ObjectChange emission) instead of per-object, and confirm it is both
        # lossless (every row tracked + merged) and dramatically faster than the
        # ~8 ms/row per-object staging measured by the sibling test.
        from django.contrib.contenttypes.models import ContentType
        from netbox.context import current_request
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="Bulk Stage Scale")
        rows = [
            {"name": f"BS Site {i}", "slug": f"bs-site-{i}"} for i in range(SITE_COUNT)
        ]
        runner = Mock()
        token = current_request.set(self.request)
        t0 = time.monotonic()
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                ok = bulk_orm_apply_simple_models(runner, "dcim.site", rows)
        finally:
            current_request.reset(token)
        stage_secs = time.monotonic() - t0
        self.assertTrue(ok)

        site_ct = ContentType.objects.get_for_model(Site)
        unmerged = branch.get_unmerged_changes().filter(changed_object_type=site_ct)
        self.assertEqual(
            unmerged.count(), SITE_COUNT, "bulk staging lost ObjectChanges"
        )

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = Mock()
        t1 = time.monotonic()
        applied, failed, _ = bulk_merge_changes(
            branch, changes, self.request, self.user, self.logger, apply_one=apply_one
        )
        merge_secs = time.monotonic() - t1
        self.assertEqual(failed, 0)
        self.assertEqual(applied, SITE_COUNT)
        self.assertEqual(apply_one.call_count, 0)
        self.assertEqual(
            Site.objects.filter(slug__startswith="bs-site-").count(), SITE_COUNT
        )

        stage_ms = (stage_secs / SITE_COUNT) * 1000.0
        merge_ms = (merge_secs / SITE_COUNT) * 1000.0
        print("\n==== Phase 4 bulk-stage timing ====")
        print(f"rows bulk-staged     : {SITE_COUNT}")
        print(f"bulk staging  wall   : {stage_secs:8.2f}s ({stage_ms:.3f} ms/row)")
        print(f"merge         wall   : {merge_secs:8.2f}s ({merge_ms:.3f} ms/row)")
        print(f"extrapolated 1M stage: {stage_ms * 1_000_000 / 1000.0 / 60.0:8.2f} min")
        print("===================================")
