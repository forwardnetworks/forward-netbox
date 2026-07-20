# Integration tests for the production single-branch bulk merge.
#
# Provisions a real netbox_branching branch, stages changes into it, and merges
# via bulk_merge_changes, proving batched writes, bounded framework fallbacks,
# atomic audit evidence, relationship convergence, and idempotent retries.
import logging
import threading
import time
import uuid
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Cable
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import InventoryItem
from dcim.models import Manufacturer
from dcim.models import Module
from dcim.models import ModuleBay
from dcim.models import ModuleType
from dcim.models import Platform
from dcim.models import Region
from dcim.models import Site
from django.db import connection
from django.db import DEFAULT_DB_ALIAS
from django.db import transaction
from django.test import RequestFactory
from django.test import TestCase
from django.test import TransactionTestCase
from django.urls import reverse
from ipam.models import Prefix
from netbox.context import current_request
from netbox.context_managers import event_tracking
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff
from netbox_branching.utilities import activate_branch

from forward_netbox.exceptions import ForwardPartialMergeError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.bulk_merge import bulk_merge_changes
from forward_netbox.utilities.merge import merge_branch


def provision_branch(*, user, name="Test Branch", **kwargs):
    branch = Branch(name=name, **kwargs)
    branch.save(provision=False)
    branch.provision(user=user)
    branch.refresh_from_db()
    return branch


def close_test_connections():
    """Close default and Branching's dynamic aliases in the current thread."""
    from django.db import connections
    from netbox_branching.utilities import _get_tracked_branch_aliases

    for alias in tuple(_get_tracked_branch_aliases()):
        connections[alias].close()
    connections.close_all()


class CleanTransactionTestCase(TransactionTestCase):
    """Keep NetBox request-local audit state isolated across database flushes."""

    @classmethod
    def _pre_setup(cls):
        current_request.set(None)
        super()._pre_setup()

    def _post_teardown(self):
        current_request.set(None)
        try:
            super()._post_teardown()
        finally:
            close_test_connections()
            current_request.set(None)


class BulkMergeIntegrationTest(CleanTransactionTestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(username="bulk-merge-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.bulk_merge")

    def _real_apply_one(self, branch):
        def apply_one(collapsed):
            from core.models import ObjectChange
            from django.db.models.signals import post_save
            from netbox_branching.utilities import record_applied_change

            dummy = collapsed.generate_object_change()
            last = collapsed.last_change

            def handler(instance, **kwargs):
                record_applied_change(instance, branch)

            post_save.connect(handler, sender=ObjectChange, weak=False)
            try:
                with transaction.atomic():
                    with event_tracking(self.request):
                        self.request.id = getattr(last, "request_id", None)
                        self.request.user = self.user
                        dummy.apply(branch, using=DEFAULT_DB_ALIAS, logger=self.logger)
                return True
            except Exception:
                return False
            finally:
                post_save.disconnect(handler, sender=ObjectChange)

        return Mock(side_effect=apply_one)

    def _ingestion_for_branch(self, branch, suffix):
        source = ForwardSource.objects.create(
            name=f"bulk-merge-source-{suffix}",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "timeout": 1200,
                "network_id": f"bulk-merge-{suffix}",
            },
        )
        sync = ForwardSync.objects.create(
            name=f"bulk-merge-sync-{suffix}",
            source=source,
            user=self.user,
            auto_merge=False,
            parameters={"snapshot_id": "LATEST_PROCESSED", "dcim.site": True},
        )
        return ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="LATEST_PROCESSED",
            snapshot_id=f"snapshot-{suffix}",
            branch=branch,
        )

    def _stage_device_cycle_with_primary_ip(self, branch, suffix):
        from dcim.models import VirtualChassis
        from ipam.models import IPAddress

        manufacturer = Manufacturer.objects.create(
            name=f"Cycle Manufacturer {suffix}",
            slug=f"cycle-manufacturer-{suffix}",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model=f"Cycle Device Type {suffix}",
            slug=f"cycle-device-type-{suffix}",
        )
        role = DeviceRole.objects.create(
            name=f"Cycle Role {suffix}",
            slug=f"cycle-role-{suffix}",
        )
        site = Site.objects.create(
            name=f"Cycle Site {suffix}",
            slug=f"cycle-site-{suffix}",
        )
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            chassis = VirtualChassis.objects.create(name=f"cycle-vc-{suffix}")
            device = Device.objects.create(
                name=f"cycle-device-{suffix}",
                device_type=device_type,
                role=role,
                site=site,
                virtual_chassis=chassis,
                vc_position=1,
            )
            interface = Interface.objects.create(
                device=device,
                name="Loopback0",
                type="virtual",
            )
            address = IPAddress.objects.create(
                address=f"198.51.100.{suffix}/32",
                assigned_object=interface,
            )
            device.primary_ip4 = address
            device.save(update_fields=["primary_ip4"])
            chassis.master = device
            chassis.save(update_fields=["master"])
        return device.pk, chassis.pk, address.pk

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
        # verified/converged, with no duplicate-pk or per-object fallback noise.
        apply_one2 = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch, changes, self.request, self.user, self.logger, apply_one=apply_one2
        )
        self.assertEqual(failed, 0)
        self.assertEqual(applied, 10)
        self.assertEqual(apply_one2.call_count, 0)
        self.assertEqual(Site.objects.filter(slug__startswith="idem-site-").count(), 10)

    def test_tag_create_precedes_device_tag_update_and_preserves_primary_key(self):
        from extras.models import Tag
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy

        manufacturer = Manufacturer.objects.create(
            name="Tag Order Manufacturer",
            slug="tag-order-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Tag Order Model",
            slug="tag-order-model",
        )
        role = DeviceRole.objects.create(
            name="Tag Order Role",
            slug="tag-order-role",
        )
        site = Site.objects.create(name="Tag Order Site", slug="tag-order-site")
        device = Device.objects.create(
            name="tag-order-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        branch = provision_branch(user=self.user, name="Tag Before Device Update")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged_tag = Tag.objects.create(
                name="Forward Include",
                slug="forward-include",
                color="00ff00",
            )
            Device.objects.get(pk=device.pk).tags.add(staged_tag)

        changes = branch.get_unmerged_changes().order_by("time")
        collapsed, _ = SquashMergeStrategy._collapse_changes(changes, self.logger)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )

        self.assertEqual((applied, failed), (len(collapsed), 0))
        self.assertEqual(Tag.objects.filter(name="Forward Include").count(), 1)
        main_tag = Tag.objects.get(name="Forward Include")
        self.assertEqual(main_tag.pk, staged_tag.pk)
        self.assertEqual(
            set(Device.objects.get(pk=device.pk).tags.values_list("pk", flat=True)),
            {staged_tag.pk},
        )

    def test_production_merge_initializes_exact_per_model_totals(self):
        branch = provision_branch(user=self.user, name="Exact Merge Model Totals")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            for index in range(3):
                Site.objects.create(
                    name=f"Counted Site {index}",
                    slug=f"counted-site-{index}",
                )
            for index in range(2):
                Region.objects.create(
                    name=f"Counted Region {index}",
                    slug=f"counted-region-{index}",
                )

        ingestion = self._ingestion_for_branch(branch, "model-totals")
        sync_logger = Mock()
        merge_branch(ingestion, sync_logger=sync_logger, user=self.user)

        sync_logger.init_statistics.assert_any_call("dcim.site", total=3)
        sync_logger.init_statistics.assert_any_call("dcim.region", total=2)
        self.assertEqual(sync_logger.init_statistics.call_count, 2)

    def test_bulk_create_resume_does_not_resurrect_deleted_row(self):
        branch = provision_branch(user=self.user, name="Deleted Create Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Site.objects.create(
                name="Deleted Resume Site",
                slug="deleted-resume-site",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (1, 0))

        with event_tracking(self.request):
            Site.objects.get(pk=staged.pk).delete()
        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (0, 1))
        resumed_apply.assert_not_called()
        self.assertFalse(Site.objects.filter(pk=staged.pk).exists())

    def test_deleted_resume_persists_issue_without_invoking_apply(self):
        branch = provision_branch(user=self.user, name="Deleted Resume Issue")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged = Site.objects.create(
                name="Deleted Resume Issue Site",
                slug="deleted-resume-issue-site",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        Site.objects.get(pk=staged.pk).delete()
        ingestion = self._ingestion_for_branch(branch, "deleted-resume")

        with (
            patch("netbox_branching.models.ObjectChange.apply") as unsafe_apply,
            self.assertRaises(ForwardPartialMergeError),
        ):
            merge_branch(ingestion, user=self.user)

        unsafe_apply.assert_not_called()
        self.assertFalse(Site.objects.filter(pk=staged.pk).exists())
        issue = ForwardIngestionIssue.objects.get(ingestion=ingestion)
        self.assertEqual(issue.phase, "merge")
        self.assertEqual(issue.model, "dcim.site")
        self.assertIn("Refusing to recreate deleted branch-owned", issue.message)

    def test_diverged_resume_persists_issue_without_invoking_apply(self):
        branch = provision_branch(user=self.user, name="Diverged Resume Issue")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged = Site.objects.create(
                name="Diverged Resume Issue Site",
                slug="diverged-resume-issue-site",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        Site.objects.filter(pk=staged.pk).update(slug="operator-owned-slug")
        ingestion = self._ingestion_for_branch(branch, "diverged-resume")

        with (
            patch("netbox_branching.models.ObjectChange.apply") as unsafe_apply,
            self.assertRaises(ForwardPartialMergeError),
        ):
            merge_branch(ingestion, user=self.user)

        unsafe_apply.assert_not_called()
        self.assertEqual(Site.objects.get(pk=staged.pk).slug, "operator-owned-slug")
        issue = ForwardIngestionIssue.objects.get(ingestion=ingestion)
        self.assertEqual(issue.phase, "merge")
        self.assertEqual(issue.model, "dcim.site")
        self.assertIn(
            "no longer matches its latest branch-applied audit", issue.message
        )

    def test_bulk_create_resume_holds_database_confirmed_row_lock(self):
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        branch = provision_branch(user=self.user, name="Bulk Locked Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Site.objects.create(
                name="Bulk Locked Site",
                slug="bulk-locked-site",
            )

        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (1, 0))

        lock_acquired = threading.Event()
        release_resume = threading.Event()
        update_started = threading.Event()
        update_finished = threading.Event()
        results = {}
        errors = []
        real_assert = bulk_merge_module._assert_existing_create_resume_provenance

        def hold_after_lock(target, locked_branch, **kwargs):
            lock_acquired.set()
            if not release_resume.wait(timeout=10):
                raise RuntimeError("timed out waiting to release bulk resume lock")
            return real_assert(target, locked_branch, **kwargs)

        def resume():
            close_test_connections()
            try:
                with patch.object(
                    bulk_merge_module,
                    "_assert_existing_create_resume_provenance",
                    side_effect=hold_after_lock,
                ):
                    results["resume"] = bulk_merge_changes(
                        branch,
                        branch.get_unmerged_changes().order_by("time"),
                        self.request,
                        self.user,
                        self.logger,
                        apply_one=self._real_apply_one(branch),
                    )[:2]
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def update():
            close_test_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    results["update_pid"] = cursor.fetchone()[0]
                update_started.set()
                Site.objects.filter(pk=staged.pk).update(slug="bulk-concurrent-update")
                update_finished.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        resume_thread = threading.Thread(target=resume)
        update_thread = threading.Thread(target=update)
        resume_thread.start()
        self.assertTrue(lock_acquired.wait(timeout=10))
        update_thread.start()
        self.assertTrue(update_started.wait(timeout=10))
        deadline = time.monotonic() + 10
        blocked_by_resume = False
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cardinality(pg_blocking_pids(%s)) > 0",
                    [results["update_pid"]],
                )
                blocked_by_resume = cursor.fetchone()[0]
            if blocked_by_resume:
                break
            time.sleep(0.05)
        self.assertTrue(
            blocked_by_resume,
            "updater never entered the batched path's database row-lock wait",
        )
        self.assertFalse(update_finished.is_set())

        release_resume.set()
        resume_thread.join(timeout=10)
        update_thread.join(timeout=10)

        self.assertFalse(resume_thread.is_alive())
        self.assertFalse(update_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results["resume"], (1, 0))
        self.assertTrue(update_finished.is_set())
        self.assertEqual(
            Site.objects.get(pk=staged.pk).slug,
            "bulk-concurrent-update",
        )

    def test_bulk_create_resume_rejects_concurrent_tag_removal(self):
        from extras.models import Tag
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        tag = Tag.objects.create(name="Resume Tag", slug="resume-tag")
        branch = provision_branch(user=self.user, name="Tagged Locked Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Site.objects.create(
                name="Tagged Locked Site",
                slug="tagged-locked-site",
            )
            staged.tags.add(tag)

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        initial_provenance_checked = threading.Event()
        release_resume = threading.Event()
        removal_started = threading.Event()
        removal_finished = threading.Event()
        results = {}
        errors = []
        real_assert = bulk_merge_module._assert_existing_create_resume_provenance

        def hold_after_initial_provenance(target, locked_branch, **kwargs):
            latest_change = real_assert(target, locked_branch, **kwargs)
            if not initial_provenance_checked.is_set():
                initial_provenance_checked.set()
                if not release_resume.wait(timeout=10):
                    raise RuntimeError("timed out waiting to release tagged resume")
            return latest_change

        def resume():
            close_test_connections()
            try:
                resumed_apply = self._real_apply_one(branch)
                results["resumed_apply"] = resumed_apply
                with patch.object(
                    bulk_merge_module,
                    "_assert_existing_create_resume_provenance",
                    side_effect=hold_after_initial_provenance,
                ):
                    results["resume"] = bulk_merge_changes(
                        branch,
                        changes,
                        self.request,
                        self.user,
                        self.logger,
                        apply_one=resumed_apply,
                    )[:2]
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def remove_tag():
            close_test_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    results["removal_pid"] = cursor.fetchone()[0]
                removal_started.set()
                Site.objects.get(pk=staged.pk).tags.remove(Tag.objects.get(pk=tag.pk))
                removal_finished.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        resume_thread = threading.Thread(target=resume)
        removal_thread = threading.Thread(target=remove_tag)
        resume_thread.start()
        self.assertTrue(initial_provenance_checked.wait(timeout=10))
        removal_thread.start()
        self.assertTrue(removal_started.wait(timeout=10))
        deadline = time.monotonic() + 10
        blocked_by_relationship_barrier = False
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cardinality(pg_blocking_pids(%s)) > 0",
                    [results["removal_pid"]],
                )
                blocked_by_relationship_barrier = cursor.fetchone()[0]
            if blocked_by_relationship_barrier:
                break
            time.sleep(0.05)
        self.assertTrue(
            blocked_by_relationship_barrier,
            "tag removal never entered the relationship-table lock wait",
        )
        self.assertFalse(removal_finished.is_set())
        release_resume.set()
        resume_thread.join(timeout=10)
        removal_thread.join(timeout=10)

        self.assertFalse(resume_thread.is_alive())
        self.assertFalse(removal_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results["resume"], (1, 0))
        results["resumed_apply"].assert_not_called()
        self.assertTrue(removal_finished.is_set())
        self.assertFalse(Site.objects.get(pk=staged.pk).tags.exists())

    def test_bulk_create_resume_relationship_queries_are_batch_bounded(self):
        from django.test.utils import CaptureQueriesContext
        from extras.models import Tag

        tag = Tag.objects.create(name="Batch Tag", slug="batch-tag")
        branch = provision_branch(user=self.user, name="Tagged Batch Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            for index in range(25):
                site = Site.objects.create(
                    name=f"Tagged Batch Site {index}",
                    slug=f"tagged-batch-site-{index}",
                )
                site.tags.add(tag)

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (25, 0))

        with CaptureQueriesContext(connection) as queries:
            applied, failed, _ = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=self._real_apply_one(branch),
            )

        self.assertEqual((applied, failed), (25, 0))
        through_table = Site.tags.through._meta.db_table.lower()
        relationship_queries = [
            query["sql"]
            for query in queries.captured_queries
            if through_table in query["sql"].lower()
        ]
        self.assertLessEqual(
            len(relationship_queries),
            3,
            "relationship query count must scale with batches, not rows",
        )

    def test_relationship_write_barrier_serializes_holders_without_deadlock(self):
        from extras.models import Tag
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        site = Site.objects.create(name="Barrier Site", slug="barrier-site")
        first_tag = Tag.objects.create(name="Barrier First", slug="barrier-first")
        second_tag = Tag.objects.create(name="Barrier Second", slug="barrier-second")
        first_acquired = threading.Event()
        release_first = threading.Event()
        second_started = threading.Event()
        second_acquired = threading.Event()
        results = {}
        errors = []

        def first_holder():
            close_test_connections()
            try:
                with transaction.atomic():
                    bulk_merge_module._lock_relationship_writes(Site, ["tags"])
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_backend_pid()")
                        results["first_pid"] = cursor.fetchone()[0]
                    Site.objects.get(pk=site.pk).tags.add(first_tag)
                    first_acquired.set()
                    if not release_first.wait(timeout=10):
                        raise RuntimeError("timed out waiting to release first barrier")
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def second_holder():
            close_test_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    results["second_pid"] = cursor.fetchone()[0]
                second_started.set()
                with transaction.atomic():
                    bulk_merge_module._lock_relationship_writes(Site, ["tags"])
                    second_acquired.set()
                    Site.objects.get(pk=site.pk).tags.add(second_tag)
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        first_thread = threading.Thread(target=first_holder)
        second_thread = threading.Thread(target=second_holder)
        first_thread.start()
        self.assertTrue(first_acquired.wait(timeout=10))
        second_thread.start()
        self.assertTrue(second_started.wait(timeout=10))
        time.sleep(0.1)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE pid = %s
                      AND relation = %s::regclass
                      AND mode = 'ShareRowExclusiveLock'
                      AND granted
                )
                """,
                [results["first_pid"], Site.tags.through._meta.db_table],
            )
            self.assertTrue(cursor.fetchone()[0])
        self.assertFalse(second_acquired.is_set())

        release_first.set()
        first_thread.join(timeout=10)
        second_thread.join(timeout=10)

        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(second_acquired.is_set())
        self.assertEqual(
            set(Site.objects.get(pk=site.pk).tags.values_list("pk", flat=True)),
            {first_tag.pk, second_tag.pk},
        )

    def test_relationship_barrier_retries_reverse_order_multitable_locks(self):
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        tables = sorted(
            {
                Interface.tagged_vlans.through._meta.db_table,
                Interface.tags.through._meta.db_table,
            }
        )
        self.assertEqual(len(tables), 2)
        last_table_locked = threading.Event()
        helper_started = threading.Event()
        reverse_order_locks_acquired = threading.Event()
        helper_finished = threading.Event()
        errors = []

        def reverse_order_holder():
            close_test_connections()
            try:
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"LOCK TABLE {connection.ops.quote_name(tables[1])} "
                            "IN ROW EXCLUSIVE MODE"
                        )
                    last_table_locked.set()
                    if not helper_started.wait(timeout=10):
                        raise RuntimeError("timed out waiting for barrier attempt")
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"LOCK TABLE {connection.ops.quote_name(tables[0])} "
                            "IN ROW EXCLUSIVE MODE"
                        )
                    reverse_order_locks_acquired.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def barrier_holder():
            close_test_connections()
            try:
                with transaction.atomic():
                    helper_started.set()
                    bulk_merge_module._lock_relationship_writes(
                        Interface,
                        ["tagged_vlans", "tags"],
                    )
                    helper_finished.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        reverse_thread = threading.Thread(target=reverse_order_holder)
        barrier_thread = threading.Thread(target=barrier_holder)
        reverse_thread.start()
        self.assertTrue(last_table_locked.wait(timeout=10))
        barrier_thread.start()
        self.assertTrue(helper_started.wait(timeout=10))
        self.assertTrue(
            reverse_order_locks_acquired.wait(timeout=10),
            "failed barrier attempt retained an earlier table lock",
        )

        reverse_thread.join(timeout=10)
        barrier_thread.join(timeout=10)

        self.assertFalse(reverse_thread.is_alive())
        self.assertFalse(barrier_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(helper_finished.is_set())

    def test_resume_takes_relationship_barrier_before_row_lock(self):
        from extras.models import Tag
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        branch_tag = Tag.objects.create(name="Branch Tag", slug="branch-tag")
        concurrent_tag = Tag.objects.create(
            name="Concurrent Tag",
            slug="concurrent-tag",
        )
        branch = provision_branch(user=self.user, name="Barrier Before Row")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Site.objects.create(
                name="Barrier Before Row Site",
                slug="barrier-before-row-site",
            )
            staged.tags.add(branch_tag)

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        before_barrier = threading.Event()
        release_barrier = threading.Event()
        writer_added = threading.Event()
        release_writer = threading.Event()
        writer_started = threading.Event()
        results = {}
        errors = []
        real_lock = bulk_merge_module._lock_relationship_writes

        def hold_before_barrier(model_class, field_names):
            before_barrier.set()
            if not release_barrier.wait(timeout=10):
                raise RuntimeError("timed out waiting to acquire relationship barrier")
            return real_lock(model_class, field_names)

        def resume():
            close_test_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    results["resume_pid"] = cursor.fetchone()[0]
                resumed_apply = self._real_apply_one(branch)
                results["resumed_apply"] = resumed_apply
                with patch.object(
                    bulk_merge_module,
                    "_lock_relationship_writes",
                    side_effect=hold_before_barrier,
                ):
                    results["resume"] = bulk_merge_changes(
                        branch,
                        changes,
                        self.request,
                        self.user,
                        self.logger,
                        apply_one=resumed_apply,
                    )[:2]
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def insert_relationship():
            close_test_connections()
            try:
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_backend_pid()")
                        results["writer_pid"] = cursor.fetchone()[0]
                    writer_started.set()
                    Site.objects.get(pk=staged.pk).tags.add(concurrent_tag)
                    writer_added.set()
                    if not release_writer.wait(timeout=10):
                        raise RuntimeError("timed out waiting to commit relationship")
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        resume_thread = threading.Thread(target=resume)
        writer_thread = threading.Thread(target=insert_relationship)
        resume_thread.start()
        self.assertTrue(before_barrier.wait(timeout=10))
        writer_thread.start()
        self.assertTrue(writer_started.wait(timeout=10))
        self.assertTrue(
            writer_added.wait(timeout=10),
            "relationship insert was blocked by a prematurely acquired row lock",
        )

        release_barrier.set()
        time.sleep(0.1)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_locks
                    WHERE pid = %s
                      AND relation = %s::regclass
                      AND mode = 'RowExclusiveLock'
                      AND granted
                )
                """,
                [results["writer_pid"], Site.tags.through._meta.db_table],
            )
            self.assertTrue(cursor.fetchone()[0])
        self.assertNotIn("resume", results)

        release_writer.set()
        writer_thread.join(timeout=10)
        resume_thread.join(timeout=10)

        self.assertFalse(writer_thread.is_alive())
        self.assertFalse(resume_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results["resume"], (0, 1))
        results["resumed_apply"].assert_not_called()
        self.assertTrue(
            Site.objects.get(pk=staged.pk).tags.filter(pk=concurrent_tag.pk).exists()
        )

    def test_resume_does_not_deadlock_parent_then_relationship_writer(self):
        from extras.models import Tag
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        branch_tag = Tag.objects.create(
            name="Parent Writer Branch Tag",
            slug="parent-writer-branch-tag",
        )
        concurrent_tag = Tag.objects.create(
            name="Parent Writer Concurrent Tag",
            slug="parent-writer-concurrent-tag",
        )
        branch = provision_branch(user=self.user, name="Parent Then Relationship")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Site.objects.create(
                name="Parent Then Relationship Site",
                slug="parent-then-relationship-site",
            )
            staged.tags.add(branch_tag)

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        parent_locked = threading.Event()
        barrier_acquired = threading.Event()
        release_resume = threading.Event()
        writer_attempting_relationship = threading.Event()
        writer_finished = threading.Event()
        results = {}
        errors = []
        real_lock = bulk_merge_module._lock_relationship_writes

        def hold_first_barrier(model_class, field_names):
            result = real_lock(model_class, field_names)
            if not barrier_acquired.is_set():
                barrier_acquired.set()
                if not release_resume.wait(timeout=10):
                    raise RuntimeError("timed out waiting to release resume barrier")
            return result

        def update_parent_then_relationship():
            close_test_connections()
            try:
                with transaction.atomic():
                    target = Site.objects.select_for_update().get(pk=staged.pk)
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT pg_backend_pid()")
                        results["writer_pid"] = cursor.fetchone()[0]
                    parent_locked.set()
                    if not barrier_acquired.wait(timeout=10):
                        raise RuntimeError("timed out waiting for resume barrier")
                    writer_attempting_relationship.set()
                    target.tags.add(concurrent_tag)
                writer_finished.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def resume():
            close_test_connections()
            try:
                resumed_apply = self._real_apply_one(branch)
                results["resumed_apply"] = resumed_apply
                with patch.object(
                    bulk_merge_module,
                    "_lock_relationship_writes",
                    side_effect=hold_first_barrier,
                ):
                    results["resume"] = bulk_merge_changes(
                        branch,
                        changes,
                        self.request,
                        self.user,
                        self.logger,
                        apply_one=resumed_apply,
                    )[:2]
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        writer_thread = threading.Thread(target=update_parent_then_relationship)
        resume_thread = threading.Thread(target=resume)
        writer_thread.start()
        self.assertTrue(parent_locked.wait(timeout=10))
        resume_thread.start()
        self.assertTrue(barrier_acquired.wait(timeout=10))
        self.assertTrue(writer_attempting_relationship.wait(timeout=10))
        deadline = time.monotonic() + 10
        writer_blocked_by_barrier = False
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cardinality(pg_blocking_pids(%s)) > 0",
                    [results["writer_pid"]],
                )
                writer_blocked_by_barrier = cursor.fetchone()[0]
            if writer_blocked_by_barrier:
                break
            time.sleep(0.05)
        self.assertTrue(writer_blocked_by_barrier)
        self.assertFalse(writer_finished.is_set())

        release_resume.set()
        writer_thread.join(timeout=10)
        resume_thread.join(timeout=10)

        self.assertFalse(writer_thread.is_alive())
        self.assertFalse(resume_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(writer_finished.is_set())
        self.assertEqual(results["resume"], (0, 1))
        results["resumed_apply"].assert_not_called()
        self.assertTrue(
            Site.objects.get(pk=staged.pk).tags.filter(pk=concurrent_tag.pk).exists()
        )

    def test_mptt_create_is_idempotent_on_crash_resume(self):
        branch = provision_branch(user=self.user, name="MPTT Create Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            region = Region.objects.create(
                name="Resume Region",
                slug="resume-region",
            )
            role = DeviceRole.objects.create(
                name="Resume Role",
                slug="resume-role",
            )
            platform = Platform.objects.create(
                name="Resume Platform",
                slug="resume-platform",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (3, 0))
        self.assertEqual(first_apply.call_count, 3)

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (3, 0))
        self.assertEqual(resumed_apply.call_count, 0)
        self.assertEqual(Region.objects.get(pk=region.pk).slug, "resume-region")
        self.assertEqual(DeviceRole.objects.get(pk=role.pk).slug, "resume-role")
        self.assertEqual(Platform.objects.get(pk=platform.pk).slug, "resume-platform")

    def test_mptt_create_resume_does_not_resurrect_deleted_row(self):
        branch = provision_branch(user=self.user, name="MPTT Deleted Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Region.objects.create(
                name="Deleted Resume Region",
                slug="deleted-resume-region",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (1, 0))

        with event_tracking(self.request):
            Region.objects.get(pk=staged.pk).delete()
        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (0, 1))
        resumed_apply.assert_not_called()
        self.assertFalse(Region.objects.filter(pk=staged.pk).exists())

    def test_mptt_create_resume_rejects_unrelated_same_pk(self):
        from netbox_branching.models import AppliedChange

        branch = provision_branch(user=self.user, name="MPTT Unrelated PK")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Region.objects.create(
                name="Staged Region",
                slug="staged-region",
            )

        unrelated = Region.objects.create(
            pk=staged.pk,
            name="Unrelated Region",
            slug="unrelated-region",
        )
        apply_one = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=apply_one,
        )

        self.assertEqual((applied, failed), (0, 1))
        apply_one.assert_not_called()
        unrelated.refresh_from_db()
        self.assertEqual(unrelated.slug, "unrelated-region")
        self.assertFalse(AppliedChange.objects.filter(branch=branch).exists())

    def test_mptt_create_resume_rejects_state_without_matching_audit(self):
        branch = provision_branch(user=self.user, name="MPTT Diverged Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Region.objects.create(
                name="Audited Region",
                slug="audited-region",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (1, 0))

        Region.objects.filter(pk=staged.pk).update(slug="untracked-divergence")
        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (0, 1))
        resumed_apply.assert_not_called()
        self.assertEqual(
            Region.objects.get(pk=staged.pk).slug,
            "untracked-divergence",
        )

    def test_create_resume_accepts_equivalent_decimal_audit_representation(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType

        branch = provision_branch(user=self.user, name="Decimal Audit Resume")
        manufacturer = Manufacturer.objects.create(
            name="Decimal Audit Manufacturer",
            slug="decimal-audit-manufacturer",
        )
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = DeviceType.objects.create(
                manufacturer=manufacturer,
                model="Decimal Audit Device Type",
                slug="decimal-audit-device-type",
                u_height=1,
            )

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        audit = ObjectChange.objects.filter(
            changed_object_type=ContentType.objects.get_for_model(DeviceType),
            changed_object_id=staged.pk,
            action="create",
        ).latest("pk")
        self.assertEqual(audit.postchange_data["u_height"], "1")
        self.assertEqual(
            DeviceType.objects.get(pk=staged.pk).serialize_object(
                exclude=["created", "last_updated"]
            )["u_height"],
            "1.0",
        )

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (1, 0))
        resumed_apply.assert_not_called()
        self.assertEqual(DeviceType.objects.get(pk=staged.pk).u_height, 1)

    def test_create_resume_rejects_semantically_different_json_values(self):
        manufacturer = Manufacturer.objects.create(
            name="JSON Audit Manufacturer",
            slug="json-audit-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="JSON Audit Device Type",
            slug="json-audit-device-type",
        )
        role = DeviceRole.objects.create(
            name="JSON Audit Role",
            slug="json-audit-role",
        )
        site = Site.objects.create(name="JSON Audit Site", slug="json-audit-site")
        branch = provision_branch(user=self.user, name="JSON Audit Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Device.objects.create(
                name="json-audit-device",
                device_type=device_type,
                role=role,
                site=site,
                local_context_data={"flag": True},
            )

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        Device.objects.filter(pk=staged.pk).update(local_context_data={"flag": 1})
        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (0, 1))
        resumed_apply.assert_not_called()
        self.assertEqual(
            Device.objects.get(pk=staged.pk).local_context_data,
            {"flag": 1},
        )

    def test_create_resume_accepts_branch_owned_inventory_count_change(self):
        manufacturer = Manufacturer.objects.create(
            name="Inventory Resume Manufacturer",
            slug="inventory-resume-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Inventory Resume Device Type",
            slug="inventory-resume-device-type",
        )
        role = DeviceRole.objects.create(
            name="Inventory Resume Role",
            slug="inventory-resume-role",
        )
        site = Site.objects.create(
            name="Inventory Resume Site",
            slug="inventory-resume-site",
        )
        branch = provision_branch(user=self.user, name="Inventory Count Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Device.objects.create(
                name="inventory-resume-device",
                device_type=device_type,
                role=role,
                site=site,
            )
            InventoryItem.objects.create(
                device=staged,
                name="branch-owned-inventory-item",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (2, 0))
        self.assertEqual(
            Device.objects.get(pk=staged.pk).serialize_object(
                exclude=["created", "last_updated"]
            )["inventory_item_count"],
            1,
        )

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (2, 0))
        resumed_apply.assert_not_called()

    def test_create_resume_accepts_branch_owned_module_bay_count_change(self):
        manufacturer = Manufacturer.objects.create(
            name="Module Bay Resume Manufacturer",
            slug="module-bay-resume-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Module Bay Resume Device Type",
            slug="module-bay-resume-device-type",
        )
        role = DeviceRole.objects.create(
            name="Module Bay Resume Role",
            slug="module-bay-resume-role",
        )
        site = Site.objects.create(
            name="Module Bay Resume Site",
            slug="module-bay-resume-site",
        )
        branch = provision_branch(user=self.user, name="Module Bay Count Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Device.objects.create(
                name="module-bay-resume-device",
                device_type=device_type,
                role=role,
                site=site,
            )
            ModuleBay.objects.create(
                device=staged,
                name="Slot 1",
                position="1",
            )

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (2, 0))
        self.assertEqual(
            Device.objects.get(pk=staged.pk).serialize_object(
                exclude=["created", "last_updated"]
            )["module_bay_count"],
            1,
        )

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (2, 0))
        resumed_apply.assert_not_called()

    def test_create_resume_accepts_branch_owned_cable_termination_change(self):
        manufacturer = Manufacturer.objects.create(
            name="Cable Resume Manufacturer",
            slug="cable-resume-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Cable Resume Device Type",
            slug="cable-resume-device-type",
        )
        role = DeviceRole.objects.create(
            name="Cable Resume Role",
            slug="cable-resume-role",
        )
        site = Site.objects.create(
            name="Cable Resume Site",
            slug="cable-resume-site",
        )
        left_device = Device.objects.create(
            name="cable-resume-left",
            device_type=device_type,
            role=role,
            site=site,
        )
        right_device = Device.objects.create(
            name="cable-resume-right",
            device_type=device_type,
            role=role,
            site=site,
        )
        branch = provision_branch(user=self.user, name="Cable Termination Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            left = Interface.objects.create(
                device=left_device,
                name="Ethernet1",
                type="1000base-t",
            )
            right = Interface.objects.create(
                device=right_device,
                name="Ethernet1",
                type="1000base-t",
            )
            cable = Cable.objects.create(
                a_terminations=[left],
                b_terminations=[right],
            )

        changes = branch.get_unmerged_changes().order_by("time")
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        logical_total = applied
        self.assertGreater(logical_total, 0)
        self.assertEqual(failed, 0)
        self.assertEqual(Cable.objects.get(pk=cable.pk).terminations.count(), 2)

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (logical_total, 0))
        resumed_apply.assert_not_called()

    def test_create_resume_converges_semantically_different_desired_json(self):
        manufacturer = Manufacturer.objects.create(
            name="Desired JSON Manufacturer",
            slug="desired-json-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Desired JSON Device Type",
            slug="desired-json-device-type",
        )
        role = DeviceRole.objects.create(
            name="Desired JSON Role",
            slug="desired-json-role",
        )
        site = Site.objects.create(
            name="Desired JSON Site",
            slug="desired-json-site",
        )
        branch = provision_branch(user=self.user, name="Desired JSON Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Device.objects.create(
                name="desired-json-device",
                device_type=device_type,
                role=role,
                site=site,
                local_context_data={"flag": True},
            )

        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
        )
        self.assertEqual((applied, failed), (1, 0))

        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            desired = Device.objects.get(pk=staged.pk)
            desired.local_context_data = {"flag": 1}
            desired.save(update_fields=["local_context_data"])

        resumed_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=resumed_apply,
        )

        self.assertEqual((applied, failed), (1, 0))
        resumed_apply.assert_not_called()
        self.assertEqual(
            Device.objects.get(pk=staged.pk).local_context_data,
            {"flag": 1},
        )

    def test_mptt_create_resume_locks_out_concurrent_update(self):
        from forward_netbox.utilities import bulk_merge as bulk_merge_module

        branch = provision_branch(user=self.user, name="MPTT Locked Resume")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged = Region.objects.create(
                name="Locked Region",
                slug="locked-region",
            )

        first_apply = self._real_apply_one(branch)
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=first_apply,
        )
        self.assertEqual((applied, failed), (1, 0))

        lock_acquired = threading.Event()
        release_resume = threading.Event()
        update_started = threading.Event()
        update_finished = threading.Event()
        results = {}
        errors = []
        real_assert = bulk_merge_module._assert_existing_create_resume_provenance

        def hold_after_lock(target, locked_branch):
            lock_acquired.set()
            if not release_resume.wait(timeout=10):
                raise RuntimeError("timed out waiting to release resume lock")
            return real_assert(target, locked_branch)

        def resume():
            close_test_connections()
            try:
                with patch.object(
                    bulk_merge_module,
                    "_assert_existing_create_resume_provenance",
                    side_effect=hold_after_lock,
                ):
                    results["resume"] = bulk_merge_changes(
                        branch,
                        branch.get_unmerged_changes().order_by("time"),
                        self.request,
                        self.user,
                        self.logger,
                        apply_one=self._real_apply_one(branch),
                    )[:2]
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        def update():
            close_test_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    results["update_pid"] = cursor.fetchone()[0]
                update_started.set()
                Region.objects.filter(pk=staged.pk).update(slug="concurrent-update")
                update_finished.set()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                close_test_connections()

        resume_thread = threading.Thread(target=resume)
        update_thread = threading.Thread(target=update)
        resume_thread.start()
        self.assertTrue(lock_acquired.wait(timeout=10))
        update_thread.start()
        self.assertTrue(update_started.wait(timeout=10))
        deadline = time.monotonic() + 10
        blocked_by_resume = False
        while time.monotonic() < deadline:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cardinality(pg_blocking_pids(%s)) > 0",
                    [results["update_pid"]],
                )
                blocked_by_resume = cursor.fetchone()[0]
            if blocked_by_resume:
                break
            time.sleep(0.05)
        self.assertTrue(
            blocked_by_resume,
            "updater never entered a database-confirmed row-lock wait",
        )
        self.assertFalse(update_finished.is_set())

        release_resume.set()
        resume_thread.join(timeout=10)
        update_thread.join(timeout=10)

        self.assertFalse(resume_thread.is_alive())
        self.assertFalse(update_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results["resume"], (1, 0))
        self.assertTrue(update_finished.is_set())
        self.assertEqual(
            Region.objects.get(pk=staged.pk).slug,
            "concurrent-update",
        )

    def test_cycle_and_deferred_fk_followups_count_as_one_logical_create(self):
        from dcim.models import VirtualChassis
        from ipam.models import IPAddress
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy

        branch = provision_branch(user=self.user, name="Combined Followups")
        device_pk, chassis_pk, address_pk = self._stage_device_cycle_with_primary_ip(
            branch,
            41,
        )
        changes = branch.get_unmerged_changes().order_by("time")
        collapsed, _ = SquashMergeStrategy._collapse_changes(changes, self.logger)
        recorded_models = []

        applied, failed, _models = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=self._real_apply_one(branch),
            record_applied=recorded_models.append,
        )

        self.assertEqual((applied, failed), (len(collapsed), 0))
        self.assertEqual(len(recorded_models), len(collapsed))
        device = Device.objects.get(pk=device_pk)
        chassis = VirtualChassis.objects.get(pk=chassis_pk)
        address = IPAddress.objects.get(pk=address_pk)
        self.assertEqual(device.virtual_chassis_id, chassis.pk)
        self.assertEqual(device.primary_ip4_id, address.pk)
        self.assertEqual(chassis.master_id, device.pk)

    def test_second_internal_followup_failure_counts_original_once(self):
        from core.choices import ObjectChangeActionChoices
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy

        from forward_netbox.utilities import bulk_merge as bulk_merge_module
        from forward_netbox.utilities.bulk_merge import _ApplyOneFailure

        branch = provision_branch(user=self.user, name="Combined Followup Failure")
        device_pk, _chassis_pk, _address_pk = self._stage_device_cycle_with_primary_ip(
            branch, 42
        )
        changes = branch.get_unmerged_changes().order_by("time")
        collapsed, _ = SquashMergeStrategy._collapse_changes(changes, self.logger)
        recorded_models = []
        real_apply = self._real_apply_one(branch)
        real_emit = bulk_merge_module._emit_main_object_changes
        record_failed = Mock()

        def fail_deferred_fallback(collapsed_change):
            if getattr(collapsed_change, "key", (None, None, None))[-1] == (
                "defer_self_ref_fk"
            ):
                return _ApplyOneFailure(RuntimeError("deferred fallback failed"))
            return real_apply(collapsed_change)

        def fail_deferred_audit(objects, action, request, source_branch):
            if action == ObjectChangeActionChoices.ACTION_UPDATE and any(
                isinstance(obj, Device) and obj.primary_ip4_id is not None
                for obj in objects
            ):
                raise RuntimeError("deferred update audit failed")
            return real_emit(objects, action, request, source_branch)

        with patch.object(
            bulk_merge_module,
            "_emit_main_object_changes",
            side_effect=fail_deferred_audit,
        ):
            applied, failed, _models = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=Mock(side_effect=fail_deferred_fallback),
                record_applied=recorded_models.append,
                record_failed=record_failed,
            )

        self.assertEqual(applied + failed, len(collapsed))
        self.assertEqual(failed, 1)
        self.assertEqual(len(recorded_models), len(collapsed) - 1)
        record_failed.assert_called_once()
        self.assertEqual(record_failed.call_args.args[0].key[1], device_pk)
        self.assertEqual(
            str(record_failed.call_args.args[1]),
            "deferred fallback failed",
        )
        device = Device.objects.get(pk=device_pk)
        self.assertIsNotNone(device.virtual_chassis_id)
        self.assertIsNone(device.primary_ip4_id)

    def test_bulk_create_and_m2m_state_roll_back_together(self):
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag

        manager_probe = Tag.objects.create(
            name="M2M Manager Probe",
            slug="m2m-manager-probe",
        )
        manager_class = type(manager_probe.object_types)
        branch = provision_branch(user=self.user, name="Atomic M2M Merge")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged_tag = Tag.objects.create(
                name="Atomic M2M Tag",
                slug="atomic-m2m-tag",
            )
            staged_tag.object_types.add(ContentType.objects.get_for_model(Site))

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = self._real_apply_one(branch)
        with patch.object(
            manager_class,
            "set",
            side_effect=RuntimeError("relationship write failed"),
        ):
            applied, failed, _ = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
            )

        self.assertEqual(applied, 0)
        self.assertGreaterEqual(failed, 1)
        self.assertFalse(Tag.objects.filter(slug="atomic-m2m-tag").exists())

    def test_merge_branch_creates_missing_bay_and_module_for_existing_device(self):
        manufacturer = Manufacturer.objects.create(
            name="Module Bay Merge Manufacturer",
            slug="module-bay-merge-manufacturer",
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Module Bay Merge Device Type",
            slug="module-bay-merge-device-type",
        )
        role = DeviceRole.objects.create(
            name="Module Bay Merge Role",
            slug="module-bay-merge-role",
        )
        site = Site.objects.create(
            name="Module Bay Merge Site",
            slug="module-bay-merge-site",
        )
        device = Device.objects.create(
            name="module-bay-merge-device",
            device_type=device_type,
            role=role,
            site=site,
            status="active",
        )
        module_type = ModuleType.objects.create(
            manufacturer=manufacturer,
            model="Module Bay Merge Module Type",
        )
        source = ForwardSource.objects.create(
            name="module-bay-merge-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "test-network",
            },
        )
        sync = ForwardSync.objects.create(
            name="module-bay-merge-sync",
            source=source,
            user=self.user,
            parameters={"snapshot_id": "latestProcessed", "dcim.device": True},
        )
        branch = provision_branch(user=self.user, name="Module Bay Device Merge")

        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            branch_device = Device.objects.get(pk=device.pk)
            branch_module_type = ModuleType.objects.get(pk=module_type.pk)
            module_bay = ModuleBay.objects.create(
                device=branch_device,
                name="Slot 1",
                label="Slot 1",
                position="1",
            )
            module = Module.objects.create(
                device=branch_device,
                module_bay=module_bay,
                module_type=branch_module_type,
            )
            module_bay_pk = module_bay.pk
            module_pk = module.pk

        self.assertFalse(
            ModuleBay.objects.filter(device__name="module-bay-merge-device").exists()
        )
        self.assertFalse(Module.objects.filter(device=device).exists())
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="module-bay-merge-snapshot",
            branch=branch,
        )

        merge_branch(ingestion)

        branch.refresh_from_db()
        ingestion.refresh_from_db()
        device.refresh_from_db()
        module_bay = ModuleBay.objects.get(pk=module_bay_pk)
        module = Module.objects.get(pk=module_pk)
        self.assertEqual(branch.status, "merged")
        self.assertEqual(Device.objects.filter(pk=device.pk).count(), 1)
        self.assertEqual(module_bay.device, device)
        self.assertEqual(module_bay.name, "Slot 1")
        self.assertEqual(module.module_bay, module_bay)
        self.assertEqual(module.module_type, module_type)
        self.assertEqual(ingestion.applied_change_count, 2)
        self.assertEqual(ingestion.failed_change_count, 0)
        self.assertFalse(ingestion.issues.exists())

    def test_merge_branch_reports_collapsed_logical_change_totals(self):
        source = ForwardSource.objects.create(
            name="logical-total-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "logical-total-network"},
        )
        sync = ForwardSync.objects.create(
            name="logical-total-sync",
            source=source,
            user=self.user,
            parameters={"snapshot_id": "latestProcessed"},
        )
        branch = provision_branch(user=self.user, name="Logical Totals")

        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            site = Site.objects.create(
                name="Logical Total Site",
                slug="logical-total-site",
            )
            site.description = "updated after create"
            site.save(update_fields=["description"])

        self.assertEqual(branch.get_unmerged_changes().count(), 2)
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="logical-total-snapshot",
            branch=branch,
        )

        merge_branch(ingestion)

        ingestion.refresh_from_db()
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (1, 0),
        )
        self.assertEqual(
            (
                ingestion.created_change_count,
                ingestion.updated_change_count,
                ingestion.deleted_change_count,
            ),
            (1, 0, 0),
        )
        self.assertEqual(
            Site.objects.get(slug="logical-total-site").description,
            "updated after create",
        )

    def test_partial_retry_replays_complete_branch_without_counter_inflation(self):
        from extras.models import Tag
        from forward_netbox.exceptions import ForwardPartialMergeError
        from netbox_branching.models import AppliedChange
        from netbox_branching.models import ObjectChange

        source = ForwardSource.objects.create(
            name="exact-retry-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "exact-retry-network"},
        )
        sync = ForwardSync.objects.create(
            name="exact-retry-sync",
            source=source,
            user=self.user,
            parameters={"snapshot_id": "latestProcessed"},
        )
        branch = provision_branch(user=self.user, name="Exact Partial Retry")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            Tag.objects.create(name="Exact Retry Tag", slug="exact-retry-tag")
            Region.objects.create(
                name="Exact Retry Region",
                slug="exact-retry-region",
            )

        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="exact-retry-snapshot",
            branch=branch,
        )
        original_apply = ObjectChange.apply

        def fail_region(change, *args, **kwargs):
            if change.changed_object_type.model == "region":
                raise RuntimeError("persistent first-attempt failure")
            return original_apply(change, *args, **kwargs)

        with (
            patch.object(ObjectChange, "apply", new=fail_region),
            self.assertRaisesRegex(
                ForwardPartialMergeError,
                "1 failed",
            ),
        ):
            merge_branch(ingestion)

        ingestion.refresh_from_db()
        branch.refresh_from_db()
        self.assertEqual(branch.status, "ready")
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (1, 1),
        )
        self.assertEqual(
            (ingestion.created_change_count, ingestion.updated_change_count),
            (2, 0),
        )
        self.assertEqual(branch.get_unmerged_changes().count(), 2)
        self.assertEqual(AppliedChange.objects.filter(branch=branch).count(), 1)

        merge_branch(ingestion)

        ingestion.refresh_from_db()
        branch.refresh_from_db()
        self.assertEqual(branch.status, "merged")
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (2, 0),
        )
        self.assertEqual(
            (ingestion.created_change_count, ingestion.updated_change_count),
            (2, 0),
        )
        self.assertEqual(branch.get_merged_changes().count(), 2)
        self.assertEqual(AppliedChange.objects.filter(branch=branch).count(), 2)

    def test_merge_branch_bulk_prefix_update_delete_use_invoking_user_and_audit(self):
        from core.models import ObjectChange
        from django.contrib.auth import get_user_model
        from django.contrib.contenttypes.models import ContentType
        from django.db.models.signals import post_delete
        from django.db.models.signals import pre_delete
        from extras.models import Tag
        from extras.models import TaggedItem
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        updated_prefix = Prefix.objects.create(prefix="100.64.0.0/24", status="active")
        deleted_prefix = Prefix.objects.create(prefix="100.64.1.0/24", status="active")
        region = Region.objects.create(
            name="Production Region", slug="production-region"
        )
        deleted_tag = Tag.objects.create(
            name="Production Prefix Tag", slug="production-prefix-tag"
        )
        deleted_prefix.tags.add(deleted_tag)
        invoking_user = get_user_model().objects.create_user(
            username="production-merge-invoker"
        )
        merge_request_id = uuid.uuid4()
        source = ForwardSource.objects.create(
            name="prefix-production-merge-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "test-network"},
        )
        sync = ForwardSync.objects.create(
            name="prefix-production-merge-sync",
            source=source,
            user=self.user,
        )

        competing_branch = provision_branch(
            user=self.user, name="Competing Prefix Review"
        )
        with activate_branch(competing_branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            for prefix_id in (updated_prefix.pk, deleted_prefix.pk):
                competing = Prefix.objects.get(pk=prefix_id)
                competing.status = "deprecated"
                competing.save(update_fields=["status"])

        branch = provision_branch(user=self.user, name="Production Prefix Merge")
        runner = Mock(sync=sync, ingestion=None)
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                branch_region = Region.objects.get(pk=region.pk)
                branch_region.name = "Production Region Updated"
                branch_region.save(update_fields=["name"])
                self.assertTrue(
                    bulk_orm_apply_simple_models(
                        runner,
                        "ipam.prefix",
                        [
                            {
                                "prefix": str(updated_prefix.prefix),
                                "vrf": None,
                                "status": "reserved",
                            }
                        ],
                    )
                )
                self.assertTrue(
                    bulk_orm_delete_prefixes(
                        runner,
                        [
                            {
                                "prefix": str(deleted_prefix.prefix),
                                "vrf": None,
                            }
                        ],
                    )
                )
        finally:
            current_request.reset(token)

        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="prefix-production-merge-snapshot",
            branch=branch,
            change_request_id=merge_request_id,
        )
        prefix_pre_delete = Mock()
        prefix_post_delete = Mock()
        tagged_pre_delete = Mock()
        tagged_post_delete = Mock()
        receivers = (
            (pre_delete, prefix_pre_delete, Prefix, "main-prefix-pre-delete-test"),
            (post_delete, prefix_post_delete, Prefix, "main-prefix-post-delete-test"),
            (pre_delete, tagged_pre_delete, TaggedItem, "main-tagged-pre-delete-test"),
            (
                post_delete,
                tagged_post_delete,
                TaggedItem,
                "main-tagged-post-delete-test",
            ),
        )
        for signal, receiver, sender, dispatch_uid in receivers:
            signal.connect(
                receiver,
                sender=sender,
                dispatch_uid=dispatch_uid,
                weak=False,
            )
        try:
            merge_branch(ingestion, user=invoking_user)
        finally:
            for signal, receiver, sender, dispatch_uid in receivers:
                signal.disconnect(
                    receiver,
                    sender=sender,
                    dispatch_uid=dispatch_uid,
                )

        updated_prefix.refresh_from_db()
        region.refresh_from_db()
        self.assertEqual(region.name, "Production Region Updated")
        self.assertEqual(updated_prefix.status, "reserved")
        self.assertFalse(Prefix.objects.filter(pk=deleted_prefix.pk).exists())
        self.assertFalse(
            TaggedItem.objects.filter(
                object_id=deleted_prefix.pk,
                tag=deleted_tag,
            ).exists()
        )
        prefix_pre_delete.assert_not_called()
        prefix_post_delete.assert_not_called()
        tagged_pre_delete.assert_called_once()
        tagged_post_delete.assert_called_once()
        audits = ObjectChange.objects.filter(
            changed_object_type=ContentType.objects.get_for_model(Prefix),
            changed_object_id__in=[updated_prefix.pk, deleted_prefix.pk],
        )
        self.assertEqual(audits.count(), 2)
        region_audits = ObjectChange.objects.filter(
            changed_object_type=ContentType.objects.get_for_model(Region),
            changed_object_id=region.pk,
            action="update",
        )
        self.assertEqual(region_audits.count(), 1)
        all_audits = audits | region_audits
        self.assertEqual(
            set(all_audits.values_list("user_id", flat=True)),
            {invoking_user.pk},
        )
        self.assertEqual(
            set(all_audits.values_list("request_id", flat=True)),
            {merge_request_id},
        )
        self.assertEqual(
            set(audits.values_list("action", flat=True)), {"update", "delete"}
        )
        branch.refresh_from_db()
        self.assertEqual(
            branch.get_merged_changes().filter(pk__in=all_audits).count(),
            3,
        )

        update_diff = ChangeDiff.objects.get(
            branch=competing_branch, object_id=updated_prefix.pk
        )
        delete_diff = ChangeDiff.objects.get(
            branch=competing_branch, object_id=deleted_prefix.pk
        )
        self.assertEqual(update_diff.current["status"], "reserved")
        self.assertIsNone(delete_diff.current)
        ingestion.refresh_from_db()
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count),
            (3, 0),
        )


class SingleBranchExecutorTest(CleanTransactionTestCase):
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
            user=self.user,
            auto_merge=True,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _context(self, snapshot_id="snap-1"):
        from forward_netbox.utilities.query_fetch_execution import ForwardQueryContext

        return ForwardQueryContext(
            network_id="net-1",
            snapshot_selector="latestProcessed",
            snapshot_id=snapshot_id,
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

    def _run_executor(self, *, workloads=None, snapshot_id="snap-1"):
        from forward_netbox.utilities.query_fetch import ForwardQueryFetcher
        from forward_netbox.utilities.single_branch_executor import (
            ForwardSingleBranchExecutor,
        )
        from forward_netbox.utilities.validation import ForwardValidationRunner

        context = self._context(snapshot_id)
        if workloads is None:
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

    def test_single_branch_repeat_run_applies_delete_phase(self):
        from forward_netbox.utilities.branch_budget import BranchWorkload

        first_workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Locations",
            upsert_rows=[
                {"name": "Keep Site", "slug": "sbe-keep-site"},
                {"name": "Delete Site", "slug": "sbe-delete-site"},
            ],
            coalesce_fields=[["slug"], ["name"]],
            query_name="Forward Locations",
        )
        self._run_executor(workloads=[first_workload], snapshot_id="snap-1")
        self.assertTrue(Site.objects.filter(slug="sbe-delete-site").exists())

        second_workload = BranchWorkload(
            model_string="dcim.site",
            label="dcim.site | Forward Locations",
            upsert_rows=[{"name": "Keep Site", "slug": "sbe-keep-site"}],
            delete_rows=[{"name": "Delete Site", "slug": "sbe-delete-site"}],
            coalesce_fields=[["slug"], ["name"]],
            query_name="Forward Locations",
        )
        self._run_executor(workloads=[second_workload], snapshot_id="snap-2")

        self.assertTrue(Site.objects.filter(slug="sbe-keep-site").exists())
        self.assertFalse(Site.objects.filter(slug="sbe-delete-site").exists())

    def test_single_branch_honors_explicit_bulk_orm_disable(self):
        self.sync.parameters = {
            **(self.sync.parameters or {}),
            "enable_bulk_orm": False,
        }
        self.sync.save(update_fields=["parameters"])

        self._run_executor()

        self.assertIs(self.sync.parameters["enable_bulk_orm"], False)


class OrderingComplexityTest(CleanTransactionTestCase):
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
        # If a real cycle survives the local split, the sort must detect it
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
            patch.object(SquashMergeStrategy, "_build_fk_dependency_graph"),
            patch.object(SquashMergeStrategy, "_log_cycle_details", create=True),
            patch.object(bulk_merge.squash_dependency_graph_built, "send"),
        ):
            with self.assertRaises(Exception):
                bulk_merge._order_collapsed_changes_fast(objs, logger, "merge")


class TagOrderingTest(TestCase):
    def test_tag_identity_release_precedes_create_then_object_updates(self):
        from types import SimpleNamespace

        from extras.models import Tag
        from netbox_branching.merge_strategies.squash import ActionType
        from netbox_branching.merge_strategies.squash import CollapsedChange
        from netbox_branching.merge_strategies.squash import SquashMergeStrategy

        from forward_netbox.utilities import bulk_merge

        release = CollapsedChange(("extras.tag", 1), Tag)
        release.final_action = ActionType.UPDATE
        release.prechange_data = {"name": "Forward", "slug": "forward"}
        release.postchange_data = {"name": "Former Forward", "slug": "former"}
        release.last_change = SimpleNamespace(time=1)
        create = CollapsedChange(("extras.tag", 2), Tag)
        create.final_action = ActionType.CREATE
        create.postchange_data = {"name": "Forward", "slug": "forward"}
        create.last_change = SimpleNamespace(time=2)
        object_update = CollapsedChange(("dcim.site", 1), Site)
        object_update.final_action = ActionType.UPDATE
        object_update.prechange_data = {"name": "Site"}
        object_update.postchange_data = {"name": "Site", "tags": ["Forward"]}
        object_update.last_change = SimpleNamespace(time=0)
        changes = {
            release.key: release,
            create.key: create,
            object_update.key: object_update,
        }

        with (
            patch.object(SquashMergeStrategy, "_build_fk_dependency_graph"),
            patch.object(bulk_merge.squash_dependency_graph_built, "send"),
        ):
            ordered = bulk_merge._order_collapsed_changes_fast(
                changes,
                logging.getLogger("forward_netbox.tests.tag-ordering"),
                "merge",
            )

        self.assertEqual(
            [change.key for change in ordered],
            [release.key, create.key, object_update.key],
        )


class SkipMissingBatchedTest(CleanTransactionTestCase):
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
        with self.assertLogs(logger, level="INFO") as captured_logs:
            with self.assertNumQueries(expected_queries):
                bulk_merge._skip_updates_missing_in_main_batched(collapsed, logger)
        self.assertTrue(
            all(c.final_action == ActionType.SKIP for c in collapsed.values())
        )
        self.assertEqual(len(captured_logs.output), 1)
        self.assertIn(f"Skipping {n} UPDATE(s) for Site", captured_logs.output[0])
        self.assertIn("sample PKs: 1, 2, 3", captured_logs.output[0])


class AffectedPrefixVRFTest(TestCase):
    def test_action_data_targets_only_explicit_pre_and_post_vrfs(self):
        from types import SimpleNamespace

        from forward_netbox.utilities.bulk_merge import _affected_prefix_vrf_ids

        create = SimpleNamespace(
            model_class=Prefix,
            prechange_data={},
            postchange_data={"vrf": 7},
        )
        update = SimpleNamespace(
            model_class=Prefix,
            prechange_data={"vrf": 7},
            postchange_data={"vrf": 8},
        )
        delete = SimpleNamespace(
            model_class=Prefix,
            prechange_data={"vrf": 8},
            postchange_data=None,
        )

        self.assertEqual(_affected_prefix_vrf_ids([create]), {7})
        self.assertEqual(_affected_prefix_vrf_ids([update]), {7, 8})
        self.assertEqual(_affected_prefix_vrf_ids([delete]), {8})


class Phase4BulkStageTest(CleanTransactionTestCase):
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
        runner = Mock()
        runner.ingestion = None
        return runner

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

    def test_bulk_lag_stage_records_final_relationship_and_merges(self):
        from django.contrib.contenttypes.models import ContentType
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(name="LAG Mfr", slug="lag-mfr")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="LAG Model", slug="lag-model"
        )
        role = DeviceRole.objects.create(name="LAG Role", slug="lag-role")
        site = Site.objects.create(name="LAG Site", slug="lag-site")
        Device.objects.create(
            name="lag-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        source = ForwardSource.objects.create(
            name="lag-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="lag-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        rows = [
            {
                "device": "lag-device",
                "name": "Port-Channel1",
                "type": "lag",
                "enabled": False,
            },
            {
                "device": "lag-device",
                "name": "Ethernet1",
                "type": "1000base-t",
                "enabled": True,
                "lag": "Port-Channel1",
            },
        ]
        branch = provision_branch(user=self.user, name="LAG Bulk Stage")
        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch(
                    "forward_netbox.utilities.sync_interface.apply_dcim_interface",
                    side_effect=AssertionError("LAG membership used adapter"),
                ),
            ):
                self.request.id = uuid.uuid4()
                self.assertTrue(bulk_orm_apply_interface(runner, rows))
                member = Interface.objects.get(
                    device__name="lag-device", name="Ethernet1"
                )
                self.assertEqual(member.lag.name, "Port-Channel1")
        finally:
            current_request.reset(token)

        self.assertFalse(Interface.objects.filter(name="Ethernet1").exists())
        interface_type = ContentType.objects.get_for_model(Interface)
        changes = branch.get_unmerged_changes().filter(
            changed_object_type=interface_type
        )
        self.assertEqual(changes.count(), 2)
        member_change = changes.get(object_repr="Ethernet1")
        parent_change = changes.get(object_repr="Port-Channel1")
        self.assertEqual(
            member_change.postchange_data["lag"],
            parent_change.changed_object_id,
        )

        apply_one = Mock()
        record_applied = Mock()
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes.order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=apply_one,
            record_applied=record_applied,
        )
        # The deferred-FK UPDATE stays on the bulk path but is internal work;
        # progress and applied totals remain bounded by the two branch changes.
        self.assertEqual((applied, failed, apply_one.call_count), (2, 0, 0))
        self.assertEqual(record_applied.call_count, 2)
        merged_member = Interface.objects.get(name="Ethernet1")
        self.assertEqual(merged_member.lag.name, "Port-Channel1")

    def test_bulk_lag_relationship_failure_rolls_back_rows_and_evidence(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Rollback Mfr", slug="rollback-mfr"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Rollback Model",
            slug="rollback-model",
        )
        role = DeviceRole.objects.create(name="Rollback Role", slug="rollback-role")
        site = Site.objects.create(name="Rollback Site", slug="rollback-site")
        Device.objects.create(
            name="rollback-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        source = ForwardSource.objects.create(
            name="rollback-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="rollback-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        branch = provision_branch(user=self.user, name="LAG Atomic Failure")
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                with (
                    patch.object(
                        Interface.objects,
                        "bulk_update",
                        side_effect=RuntimeError("injected relationship failure"),
                    ),
                    self.assertRaisesRegex(
                        RuntimeError, "injected relationship failure"
                    ),
                ):
                    bulk_orm_apply_interface(
                        runner,
                        [
                            {
                                "device": "rollback-device",
                                "name": "Ethernet1",
                                "type": "1000base-t",
                                "enabled": True,
                                "lag": "Port-Channel1",
                            }
                        ],
                    )
                self.assertFalse(
                    Interface.objects.filter(
                        device__name="rollback-device",
                        name__in=["Ethernet1", "Port-Channel1"],
                    ).exists()
                )
        finally:
            current_request.reset(token)

        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_lag_canonical_self_parent_never_stages_new_or_existing_row(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Self Parent Mfr", slug="self-parent-mfr"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Self Parent Model",
            slug="self-parent-model",
        )
        role = DeviceRole.objects.create(
            name="Self Parent Role", slug="self-parent-role"
        )
        site = Site.objects.create(name="Self Parent Site", slug="self-parent-site")
        existing_device = Device.objects.create(
            name="self-parent-existing",
            device_type=device_type,
            role=role,
            site=site,
        )
        new_device = Device.objects.create(
            name="self-parent-new",
            device_type=device_type,
            role=role,
            site=site,
        )
        existing = Interface.objects.create(
            device=existing_device,
            name="Po1",
            type="1000base-t",
            description="unchanged",
        )
        source = ForwardSource.objects.create(
            name="self-parent-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="self-parent-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._record_issue = Mock()
        runner._mark_dependency_failed = Mock()
        rows = [
            {
                "device": device_name,
                "name": "Po1",
                "type": "1000base-t",
                "enabled": True,
                "lag": "Port-channel1",
            }
            for device_name in (existing_device.name, new_device.name)
        ]
        branch = provision_branch(user=self.user, name="Canonical Self Parent")
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.assertTrue(bulk_orm_apply_interface(runner, rows))
                existing.refresh_from_db()
                self.assertEqual(existing.type, "1000base-t")
                self.assertEqual(existing.description, "unchanged")
                self.assertIsNone(existing.lag_id)
                self.assertFalse(
                    Interface.objects.filter(device=new_device, name="Po1").exists()
                )
        finally:
            current_request.reset(token)

        self.assertEqual(runner._record_issue.call_count, 2)
        self.assertEqual(runner._mark_dependency_failed.call_count, 2)
        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_lag_cabled_parent_stages_and_production_merges_atomically(self):
        from core.models import ObjectChange
        from django.contrib.auth import get_user_model
        from django.contrib.contenttypes.models import ContentType
        from django.db.models import Q
        from netbox_branching.utilities import deactivate_branch
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Cabled Success Mfr", slug="cabled-success-mfr"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Cabled Success Model",
            slug="cabled-success-model",
        )
        role = DeviceRole.objects.create(
            name="Cabled Success Role", slug="cabled-success-role"
        )
        site = Site.objects.create(
            name="Cabled Success Site", slug="cabled-success-site"
        )
        device = Device.objects.create(
            name="cabled-success-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        remote_device = Device.objects.create(
            name="cabled-success-remote",
            device_type=device_type,
            role=role,
            site=site,
        )
        parent = Interface.objects.create(
            device=device,
            name="bond0",
            type="1000base-t",
        )
        remote = Interface.objects.create(
            device=remote_device,
            name="Ethernet1",
            type="1000base-t",
        )
        cable = Cable.objects.create(a_terminations=[parent], b_terminations=[remote])
        source = ForwardSource.objects.create(
            name="cabled-success-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(
            name="cabled-success-sync",
            source=source,
            user=self.user,
        )
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        branch = provision_branch(user=self.user, name="Cabled Parent Success")
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.assertTrue(
                    bulk_orm_apply_interface(
                        runner,
                        [
                            {
                                "device": device.name,
                                "name": "Ethernet-member",
                                "type": "1000base-t",
                                "enabled": True,
                                "lag": parent.name,
                            }
                        ],
                    )
                )
                parent.refresh_from_db()
                member = Interface.objects.get(
                    device=device,
                    name="Ethernet-member",
                )
                self.assertEqual(parent.type, "lag")
                self.assertIsNone(parent.cable)
                self.assertEqual(member.lag_id, parent.pk)
                member_pk = member.pk
        finally:
            current_request.reset(token)

        with deactivate_branch():
            main_parent = Interface.objects.get(pk=parent.pk)
            self.assertEqual(main_parent.type, "1000base-t")
            self.assertEqual(main_parent.cable.pk, cable.pk)
            self.assertFalse(Interface.objects.filter(pk=member_pk).exists())
        interface_type = ContentType.objects.get_for_model(Interface)
        cable_type = ContentType.objects.get_for_model(Cable)
        staged_changes = branch.get_unmerged_changes()
        self.assertEqual(staged_changes.count(), 3)
        self.assertEqual(
            set(staged_changes.values_list("action", flat=True)),
            {"create", "update", "delete"},
        )
        self.assertEqual(
            ChangeDiff.objects.filter(
                branch=branch,
                object_type__in=[interface_type, cable_type],
            ).count(),
            3,
        )

        invoking_user = get_user_model().objects.create_user(
            username="cabled-success-invoker"
        )
        merge_request_id = uuid.uuid4()
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id="cabled-success-snapshot",
            branch=branch,
            change_request_id=merge_request_id,
        )
        merge_branch(ingestion, user=invoking_user)

        branch.refresh_from_db()
        ingestion.refresh_from_db()
        with deactivate_branch():
            main_parent = Interface.objects.get(pk=parent.pk)
            member = Interface.objects.get(pk=member_pk)
        self.assertEqual(branch.status, "merged")
        self.assertEqual(
            (ingestion.applied_change_count, ingestion.failed_change_count), (3, 0)
        )
        self.assertFalse(Cable.objects.filter(pk=cable.pk).exists())
        self.assertEqual(main_parent.type, "lag")
        self.assertEqual(member.lag_id, main_parent.pk)
        audits = ObjectChange.objects.filter(
            Q(
                changed_object_type=interface_type,
                changed_object_id__in=[main_parent.pk, member.pk],
            )
            | Q(changed_object_type=cable_type, changed_object_id=cable.pk),
            request_id=merge_request_id,
        )
        # The member is audited as CREATE plus the internal deferred-LAG UPDATE;
        # the two audit rows still count as one logical branch change.
        self.assertEqual(audits.count(), 4)
        self.assertEqual(
            set(audits.values_list("user_id", flat=True)), {invoking_user.pk}
        )
        self.assertEqual(
            set(audits.values_list("request_id", flat=True)), {merge_request_id}
        )
        self.assertEqual(
            set(audits.values_list("action", flat=True)),
            {"create", "update", "delete"},
        )
        self.assertEqual(
            branch.get_merged_changes().filter(pk__in=audits).count(),
            4,
        )
        self.assertEqual(
            list(
                audits.filter(changed_object_id=member.pk)
                .order_by("time")
                .values_list("action", flat=True)
            ),
            ["create", "update"],
        )
        self.assertEqual(
            audits.get(changed_object_id=member.pk, action="update").postchange_data[
                "lag"
            ],
            main_parent.pk,
        )

    def test_bulk_lag_cabled_parent_failure_rolls_back_parent_not_unrelated_rows(self):
        from django.contrib.contenttypes.models import ContentType
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Cabled Parent Mfr", slug="cabled-parent-mfr"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Cabled Parent Model",
            slug="cabled-parent-model",
        )
        role = DeviceRole.objects.create(
            name="Cabled Parent Role", slug="cabled-parent-role"
        )
        site = Site.objects.create(name="Cabled Parent Site", slug="cabled-parent-site")
        device = Device.objects.create(
            name="cabled-parent-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        remote_device = Device.objects.create(
            name="cabled-parent-remote",
            device_type=device_type,
            role=role,
            site=site,
        )
        parent = Interface.objects.create(
            device=device,
            name="bond0",
            type="1000base-t",
        )
        remote = Interface.objects.create(
            device=remote_device,
            name="Ethernet1",
            type="1000base-t",
        )
        cable = Cable.objects.create(a_terminations=[parent], b_terminations=[remote])
        source = ForwardSource.objects.create(
            name="cabled-parent-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="cabled-parent-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._record_issue = Mock()
        rows = [
            {
                "device": device.name,
                "name": "Ethernet-unrelated",
                "type": "1000base-t",
                "enabled": True,
            },
            {
                "device": device.name,
                "name": "Ethernet-member",
                "type": "1000base-t",
                "enabled": True,
                "lag": parent.name,
            },
        ]

        original_bulk_create = Interface.objects.bulk_create
        bulk_create_calls = 0

        def fail_member_create(objects, *args, **kwargs):
            nonlocal bulk_create_calls
            bulk_create_calls += 1
            if bulk_create_calls == 2:
                raise RuntimeError("injected member failure")
            return original_bulk_create(objects, *args, **kwargs)

        branch = provision_branch(user=self.user, name="Cabled Parent Isolation")
        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch.object(
                    Interface.objects,
                    "bulk_create",
                    side_effect=fail_member_create,
                ),
            ):
                self.request.id = uuid.uuid4()
                self.assertTrue(bulk_orm_apply_interface(runner, rows))
                parent.refresh_from_db()
                self.assertEqual(parent.type, "1000base-t")
                self.assertEqual(parent.cable.pk, cable.pk)
                self.assertTrue(
                    Interface.objects.filter(
                        device=device, name="Ethernet-unrelated"
                    ).exists()
                )
                self.assertFalse(
                    Interface.objects.filter(
                        device=device, name="Ethernet-member"
                    ).exists()
                )
        finally:
            current_request.reset(token)

        self.assertEqual(runner._record_issue.call_count, 1)
        changes = branch.get_unmerged_changes()
        self.assertEqual(changes.count(), 1)
        self.assertEqual(changes.get().object_repr, "Ethernet-unrelated")
        interface_type = ContentType.objects.get_for_model(Interface)
        cable_type = ContentType.objects.get_for_model(Cable)
        self.assertFalse(
            ChangeDiff.objects.filter(
                branch=branch,
                object_type=interface_type,
                object_id=parent.pk,
            ).exists()
        )
        self.assertFalse(
            ChangeDiff.objects.filter(
                branch=branch,
                object_type=cable_type,
                object_id=cable.pk,
            ).exists()
        )

    def test_bulk_device_scope_tag_is_reviewable_and_survives_merge(self):
        from extras.models import Tag
        from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(name="Scope Mfr", slug="scope-mfr")
        DeviceType.objects.create(
            manufacturer=manufacturer, model="Scope Model", slug="scope-model"
        )
        DeviceRole.objects.create(name="Scope Role", slug="scope-role")
        Site.objects.create(name="Scope Site", slug="scope-site")
        source = ForwardSource.objects.create(
            name="scope-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={
                "network_id": "network-1",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Forward Include"],
            },
        )
        sync = ForwardSync.objects.create(name="scope-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._scope_matched_tags = {"scope-device": ["Forward Include"]}
        row = {
            "name": "scope-device",
            "site": "Scope Site",
            "site_slug": "scope-site",
            "role": "Scope Role",
            "role_slug": "scope-role",
            "role_color": "9e9e9e",
            "manufacturer": "Scope Mfr",
            "manufacturer_slug": "scope-mfr",
            "device_type": "Scope Model",
            "device_type_slug": "scope-model",
            "platform": "",
            "platform_slug": "",
            "status": "active",
        }
        branch = provision_branch(user=self.user, name="Scope Tag Bulk Stage")
        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch(
                    "forward_netbox.utilities.sync_device.apply_dcim_device",
                    side_effect=AssertionError("scope tagging used adapter"),
                ),
            ):
                self.request.id = uuid.uuid4()
                self.assertTrue(bulk_orm_apply_device(runner, [row]))
                staged = Device.objects.get(name="scope-device")
                self.assertEqual(
                    list(staged.tags.values_list("name", flat=True)),
                    ["Forward Include"],
                )
        finally:
            current_request.reset(token)

        self.assertFalse(Device.objects.filter(name="scope-device").exists())
        changes = branch.get_unmerged_changes()
        self.assertEqual(changes.filter(object_repr="scope-device").count(), 1)
        device_change = changes.get(object_repr="scope-device")
        self.assertEqual(device_change.postchange_data["tags"], ["Forward Include"])

        apply_one = Mock()
        applied, failed, _ = bulk_merge_changes(
            branch,
            changes.order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=apply_one,
        )
        self.assertEqual(failed, 0)
        self.assertEqual(apply_one.call_count, 0)
        self.assertGreaterEqual(applied, 2)
        merged = Device.objects.get(name="scope-device")
        self.assertEqual(
            list(merged.tags.values_list("name", flat=True)), ["Forward Include"]
        )
        self.assertTrue(Tag.objects.filter(name="Forward Include").exists())

    def test_bulk_interface_accepts_device_created_in_same_branch(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_interface,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Branch Device Mfr", slug="branch-device-mfr"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Branch Device Model",
            slug="branch-device-model",
        )
        role = DeviceRole.objects.create(
            name="Branch Device Role", slug="branch-device-role"
        )
        site = Site.objects.create(name="Branch Device Site", slug="branch-device-site")
        source = ForwardSource.objects.create(
            name="branch-device-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={"network_id": "network-1"},
        )
        sync = ForwardSync.objects.create(name="branch-device-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        branch = provision_branch(user=self.user, name="Branch Device Interface")
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                branch_device = Device.objects.create(
                    name="branch-only-device",
                    device_type=device_type,
                    role=role,
                    site=site,
                )
                self.assertFalse(
                    Device.objects.using("default").filter(pk=branch_device.pk).exists()
                )
                self.assertTrue(
                    bulk_orm_apply_interface(
                        runner,
                        [
                            {
                                "device": branch_device.name,
                                "name": "Ethernet1",
                                "type": "1000base-t",
                                "enabled": True,
                            }
                        ],
                    )
                )
                interface = Interface.objects.get(
                    device=branch_device, name="Ethernet1"
                )
                self.assertEqual(interface.device_id, branch_device.pk)
        finally:
            current_request.reset(token)

    def test_bulk_device_scope_tag_evidence_failure_rolls_back_tag_and_assignment(self):
        from extras.models import Tag
        from extras.models import TaggedItem
        from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_device
        from forward_netbox.utilities.apply_engine_bulk import (
            emit_branch_object_changes,
        )
        from forward_netbox.utilities.sync import ForwardSyncRunner

        manufacturer = Manufacturer.objects.create(
            name="Scope Rollback Mfr", slug="scope-rollback-mfr"
        )
        DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Scope Rollback Model",
            slug="scope-rollback-model",
        )
        DeviceRole.objects.create(
            name="Scope Rollback Role", slug="scope-rollback-role"
        )
        Site.objects.create(name="Scope Rollback Site", slug="scope-rollback-site")
        source = ForwardSource.objects.create(
            name="scope-rollback-source",
            type="saas",
            url="https://forward.example",
            status="ready",
            parameters={
                "network_id": "network-1",
                "apply_device_scope_tags": True,
                "device_tag_include_tags": ["Forward Rollback Include"],
            },
        )
        sync = ForwardSync.objects.create(name="scope-rollback-sync", source=source)
        runner = ForwardSyncRunner(
            sync=sync, ingestion=None, client=None, logger_=Mock()
        )
        runner._scope_matched_tags = {
            "scope-rollback-device": ["Forward Rollback Include"]
        }
        row = {
            "name": "scope-rollback-device",
            "site": "Scope Rollback Site",
            "site_slug": "scope-rollback-site",
            "role": "Scope Rollback Role",
            "role_slug": "scope-rollback-role",
            "role_color": "9e9e9e",
            "manufacturer": "Scope Rollback Mfr",
            "manufacturer_slug": "scope-rollback-mfr",
            "device_type": "Scope Rollback Model",
            "device_type_slug": "scope-rollback-model",
            "platform": "",
            "platform_slug": "",
            "status": "active",
        }
        branch = provision_branch(user=self.user, name="Scope Tag Atomic Rollback")
        evidence_calls = 0

        def fail_device_evidence(*args, **kwargs):
            nonlocal evidence_calls
            evidence_calls += 1
            if evidence_calls == 2:
                raise RuntimeError("injected device evidence failure")
            return emit_branch_object_changes(*args, **kwargs)

        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch(
                    "forward_netbox.utilities.apply_engine_bulk."
                    "emit_branch_object_changes",
                    side_effect=fail_device_evidence,
                ),
                self.assertRaisesRegex(
                    RuntimeError, "injected device evidence failure"
                ),
            ):
                self.request.id = uuid.uuid4()
                bulk_orm_apply_device(runner, [row])
        finally:
            current_request.reset(token)

        self.assertFalse(Device.objects.filter(name=row["name"]).exists())
        self.assertFalse(Tag.objects.filter(name="Forward Rollback Include").exists())
        self.assertFalse(TaggedItem.objects.exists())
        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_prefix_stage_and_merge_preserve_hierarchy_without_row_saves(self):
        from django.contrib.contenttypes.models import ContentType
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="Prefix Bulk Stage")
        rows = [
            {"prefix": "10.0.0.0/16", "vrf": None, "status": "active"},
            {"prefix": "10.0.1.0/24", "vrf": None, "status": "active"},
        ]
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch.object(
                    Prefix, "save", side_effect=AssertionError("per-prefix save used")
                ),
            ):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_apply_simple_models(runner, "ipam.prefix", rows)
                )
                staged = list(Prefix.objects.order_by("prefix"))
                self.assertEqual(
                    [(item._depth, item._children) for item in staged],
                    [(0, 1), (1, 0)],
                )
        finally:
            current_request.reset(token)

        prefix_ct = ContentType.objects.get_for_model(Prefix)
        unmerged = branch.get_unmerged_changes().filter(changed_object_type=prefix_ct)
        self.assertEqual(unmerged.count(), 2)
        token = current_request.set(self.request)
        try:
            with (
                activate_branch(branch),
                event_tracking(self.request),
                patch.object(Prefix, "snapshot") as snapshot,
                patch(
                    "forward_netbox.utilities.apply_engine_bulk."
                    "_rebuild_prefix_hierarchies"
                ) as rebuild,
            ):
                self.request.id = uuid.uuid4()
                self.assertTrue(
                    bulk_orm_apply_simple_models(runner, "ipam.prefix", rows)
                )
                snapshot.assert_not_called()
                rebuild.assert_not_called()
        finally:
            current_request.reset(token)
        self.assertEqual(
            branch.get_unmerged_changes().filter(changed_object_type=prefix_ct).count(),
            2,
        )
        apply_one = Mock()
        with (
            activate_branch(branch),
            patch.object(
                Prefix,
                "save",
                side_effect=AssertionError("per-prefix merge save used"),
            ),
        ):
            applied, failed, _ = bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
            )
        self.assertEqual((applied, failed), (2, 0))
        self.assertEqual(apply_one.call_count, 0)
        merged = list(Prefix.objects.order_by("prefix"))
        self.assertEqual(
            [(item._depth, item._children) for item in merged],
            [(0, 1), (1, 0)],
        )
        from core.models import ObjectChange

        prefix_ct = ContentType.objects.get_for_model(Prefix)
        audits = ObjectChange.objects.filter(
            changed_object_type=prefix_ct,
            changed_object_id__in=[item.pk for item in merged],
            action="create",
        )
        self.assertEqual(audits.count(), 2)
        self.assertEqual(set(audits.values_list("user_id", flat=True)), {self.user.pk})

    def test_bulk_prefix_create_audit_failure_rolls_back_main_tree(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )
        from forward_netbox.utilities.bulk_merge import _BulkMergeAuditError

        parent = Prefix.objects.create(prefix="203.0.113.0/24", status="active")
        branch = provision_branch(user=self.user, name="Prefix Main Audit Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_apply_simple_models(
                        runner,
                        "ipam.prefix",
                        [
                            {
                                "prefix": "203.0.113.0/25",
                                "vrf": None,
                                "status": "active",
                            }
                        ],
                    )
                )
        finally:
            current_request.reset(token)

        child_change = branch.get_unmerged_changes().get(
            changed_object_type=ContentType.objects.get_for_model(Prefix)
        )
        with (
            patch(
                "forward_netbox.utilities.bulk_merge._emit_main_object_changes",
                side_effect=RuntimeError("injected main audit failure"),
            ),
            self.assertRaises(_BulkMergeAuditError),
        ):
            bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=Mock(),
            )

        self.assertFalse(
            Prefix.objects.filter(pk=child_change.changed_object_id).exists()
        )
        parent.refresh_from_db()
        self.assertEqual(parent._children, 0)
        self.assertFalse(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Prefix),
                changed_object_id=child_change.changed_object_id,
            ).exists()
        )

    def test_bulk_prefix_rebuild_failure_rolls_back_branch_evidence(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="Prefix Atomic Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                with (
                    patch(
                        "forward_netbox.utilities.apply_engine_bulk."
                        "_rebuild_prefix_hierarchies",
                        side_effect=RuntimeError("injected rebuild failure"),
                    ),
                    self.assertRaisesRegex(RuntimeError, "injected rebuild failure"),
                ):
                    bulk_orm_apply_simple_models(
                        runner,
                        "ipam.prefix",
                        [
                            {
                                "prefix": "203.0.113.0/24",
                                "vrf": None,
                                "status": "active",
                            }
                        ],
                    )
                self.assertFalse(
                    Prefix.objects.filter(prefix="203.0.113.0/24").exists()
                )
        finally:
            current_request.reset(token)

        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_prefix_delete_stages_evidence_repairs_tree_and_merges(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        parent = Prefix.objects.create(prefix="10.20.0.0/16", status="active")
        child = Prefix.objects.create(prefix="10.20.1.0/24", status="active")
        branch = provision_branch(user=self.user, name="Prefix Bulk Delete")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_delete_prefixes(
                        runner,
                        [{"prefix": str(child.prefix), "vrf": None}],
                    )
                )
                self.assertFalse(Prefix.objects.filter(pk=child.pk).exists())
                parent.refresh_from_db()
                self.assertEqual(parent._children, 0)
        finally:
            current_request.reset(token)

        self.assertTrue(Prefix.objects.filter(pk=child.pk).exists())
        change = branch.get_unmerged_changes().get(changed_object_id=child.pk)
        self.assertEqual(change.action, "delete")
        diff = ChangeDiff.objects.get(branch=branch, object_id=child.pk)
        self.assertEqual(diff.action, "delete")
        self.assertIsNotNone(diff.original)
        self.assertIsNone(diff.modified)
        self.assertIsNotNone(diff.current)

        def apply_one(collapsed):
            dummy = collapsed.generate_object_change()
            with transaction.atomic(), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                dummy.apply(branch, using=DEFAULT_DB_ALIAS, logger=self.logger)
            return True

        fallback = Mock(side_effect=apply_one)
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=fallback,
        )
        self.assertEqual((applied, failed), (1, 0))
        fallback.assert_not_called()
        self.assertFalse(Prefix.objects.filter(pk=child.pk).exists())
        parent.refresh_from_db()
        self.assertEqual(parent._children, 0)

    def test_bulk_prefix_delete_suppresses_per_row_hierarchy_signals(self):
        from django.contrib.contenttypes.models import ContentType
        from django.db.models.signals import post_delete
        from django.db.models.signals import pre_delete
        from extras.models import Tag
        from extras.models import TaggedItem
        from ipam.utils import rebuild_prefixes
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        prefixes = [Prefix(prefix="172.20.0.0/16", status="active")]
        prefixes.extend(
            Prefix(prefix=f"172.20.{index}.0/24", status="active")
            for index in range(200)
        )
        Prefix.objects.bulk_create(prefixes, batch_size=200)
        rebuild_prefixes(None)
        tagged_prefix = Prefix.objects.get(prefix="172.20.0.0/16")
        tag = Tag.objects.create(name="prefix-delete-tag", slug="prefix-delete-tag")
        tagged_prefix.tags.add(tag)
        prefix_ids = set(Prefix.objects.values_list("pk", flat=True))
        rows = [
            {"prefix": str(prefix.prefix), "vrf": None}
            for prefix in Prefix.objects.order_by("prefix")
        ]
        branch = provision_branch(user=self.user, name="Prefix Delete Scale")
        runner = self._runner()
        prefix_pre_delete = Mock()
        prefix_post_delete = Mock()
        tagged_pre_delete = Mock()
        tagged_post_delete = Mock()
        receivers = (
            (pre_delete, prefix_pre_delete, Prefix, "prefix-pre-delete-test"),
            (post_delete, prefix_post_delete, Prefix, "prefix-post-delete-test"),
            (pre_delete, tagged_pre_delete, TaggedItem, "tagged-pre-delete-test"),
            (post_delete, tagged_post_delete, TaggedItem, "tagged-post-delete-test"),
        )
        for signal, receiver, sender, dispatch_uid in receivers:
            signal.connect(
                receiver,
                sender=sender,
                dispatch_uid=dispatch_uid,
                weak=False,
            )
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                with patch(
                    "forward_netbox.utilities.apply_engine_bulk."
                    "_rebuild_prefix_hierarchies",
                    wraps=__import__(
                        "forward_netbox.utilities.apply_engine_bulk",
                        fromlist=["_rebuild_prefix_hierarchies"],
                    )._rebuild_prefix_hierarchies,
                ) as rebuild:
                    self.assertTrue(bulk_orm_delete_prefixes(runner, rows))
                rebuild.assert_called_once()
                prefix_pre_delete.assert_not_called()
                prefix_post_delete.assert_not_called()
                tagged_pre_delete.assert_called_once()
                tagged_post_delete.assert_called_once()
                self.assertFalse(Prefix.objects.filter(pk__in=prefix_ids).exists())
                self.assertFalse(
                    TaggedItem.objects.filter(
                        object_id=tagged_prefix.pk, tag=tag
                    ).exists()
                )
        finally:
            current_request.reset(token)
            for signal, receiver, sender, dispatch_uid in receivers:
                signal.disconnect(
                    receiver,
                    sender=sender,
                    dispatch_uid=dispatch_uid,
                )

        self.assertEqual(Prefix.objects.filter(pk__in=prefix_ids).count(), 201)
        prefix_type = ContentType.objects.get_for_model(Prefix)
        self.assertEqual(
            branch.get_unmerged_changes()
            .filter(changed_object_type=prefix_type, action="delete")
            .count(),
            201,
        )

    def test_bulk_prefix_protected_delete_rolls_back_and_uses_fallback(self):
        from django.db.models.deletion import ProtectedError
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        prefix = Prefix.objects.create(prefix="198.18.0.0/24", status="active")
        branch = provision_branch(user=self.user, name="Prefix Protected Delete")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                with patch(
                    "forward_netbox.utilities.bulk_delete."
                    "collector_delete_without_model_signals",
                    side_effect=ProtectedError("protected", {prefix}),
                ):
                    self.assertFalse(
                        bulk_orm_delete_prefixes(
                            runner,
                            [{"prefix": str(prefix.prefix), "vrf": None}],
                        )
                    )
                self.assertTrue(Prefix.objects.filter(pk=prefix.pk).exists())
        finally:
            current_request.reset(token)

        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_prefix_main_protected_delete_uses_per_object_fallback(self):
        from django.db.models.deletion import ProtectedError
        from extras.models import Tag
        from extras.models import TaggedItem
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        prefix = Prefix.objects.create(prefix="198.19.0.0/24", status="active")
        tag = Tag.objects.create(name="main-protected-tag", slug="main-protected-tag")
        prefix.tags.add(tag)
        branch = provision_branch(user=self.user, name="Main Protected Delete")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_delete_prefixes(
                        runner,
                        [{"prefix": str(prefix.prefix), "vrf": None}],
                    )
                )
        finally:
            current_request.reset(token)

        def apply_change(collapsed):
            dummy = collapsed.generate_object_change()
            with transaction.atomic(), event_tracking(self.request):
                dummy.apply(branch, using=DEFAULT_DB_ALIAS, logger=self.logger)
            return True

        apply_one = Mock(side_effect=apply_change)
        with patch(
            "forward_netbox.utilities.bulk_delete."
            "collector_delete_without_model_signals",
            side_effect=ProtectedError("injected protected row", {prefix}),
        ):
            applied, failed, _ = bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
            )

        self.assertEqual((applied, failed, apply_one.call_count), (1, 0, 1))
        self.assertFalse(Prefix.objects.filter(pk=prefix.pk).exists())
        self.assertFalse(
            TaggedItem.objects.filter(object_id=prefix.pk, tag=tag).exists()
        )

    def test_bulk_prefix_main_delete_audit_failure_rolls_back_related_cleanup(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag
        from extras.models import TaggedItem
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        prefix = Prefix.objects.create(prefix="198.20.0.0/24", status="active")
        tag = Tag.objects.create(name="main-audit-tag", slug="main-audit-tag")
        prefix.tags.add(tag)
        branch = provision_branch(user=self.user, name="Main Delete Audit Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_delete_prefixes(
                        runner,
                        [{"prefix": str(prefix.prefix), "vrf": None}],
                    )
                )
        finally:
            current_request.reset(token)

        with (
            patch(
                "forward_netbox.utilities.bulk_merge._emit_main_object_changes",
                side_effect=RuntimeError("injected main delete audit failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected main delete audit failure"),
        ):
            bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=Mock(),
            )

        self.assertTrue(Prefix.objects.filter(pk=prefix.pk).exists())
        self.assertTrue(
            TaggedItem.objects.filter(object_id=prefix.pk, tag=tag).exists()
        )
        self.assertFalse(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Prefix),
                changed_object_id=prefix.pk,
                action="delete",
            ).exists()
        )

    def test_bulk_prefix_main_delete_rebuild_failure_rolls_back_audit_and_row(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag
        from extras.models import TaggedItem
        from ipam.utils import rebuild_prefixes
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        parent = Prefix.objects.create(prefix="198.21.0.0/24", status="active")
        child = Prefix.objects.create(prefix="198.21.0.0/25", status="active")
        rebuild_prefixes(None)
        parent.refresh_from_db()
        self.assertEqual(parent._children, 1)
        tag = Tag.objects.create(name="main-rebuild-tag", slug="main-rebuild-tag")
        child.tags.add(tag)
        branch = provision_branch(user=self.user, name="Main Delete Rebuild Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_delete_prefixes(
                        runner,
                        [{"prefix": str(child.prefix), "vrf": None}],
                    )
                )
        finally:
            current_request.reset(token)

        with (
            patch(
                "forward_netbox.utilities.bulk_merge."
                "_rebuild_main_prefix_hierarchies",
                side_effect=RuntimeError("injected main delete rebuild failure"),
            ),
            self.assertRaisesRegex(
                RuntimeError, "injected main delete rebuild failure"
            ),
        ):
            bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=Mock(),
            )

        self.assertTrue(Prefix.objects.filter(pk=child.pk).exists())
        self.assertTrue(TaggedItem.objects.filter(object_id=child.pk, tag=tag).exists())
        parent.refresh_from_db()
        self.assertEqual(parent._children, 1)
        self.assertFalse(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Prefix),
                changed_object_id=child.pk,
                action="delete",
            ).exists()
        )

    def test_bulk_prefix_delete_rebuild_failure_rolls_back_all_branch_state(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_delete_prefixes,
        )

        prefix = Prefix.objects.create(prefix="10.30.0.0/24", status="active")
        branch = provision_branch(user=self.user, name="Prefix Delete Atomic Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                with (
                    patch(
                        "forward_netbox.utilities.apply_engine_bulk."
                        "_rebuild_prefix_hierarchies",
                        side_effect=RuntimeError("injected delete rebuild failure"),
                    ),
                    self.assertRaisesRegex(
                        RuntimeError, "injected delete rebuild failure"
                    ),
                ):
                    bulk_orm_delete_prefixes(
                        runner,
                        [{"prefix": str(prefix.prefix), "vrf": None}],
                    )
                self.assertTrue(Prefix.objects.filter(pk=prefix.pk).exists())
        finally:
            current_request.reset(token)

        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_prefix_changediff_failure_rolls_back_rows_and_objectchanges(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="Prefix Evidence Failure")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                with (
                    patch(
                        "forward_netbox.utilities.apply_engine_bulk."
                        "_sync_branch_change_diffs",
                        side_effect=RuntimeError("injected evidence failure"),
                    ),
                    self.assertRaisesRegex(RuntimeError, "injected evidence failure"),
                ):
                    bulk_orm_apply_simple_models(
                        runner,
                        "ipam.prefix",
                        [
                            {
                                "prefix": "192.0.2.0/24",
                                "vrf": None,
                                "status": "active",
                            }
                        ],
                    )
                self.assertFalse(Prefix.objects.filter(prefix="192.0.2.0/24").exists())
        finally:
            current_request.reset(token)

        self.assertEqual(branch.get_unmerged_changes().count(), 0)
        self.assertEqual(ChangeDiff.objects.filter(branch=branch).count(), 0)

    def test_bulk_prefix_update_records_review_and_concurrent_main_conflict(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        prefix = Prefix.objects.create(prefix="198.51.100.0/24", status="active")
        branch = provision_branch(user=self.user, name="Prefix Update Conflict")
        prefix.snapshot()
        prefix.status = "deprecated"
        prefix.save(update_fields=["status"])

        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                self.assertTrue(
                    bulk_orm_apply_simple_models(
                        runner,
                        "ipam.prefix",
                        [
                            {
                                "prefix": "198.51.100.0/24",
                                "vrf": None,
                                "status": "reserved",
                            }
                        ],
                    )
                )
        finally:
            current_request.reset(token)

        change = branch.get_unmerged_changes().get()
        self.assertEqual(change.action, "update")
        diff = ChangeDiff.objects.get(branch=branch, object_id=prefix.pk)
        self.assertEqual(diff.action, "update")
        self.assertEqual(diff.original["status"], "active")
        self.assertEqual(diff.modified["status"], "reserved")
        self.assertEqual(diff.current["status"], "deprecated")
        self.assertIn("status", diff.conflicts)

        fallback = Mock()
        applied, failed, _ = bulk_merge_changes(
            branch,
            branch.get_unmerged_changes().order_by("time"),
            self.request,
            self.user,
            self.logger,
            apply_one=fallback,
        )
        self.assertEqual((applied, failed), (1, 0))
        fallback.assert_not_called()
        prefix.refresh_from_db()
        self.assertEqual(prefix.status, "reserved")

    def test_bulk_prefix_merge_callback_failure_leaves_valid_main_hierarchy(self):
        from forward_netbox.utilities.apply_engine_bulk import (
            bulk_orm_apply_simple_models,
        )

        branch = provision_branch(user=self.user, name="Prefix Merge Interruption")
        runner = self._runner()
        token = current_request.set(self.request)
        try:
            with activate_branch(branch), event_tracking(self.request):
                self.request.id = uuid.uuid4()
                self.request.user = self.user
                bulk_orm_apply_simple_models(
                    runner,
                    "ipam.prefix",
                    [
                        {
                            "prefix": "172.16.0.0/16",
                            "vrf": None,
                            "status": "active",
                        },
                        {
                            "prefix": "172.16.1.0/24",
                            "vrf": None,
                            "status": "active",
                        },
                    ],
                )
        finally:
            current_request.reset(token)

        def interrupt_after_commit(_model):
            raise RuntimeError("injected post-commit interruption")

        apply_one = Mock()
        with self.assertRaisesRegex(RuntimeError, "injected post-commit interruption"):
            bulk_merge_changes(
                branch,
                branch.get_unmerged_changes().order_by("time"),
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
                record_applied=interrupt_after_commit,
            )

        apply_one.assert_not_called()
        merged = list(Prefix.objects.order_by("prefix"))
        self.assertEqual(
            [(item._depth, item._children) for item in merged],
            [(0, 1), (1, 0)],
        )


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


class BulkMergeTagNameCollisionTest(CleanTransactionTestCase):
    """Alternate Tag identities are exact-match, audit-backed, and fail closed."""

    def setUp(self):
        from django.contrib.auth import get_user_model

        self.user = get_user_model().objects.create_user(username="tag-merge-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.logger = logging.getLogger("forward_netbox.tests.bulk_merge")

    def test_mismatched_tag_collision_preserves_unclaimed_operator_tag(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag
        from forward_netbox.utilities import bulk_merge as bulk_merge_module
        from netbox_branching.models import AppliedChange

        branch = provision_branch(user=self.user, name="Tag Collision")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            self.request.user = self.user
            staged_tag = Tag.objects.create(
                name="Mgmt_Coll",
                slug="mgmt-coll",
                color="ff0000",
                description="Forward-managed description",
            )
            staged_tag.object_types.add(ContentType.objects.get_for_model(Site))

        # A same-name operator Tag is not proof that the branch owns its mutable
        # fields. A mismatch must block without adopting or changing the row.
        main_tag = Tag.objects.create(
            name="Mgmt_Coll",
            slug="mgmt-coll",
            color="00ff00",
            description="stale description",
        )
        self.request.id = uuid.uuid4()

        changes = branch.get_unmerged_changes().order_by("time")
        apply_one = Mock(return_value=False)
        record_failed = Mock()
        with patch.object(
            bulk_merge_module,
            "_lock_relationship_writes",
            wraps=bulk_merge_module._lock_relationship_writes,
        ) as relationship_barrier:
            applied, failed, _models = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=apply_one,
                record_failed=record_failed,
            )

        self.assertEqual((applied, failed), (0, 1))
        self.assertTrue(
            any(
                call.args[0] is Tag and set(call.args[1]) == {"object_types"}
                for call in relationship_barrier.call_args_list
            ),
            "same-name tag comparison did not acquire its relationship barrier",
        )
        apply_one.assert_not_called()
        record_failed.assert_called_once()
        self.assertEqual(Tag.objects.filter(name="Mgmt_Coll").count(), 1)
        tag = Tag.objects.get(name="Mgmt_Coll")
        self.assertEqual(tag.color, "00ff00")
        self.assertEqual(tag.description, "stale description")
        self.assertFalse(tag.object_types.exists())
        self.assertFalse(Tag.objects.filter(pk=staged_tag.pk).exists())
        self.assertFalse(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Tag),
                changed_object_id=main_tag.pk,
                action="update",
            ).exists()
        )
        self.assertFalse(AppliedChange.objects.filter(branch=branch).exists())

    def test_unchanged_tag_collision_writes_identity_attestation_once(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag
        from netbox_branching.models import AppliedChange

        branch = provision_branch(user=self.user, name="Tag No-op Attestation")
        object_type = ContentType.objects.get_for_model(Site)
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged_tag = Tag.objects.create(
                name="Mgmt No-op",
                slug="mgmt-no-op",
                color="ff0000",
                description="already converged",
            )
            staged_tag.object_types.add(object_type)

        main_tag = Tag.objects.create(
            name="Mgmt No-op",
            slug="mgmt-no-op",
            color="ff0000",
            description="already converged",
        )
        main_tag.object_types.add(object_type)
        changes = branch.get_unmerged_changes().order_by("time")

        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=Mock(return_value=False),
        )

        self.assertEqual((applied, failed), (1, 0))
        audit = ObjectChange.objects.get(
            changed_object_type=ContentType.objects.get_for_model(Tag),
            changed_object_id=main_tag.pk,
            action="update",
        )
        self.assertEqual(audit.prechange_data, audit.postchange_data)
        self.assertEqual(
            audit.message,
            "Forward NetBox alternate CREATE identity for "
            f"extras.tag:{staged_tag.pk}.",
        )
        self.assertTrue(
            AppliedChange.objects.filter(branch=branch, change=audit).exists()
        )

        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=Mock(return_value=False),
        )
        self.assertEqual((applied, failed), (1, 0))
        self.assertEqual(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Tag),
                changed_object_id=main_tag.pk,
                action="update",
            ).count(),
            1,
        )

    def test_tag_collision_retry_preserves_operator_edit(self):
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag

        branch = provision_branch(user=self.user, name="Tag Collision Edit")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged_tag = Tag.objects.create(
                name="Mgmt Edited",
                slug="mgmt-edited",
                color="ff0000",
                description="Forward-managed",
            )
            staged_tag.object_types.add(ContentType.objects.get_for_model(Site))

        main_tag = Tag.objects.create(
            name="Mgmt Edited",
            slug="mgmt-edited",
            color="ff0000",
            description="Forward-managed",
        )
        main_tag.object_types.add(ContentType.objects.get_for_model(Site))
        changes = branch.get_unmerged_changes().order_by("time")
        bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=Mock(return_value=False),
        )
        Tag.objects.filter(pk=main_tag.pk).update(description="operator edit")
        unsafe_apply = Mock(return_value=False)
        record_failed = Mock()

        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=unsafe_apply,
            record_failed=record_failed,
        )

        self.assertEqual((applied, failed), (0, 1))
        unsafe_apply.assert_not_called()
        record_failed.assert_called_once()
        self.assertEqual(Tag.objects.get(pk=main_tag.pk).description, "operator edit")
        self.assertFalse(Tag.objects.filter(pk=staged_tag.pk).exists())

    def test_tag_collision_retry_does_not_resurrect_deleted_identity(self):
        from extras.models import Tag

        branch = provision_branch(user=self.user, name="Tag Collision Deleted")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged_tag = Tag.objects.create(
                name="Mgmt Deleted",
                slug="mgmt-deleted",
                color="ff0000",
            )

        main_tag = Tag.objects.create(
            name="Mgmt Deleted",
            slug="mgmt-deleted",
            color="ff0000",
        )
        changes = branch.get_unmerged_changes().order_by("time")
        bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=Mock(return_value=False),
        )
        main_tag.delete()
        unsafe_apply = Mock(return_value=False)
        record_failed = Mock()

        applied, failed, _ = bulk_merge_changes(
            branch,
            changes,
            self.request,
            self.user,
            self.logger,
            apply_one=unsafe_apply,
            record_failed=record_failed,
        )

        self.assertEqual((applied, failed), (0, 1))
        unsafe_apply.assert_not_called()
        record_failed.assert_called_once()
        self.assertFalse(Tag.objects.filter(pk=staged_tag.pk).exists())
        self.assertFalse(Tag.objects.filter(name="Mgmt Deleted").exists())

    def test_tag_identity_attestation_failure_leaves_exact_match_unclaimed(self):
        from core.models import ObjectChange
        from django.contrib.contenttypes.models import ContentType
        from extras.models import Tag
        from netbox_branching.models import AppliedChange

        branch = provision_branch(user=self.user, name="Tag Collision Rollback")
        with activate_branch(branch), event_tracking(self.request):
            self.request.id = uuid.uuid4()
            staged_tag = Tag.objects.create(
                name="Mgmt Rollback",
                slug="mgmt-rollback",
                color="ff0000",
                description="desired description",
            )
            staged_tag.object_types.add(ContentType.objects.get_for_model(Site))

        main_tag = Tag.objects.create(
            name="Mgmt Rollback",
            slug="mgmt-rollback",
            color="ff0000",
            description="desired description",
        )
        main_tag.object_types.add(ContentType.objects.get_for_model(Site))
        self.request.id = uuid.uuid4()
        changes = branch.get_unmerged_changes().order_by("time")

        with patch.object(
            AppliedChange.objects,
            "bulk_create",
            side_effect=RuntimeError("lineage write failed"),
        ):
            applied, failed, _models = bulk_merge_changes(
                branch,
                changes,
                self.request,
                self.user,
                self.logger,
                apply_one=Mock(return_value=False),
            )

        self.assertEqual((applied, failed), (0, 1))
        main_tag.refresh_from_db()
        self.assertEqual(main_tag.color, "ff0000")
        self.assertEqual(main_tag.description, "desired description")
        self.assertEqual(
            set(main_tag.object_types.values_list("pk", flat=True)),
            {ContentType.objects.get_for_model(Site).pk},
        )
        self.assertFalse(
            ObjectChange.objects.filter(
                changed_object_type=ContentType.objects.get_for_model(Tag),
                changed_object_id=main_tag.pk,
                action="update",
            ).exists()
        )
        self.assertFalse(AppliedChange.objects.filter(branch=branch).exists())


class BranchingCycleSplitTest(TestCase):
    """The exact 1.1.1 merge path breaks bidirectional CREATE pairs locally."""

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
        _split_bidirectional_create_cycles(to_process, logging.getLogger("cycle-test"))

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

    def test_ordering_uses_local_cycle_splitter(self):
        from forward_netbox.utilities.bulk_merge import (
            _order_collapsed_changes_fast,
        )

        to_process = self._bidirectional_pair()
        ordered = _order_collapsed_changes_fast(
            to_process, logging.getLogger("cycle-test"), operation="merge"
        )
        # The function orders an internal copy: 2 CREATEs plus the synthetic
        # UPDATEs added by the cycle splitter(s). Success = no cycle exception
        # and both original keys present in the ordering.
        ordered_keys = {c.key[:2] for c in ordered}
        for key in to_process:
            self.assertIn(key[:2], ordered_keys)
