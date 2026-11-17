# A customer's inventory system writes its own sites into NetBox. The sync used to
# rename any site it matched to its own lowercase name, so the two systems would
# rename the same site back and forth on every run, and it would delete a site it
# had matched once Forward stopped returning it. A matched site is now used as it
# is, and a site another actor provably created is never deleted by the sync.
import uuid
from types import SimpleNamespace
from unittest.mock import Mock

from core.models import ObjectChange
from core.models import ObjectType
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine_bulk import bulk_orm_apply_simple_models
from forward_netbox.utilities.apply_engine_bulk import (
    CREATE_ONLY_UPDATE_FIELDS_BY_MODEL,
)
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_core_models import _site_created_by_someone_else
from forward_netbox.utilities.sync_core_models import delete_dcim_site


class _Base(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="adopt-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="adopt-sync",
            source=source,
            parameters={"snapshot_id": "latestProcessed"},
        )
        self.request_id = uuid.uuid4()
        ForwardIngestion.objects.create(
            sync=self.sync, change_request_id=self.request_id
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )


class EnsureSiteTest(_Base):
    def test_a_site_matched_by_slug_keeps_its_own_name(self):
        theirs = Site.objects.create(name="Campus-Production", slug="cmp1")

        site = self._runner()._ensure_site({"name": "cmp1", "slug": "cmp1"})

        theirs.refresh_from_db()
        self.assertEqual(site.pk, theirs.pk)
        self.assertEqual(theirs.name, "Campus-Production")

    def test_a_site_matched_by_name_keeps_its_own_slug(self):
        theirs = Site.objects.create(name="campus-production", slug="their-slug")

        site = self._runner()._ensure_site(
            {"name": "campus-production", "slug": "campus-production"}
        )

        theirs.refresh_from_db()
        self.assertEqual(site.pk, theirs.pk)
        self.assertEqual(theirs.slug, "their-slug")

    def test_a_new_site_is_created_when_nothing_matches(self):
        site = self._runner()._ensure_site({"name": "lab", "slug": "lab"})

        self.assertEqual((site.name, site.slug), ("lab", "lab"))


class BulkPathTest(_Base):
    def _mock_runner(self):
        runner = Mock()
        runner.logger = Mock()
        runner.events_clearer = Mock()
        runner._record_issue = Mock()
        runner._content_type_for = lambda model: ContentType.objects.get_for_model(
            model
        )
        runner._dependency_failed = Mock(return_value=False)
        runner._mark_dependency_failed = Mock()
        return runner

    def test_site_name_and_slug_are_create_only(self):
        self.assertEqual(
            CREATE_ONLY_UPDATE_FIELDS_BY_MODEL["dcim.site"], frozenset({"name", "slug"})
        )

    def test_the_bulk_path_does_not_rename_a_matched_site(self):
        theirs = Site.objects.create(name="Campus-Production", slug="cmp1")

        bulk_orm_apply_simple_models(
            self._mock_runner(), "dcim.site", [{"name": "cmp1", "slug": "cmp1"}]
        )

        theirs.refresh_from_db()
        self.assertEqual(theirs.name, "Campus-Production")
        self.assertEqual(Site.objects.filter(slug="cmp1").count(), 1)


class DeleteGuardTest(_Base):
    def _created_under(self, site, request_id):
        ObjectChange.objects.create(
            request_id=request_id,
            user_name="someone",
            changed_object_type=ObjectType.objects.get_for_model(Site),
            changed_object_id=site.pk,
            object_repr=site.name,
            action="create",
            prechange_data=None,
            postchange_data={"name": site.name, "slug": site.slug},
        )

    def test_a_site_another_actor_created_is_kept(self):
        theirs = Site.objects.create(name="Campus-Production", slug="cmp1")
        self._created_under(theirs, uuid.uuid4())
        runner = self._runner()
        runner._record_aggregated_skip_warning = Mock()

        deleted = delete_dcim_site(runner, {"name": "cmp1", "slug": "cmp1"})

        self.assertFalse(deleted)
        self.assertTrue(Site.objects.filter(pk=theirs.pk).exists())
        runner._record_aggregated_skip_warning.assert_called_once()

    def test_a_site_this_sync_created_is_still_deleted(self):
        ours = Site.objects.create(name="old-hall", slug="old-hall")
        self._created_under(ours, self.request_id)

        delete_dcim_site(self._runner(), {"name": "old-hall", "slug": "old-hall"})

        self.assertFalse(Site.objects.filter(pk=ours.pk).exists())

    def test_a_site_with_no_create_record_is_deleted_as_before(self):
        # A fast baseline load writes no change log; absence is not proof.
        site = Site.objects.create(name="no-record", slug="no-record")

        delete_dcim_site(self._runner(), {"name": "no-record", "slug": "no-record"})

        self.assertFalse(Site.objects.filter(pk=site.pk).exists())

    def test_the_guard_reads_main_while_a_branch_is_active(self):
        # Deletes run inside the sync's branch, where netbox_branching routes every
        # change-log read to the branch's own table. The create record that proves
        # another actor made the site is in main, so the guard must read main.
        from netbox_branching.contextvars import active_branch

        theirs = Site.objects.create(name="Campus-Production", slug="cmp1")
        self._created_under(theirs, uuid.uuid4())
        token = active_branch.set(
            SimpleNamespace(schema_name="missing", connection_name="schema_missing")
        )
        try:
            kept = _site_created_by_someone_else(
                self._runner(), {"name": "cmp1", "slug": "cmp1"}
            )
        finally:
            active_branch.reset(token)

        self.assertTrue(kept)


class DeleteNeverCascadesTest(_Base):
    """NetBox cascades a site delete into prefixes and clusters scoped to it."""

    def test_a_site_a_prefix_is_scoped_to_is_kept_with_the_prefix(self):
        from ipam.models import Prefix

        site = Site.objects.create(name="old-hall", slug="old-hall")
        prefix = Prefix.objects.create(prefix="10.20.0.0/24", scope=site)

        with self.assertRaises(ForwardDependencySkipError):
            delete_dcim_site(self._runner(), {"name": "old-hall", "slug": "old-hall"})

        self.assertTrue(Site.objects.filter(pk=site.pk).exists())
        self.assertTrue(Prefix.objects.filter(pk=prefix.pk).exists())

    def test_a_site_a_change_policy_rule_is_scoped_to_is_kept_with_the_rule(self):
        # A relation declared with related_name="+" is invisible to
        # Site._meta.related_objects, and would be cascade-deleted.
        from forward_netbox.models import ForwardChangePolicy
        from forward_netbox.models import ForwardChangePolicyRule

        site = Site.objects.create(name="rule-hall", slug="rule-hall")
        policy = ForwardChangePolicy.objects.create(name="policy-1")
        rule = ForwardChangePolicyRule.objects.create(policy=policy, site=site)

        with self.assertRaises(ForwardDependencySkipError):
            delete_dcim_site(self._runner(), {"name": "rule-hall", "slug": "rule-hall"})

        self.assertTrue(Site.objects.filter(pk=site.pk).exists())
        self.assertTrue(ForwardChangePolicyRule.objects.filter(pk=rule.pk).exists())

    def test_a_site_nothing_points_at_is_deleted(self):
        site = Site.objects.create(name="empty-hall", slug="empty-hall")

        delete_dcim_site(self._runner(), {"name": "empty-hall", "slug": "empty-hall"})

        self.assertFalse(Site.objects.filter(pk=site.pk).exists())
