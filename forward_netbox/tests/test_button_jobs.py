# Shared overlap-guard/enqueue path for the operator "button jobs"
# (dependency preview, prune orphans, delete-eligible IPAM tagging, module-bay
# creation). The HTML buttons and the REST API actions must produce
# byte-identical job names (several lookups match on these strings) and refuse
# to stack duplicates.
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.sync_facade import BUTTON_JOB_SPECS
from forward_netbox.utilities.sync_facade import enqueue_button_job
from forward_netbox.utilities.sync_facade import JobAlreadyActive


class ButtonJobEnqueueTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.source = ForwardSource.objects.create(
            name="button-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="button-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _job(self, name, status=JobStatusChoices.STATUS_PENDING, job_id_suffix="0"):
        return Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=name,
            status=status,
            job_id=f"123e4567-e89b-12d3-a456-42661417400{job_id_suffix}",
        )

    def test_job_names_are_byte_identical_to_the_legacy_strings(self):
        # Several lookups couple to these strings (drift report + preview GET
        # use icontains "dependency preview"; the webhook checks "- adhoc"/
        # "- scheduled"; auto-prune appends " (auto)"). Renaming breaks them.
        expected = {
            "dependency_preview": "dependency preview",
            "prune_orphans": "prune orphans",
            "tag_delete_eligible_ipam": "tag delete-eligible IPAM",
            "create_module_bays": "create module bays",
        }
        for kind, suffix in expected.items():
            self.assertEqual(BUTTON_JOB_SPECS[kind][1], suffix, kind)

    def test_enqueue_uses_spec_path_and_name(self):
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=1),
        ) as enqueue:
            enqueue_button_job(self.sync, "dependency_preview", None)
        kwargs = enqueue.call_args.kwargs
        self.assertEqual(kwargs["name"], "button-sync - dependency preview")
        self.assertIs(kwargs["instance"], self.sync)

    def test_pending_duplicate_raises(self):
        self._job("button-sync - dependency preview")
        with patch("forward_netbox.utilities.sync_facade.Job.enqueue") as enqueue:
            with self.assertRaises(JobAlreadyActive):
                enqueue_button_job(self.sync, "dependency_preview", None)
        enqueue.assert_not_called()

    def test_completed_job_does_not_block(self):
        self._job(
            "button-sync - dependency preview",
            status=JobStatusChoices.STATUS_COMPLETED,
        )
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=2),
        ) as enqueue:
            enqueue_button_job(self.sync, "dependency_preview", None)
        enqueue.assert_called_once()

    def test_auto_variant_blocks_manual_prune(self):
        # Prefix semantics: "prune orphans (auto)" blocks a manual
        # "prune orphans" click (the exact-match guard used to miss this).
        self._job(
            "button-sync - prune orphans (auto)",
            status=JobStatusChoices.STATUS_RUNNING,
        )
        with self.assertRaises(JobAlreadyActive):
            enqueue_button_job(self.sync, "prune_orphans", None)

    def test_manual_prune_blocks_auto_variant(self):
        self._job("button-sync - prune orphans")
        with self.assertRaises(JobAlreadyActive):
            enqueue_button_job(
                self.sync,
                "prune_orphans",
                None,
                name_suffix_extra=" (auto)",
                during_sync_ok=True,
            )

    def test_active_sync_blocks_prune_but_not_other_kinds(self):
        self._job("button-sync - adhoc", status=JobStatusChoices.STATUS_RUNNING)
        with self.assertRaises(JobAlreadyActive):
            enqueue_button_job(self.sync, "prune_orphans", None)
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=3),
        ) as enqueue:
            enqueue_button_job(self.sync, "dependency_preview", None)
        enqueue.assert_called_once()

    def test_during_sync_ok_bypasses_only_the_sync_check(self):
        # The post-sync auto-prune hook enqueues from inside the still-running
        # sync job; it must bypass the sync-running check but still honor the
        # duplicate-prune guard.
        self._job("button-sync - adhoc", status=JobStatusChoices.STATUS_RUNNING)
        with patch(
            "forward_netbox.utilities.sync_facade.Job.enqueue",
            return_value=Mock(pk=4),
        ) as enqueue:
            enqueue_button_job(
                self.sync,
                "prune_orphans",
                None,
                name_suffix_extra=" (auto)",
                during_sync_ok=True,
            )
        enqueue.assert_called_once()
        self.assertEqual(
            enqueue.call_args.kwargs["name"],
            "button-sync - prune orphans (auto)",
        )


class ButtonJobAPIActionTest(TestCase):
    """REST parity for the four button jobs (chunk 2): per-action permission,
    201 + JobSerializer on success, 409 when an equivalent job is active."""

    KINDS = (
        ("dependency_preview", "dependency preview"),
        ("prune_orphans", "prune orphans"),
        ("tag_delete_eligible_ipam", "tag delete-eligible IPAM"),
        ("create_module_bays", "create module bays"),
    )

    @classmethod
    def setUpTestData(cls):
        from django.contrib.auth import get_user_model

        User = get_user_model()
        cls.admin = User.objects.create_superuser(
            username="btn_admin",
            password="TestPassword123!",
            email="btn_admin@example.com",
        )
        cls.plain_user = User.objects.create_user(
            username="btn_plain", password="TestPassword123!"
        )
        cls.source = ForwardSource.objects.create(
            name="btn-api-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "net-1",
            },
        )
        cls.sync = ForwardSync.objects.create(
            name="btn-api-sync",
            source=cls.source,
            parameters={"snapshot_id": "latestProcessed"},
        )

    def _post(self, user, action_name):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        factory = APIRequestFactory()
        request = factory.post(f"/api/plugins/forward/sync/{self.sync.pk}/x/")
        force_authenticate(request, user=user)
        view = ForwardSyncViewSet.as_view({"post": action_name})
        return view(request, pk=self.sync.pk)

    def test_actions_enqueue_with_permission(self):
        for index, (kind, suffix) in enumerate(self.KINDS):
            with self.subTest(kind=kind):
                real_job = Job.objects.create(
                    object_type=ContentType.objects.get_for_model(ForwardSync),
                    object_id=self.sync.pk,
                    name=f"btn-api-sync - {suffix}",
                    status=JobStatusChoices.STATUS_COMPLETED,
                    job_id=f"123e4567-e89b-12d3-a456-4266141741{index:02d}",
                )
                with patch(
                    "forward_netbox.utilities.sync_facade.Job.enqueue",
                    return_value=real_job,
                ) as enqueue:
                    response = self._post(self.admin, kind)
                self.assertEqual(response.status_code, 201, kind)
                self.assertEqual(response.data["name"], f"btn-api-sync - {suffix}")
                enqueue.assert_called_once()

    def test_actions_403_without_permission(self):
        for kind, _suffix in self.KINDS:
            with self.subTest(kind=kind):
                with patch(
                    "forward_netbox.utilities.sync_facade.Job.enqueue"
                ) as enqueue:
                    response = self._post(self.plain_user, kind)
                self.assertEqual(response.status_code, 403, kind)
                enqueue.assert_not_called()

    def test_active_job_maps_to_409(self):
        Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="btn-api-sync - dependency preview",
            status=JobStatusChoices.STATUS_RUNNING,
            job_id="123e4567-e89b-12d3-a456-426614174199",
        )
        response = self._post(self.admin, "dependency_preview")
        self.assertEqual(response.status_code, 409)
        self.assertIn("already", response.data["detail"])
