# The stale-row removal deletes rows of eleven models, but was gated only on the
# right to delete interfaces. Pressing it needs the right to delete every model
# it can touch. A sync created through the API also starts with the primary-IP
# settings on, as one created in the web form does.
from unittest.mock import patch

from core.models import ObjectType
from django.apps import apps
from django.contrib.auth import get_user_model
from django.test import Client
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APIRequestFactory
from rest_framework.test import force_authenticate
from users.models import ObjectPermission

from forward_netbox.api.serializers import ForwardSyncSerializer
from forward_netbox.api.views import ForwardSyncViewSet
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.stale_row_cleanup import STALE_ROW_CLEANUP_MODELS
from forward_netbox.utilities.sync_facade import apply_new_sync_primary_ip_defaults
from forward_netbox.utilities.sync_facade import button_job_extra_permissions
from forward_netbox.utilities.sync_facade import missing_button_job_permissions

REMOVE = "remove_stale_device_rows"


def grant(user, model, *actions):
    permission = ObjectPermission.objects.create(
        name=f"{user.username}-{model._meta.label_lower}-{'-'.join(actions)}",
        actions=list(actions),
    )
    permission.object_types.add(ObjectType.objects.get_for_model(model))
    permission.users.add(user)


class ButtonJobPermissionFixture(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="perm-src",
            type="saas",
            url="https://forward.example.com",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="perm-sync", source=source, parameters={}
        )

    def user(self, name, *, skip=None):
        """A user who may run the action and delete every cleanup model but ``skip``."""
        user = get_user_model().objects.create_user(username=name, password="x")
        grant(user, ForwardSync, "add", "run", "view", "delete")
        for model_string in STALE_ROW_CLEANUP_MODELS:
            if model_string != skip:
                grant(user, apps.get_model(model_string), "delete")
        return user


class RemovalNeedsEveryDeletePermissionTest(ButtonJobPermissionFixture):
    def test_the_extra_permissions_cover_every_model_the_cleanup_can_delete(self):
        extras = set(button_job_extra_permissions(REMOVE))

        for model_string in STALE_ROW_CLEANUP_MODELS:
            model = apps.get_model(model_string)
            self.assertIn(
                f"{model._meta.app_label}.delete_{model._meta.model_name}", extras
            )

    def test_other_actions_need_no_extra_permissions(self):
        self.assertEqual(button_job_extra_permissions("preview_stale_device_rows"), ())

    def test_the_platform_restore_needs_the_right_to_change_what_it_edits(self):
        self.assertEqual(
            set(button_job_extra_permissions("restore_platforms")),
            {"virtualization.change_virtualmachine", "extras.change_configcontext"},
        )
        user = get_user_model().objects.create_user(username="dev-only", password="x")
        grant(user, ForwardSync, "run")
        grant(user, apps.get_model("dcim.device"), "change")

        self.assertEqual(
            set(missing_button_job_permissions(user, "restore_platforms")),
            {"virtualization.change_virtualmachine", "extras.change_configcontext"},
        )

    def test_a_user_who_may_only_delete_interfaces_is_missing_the_rest(self):
        user = get_user_model().objects.create_user(username="iface-only", password="x")
        grant(user, ForwardSync, "run")
        grant(user, apps.get_model("dcim.interface"), "delete")

        missing = missing_button_job_permissions(user, REMOVE)

        self.assertNotIn("dcim.delete_interface", missing)
        self.assertIn("ipam.delete_ipaddress", missing)
        self.assertIn("netbox_routing.delete_bgppeer", missing)

    def test_a_user_who_may_delete_every_model_is_missing_nothing(self):
        self.assertEqual(
            missing_button_job_permissions(self.user("all-deletes"), REMOVE), []
        )

    def test_the_rest_api_refuses_without_every_delete_permission(self):
        user = self.user("api-short", skip="netbox_routing.bgppeer")
        request = APIRequestFactory().post("/x/", data={}, format="json")
        force_authenticate(request, user=user)

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = ForwardSyncViewSet.as_view({"post": REMOVE})(
                request, pk=self.sync.pk
            )

        self.assertEqual(response.status_code, 403)
        self.assertIn("netbox_routing.delete_bgppeer", str(response.data))
        enqueue.assert_not_called()

    def test_the_rest_api_allows_a_user_with_every_delete_permission(self):
        from core.choices import JobStatusChoices
        from core.models import Job
        import uuid

        from django.contrib.contenttypes.models import ContentType

        user = self.user("api-all")
        request = APIRequestFactory().post("/x/", data={}, format="json")
        force_authenticate(request, user=user)
        job = Job.objects.create(
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name=f"{self.sync.name} - remove stale device rows",
            status=JobStatusChoices.STATUS_COMPLETED,
            job_id=uuid.uuid4(),
        )

        with patch("forward_netbox.jobs.enqueue_forward_job", return_value=job):
            response = ForwardSyncViewSet.as_view({"post": REMOVE})(
                request, pk=self.sync.pk
            )

        self.assertEqual(response.status_code, 201)

    def test_the_page_refuses_without_every_delete_permission(self):
        client = Client()
        client.force_login(self.user("page-short", skip="dcim.module"))

        with patch("forward_netbox.jobs.enqueue_forward_job") as enqueue:
            response = client.post(
                reverse(
                    "plugins:forward_netbox:forwardsync_remove_stale_device_rows",
                    kwargs={"pk": self.sync.pk},
                )
            )

        self.assertEqual(response.status_code, 403)
        enqueue.assert_not_called()


class NewSyncPrimaryIpDefaultsTest(ButtonJobPermissionFixture):
    KEYS = (
        "set_primary_ip_from_forward_management_ip",
        "give_contexts_their_own_management_ip",
        "add_management_interface",
    )

    def test_a_new_api_sync_starts_with_all_three_on(self):
        parameters = apply_new_sync_primary_ip_defaults({})

        for key in self.KEYS:
            self.assertTrue(parameters[key], key)

    def test_what_the_caller_sent_is_kept(self):
        parameters = apply_new_sync_primary_ip_defaults(
            {"add_management_interface": False, "snapshot_id": "latestProcessed"}
        )

        self.assertFalse(parameters["add_management_interface"])
        self.assertTrue(parameters["give_contexts_their_own_management_ip"])
        self.assertEqual(parameters["snapshot_id"], "latestProcessed")

    def test_the_copy_and_the_interface_stay_off_when_the_fallback_is_off(self):
        # Both need the management-IP fallback, so they cannot default on alone.
        parameters = apply_new_sync_primary_ip_defaults(
            {"set_primary_ip_from_forward_management_ip": False}
        )

        self.assertFalse(parameters["set_primary_ip_from_forward_management_ip"])
        self.assertNotIn("add_management_interface", parameters)
        self.assertNotIn("give_contexts_their_own_management_ip", parameters)

    def test_the_serializer_fills_the_defaults_only_when_creating(self):
        creating = ForwardSyncSerializer().validate(
            {"name": "api-new", "source": self.sync.source, "parameters": {}}
        )
        self.assertTrue(creating["parameters"]["add_management_interface"])

        updating = ForwardSyncSerializer(instance=self.sync).validate(
            {"parameters": {"snapshot_id": "latestProcessed"}}
        )
        self.assertNotIn("add_management_interface", updating["parameters"])
