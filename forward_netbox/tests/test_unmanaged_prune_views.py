# The operator route to delete unmanaged devices Forward does not report. The
# page reviews, then asks for the count to be typed; nothing is queued before
# that, and the job re-checks every device against Forward itself.
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync


class PruneUnmanagedViewTest(TestCase):
    def setUp(self):
        self.source = ForwardSource.objects.create(
            name="pu-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="pu-sync", source=self.source, parameters={}
        )
        maker = Manufacturer.objects.create(name="pu-mfr", slug="pu-mfr")
        kind = DeviceType.objects.create(manufacturer=maker, model="pu", slug="pu")
        role = DeviceRole.objects.create(name="pu-role", slug="pu-role")
        site = Site.objects.create(name="pu-site", slug="pu-site")
        self.devices = [
            Device.objects.create(
                name=f"pu-dev-{index}", device_type=kind, role=role, site=site
            )
            for index in range(2)
        ]
        self.admin = get_user_model().objects.create_superuser(
            username="pu-admin", password="x"
        )
        self.client = Client()
        self.client.force_login(self.admin)
        self.url = (
            reverse("plugins:forward_netbox:device_list_prune_unmanaged")
            + f"?forward_sync={self.sync.pk}&forward_list=unclaimed_absent"
        )

    def _post(self, extra=None):
        data = {"pk": [device.pk for device in self.devices]}
        data.update(extra or {})
        return self.client.post(self.url, data)

    def test_get_goes_back_to_the_list_and_never_acts(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 302)
        self.assertIn("unclaimed-absent-devices", response["Location"])

    def test_the_first_post_reviews_and_queues_nothing(self):
        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            response = self._post()

        self.assertEqual(response.status_code, 200)
        queue.assert_not_called()
        content = response.content.decode()
        self.assertIn("pu-dev-0", content)
        self.assertIn("Type 2 to confirm", content)
        self.assertIn("disabled in Forward", content)

    def test_a_wrong_count_queues_nothing(self):
        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            response = self._post({"_confirm": "1", "confirm_count": "1"})

        queue.assert_not_called()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(Device.objects.filter(pk=self.devices[0].pk).exists())

    def test_the_typed_count_queues_the_job_with_the_named_devices_only(self):
        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_button_job",
            return_value=Mock(pk=77),
        ) as queue:
            response = self._post({"_confirm": "1", "confirm_count": "2"})

        self.assertEqual(response.status_code, 302)
        queue.assert_called_once()
        args, kwargs = queue.call_args
        self.assertEqual(args[1], "prune_unmanaged")
        self.assertEqual(
            kwargs["job_kwargs"],
            {"restrict_to_device_pks": sorted(device.pk for device in self.devices)},
        )
        # The view itself deletes nothing; the job does, after re-checking.
        self.assertEqual(Device.objects.filter(name__startswith="pu-dev-").count(), 2)

    def test_a_user_who_may_not_delete_devices_is_refused(self):
        viewer = get_user_model().objects.create_user(
            username="pu-viewer", password="x"
        )
        client = Client()
        client.force_login(viewer)

        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            response = client.post(
                self.url,
                {
                    "pk": [self.devices[0].pk],
                    "_confirm": "1",
                    "confirm_count": "1",
                },
            )

        self.assertEqual(response.status_code, 403)
        queue.assert_not_called()

    def test_no_selection_queues_nothing(self):
        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            response = self.client.post(
                self.url, {"_confirm": "1", "confirm_count": "0"}
            )

        queue.assert_not_called()
        self.assertEqual(response.status_code, 302)

    def test_the_list_page_renders(self):
        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_unclaimed_absent_devices",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "Unmanaged devices Forward does not report", response.content.decode()
        )

    def test_the_html_view_refuses_devices_outside_the_users_constraint(self):
        from users.models import ObjectPermission

        viewer = get_user_model().objects.create_user(
            username="pu-scoped", password="x"
        )
        allowed = ObjectPermission.objects.create(
            name="pu-delete-site-a",
            actions=["delete", "view"],
            constraints={"site__slug": "no-such-site"},
        )
        allowed.object_types.add(ContentType.objects.get_for_model(Device))
        allowed.users.add(viewer)
        client = Client()
        client.force_login(viewer)

        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            client.post(
                self.url,
                {"pk": [self.devices[0].pk], "_confirm": "1", "confirm_count": "1"},
            )

        queue.assert_not_called()

    def test_the_api_refuses_booleans_and_oversized_lists(self):
        from rest_framework.test import APIClient

        api = APIClient()
        api.force_authenticate(self.admin)
        url = reverse(
            "plugins-api:forward_netbox-api:forwardsync-prune-unmanaged",
            kwargs={"pk": self.sync.pk},
        )

        for bad in ([True], [], list(range(1, 502)), "7", [1.5]):
            with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as q:
                response = api.post(url, {"device_pks": bad}, format="json")
            self.assertEqual(response.status_code, 400, bad)
            q.assert_not_called()

    def test_the_api_refuses_devices_the_user_may_not_delete(self):
        from rest_framework.test import APIClient
        from users.models import ObjectPermission

        viewer = get_user_model().objects.create_user(username="pu-api", password="x")
        grant = ObjectPermission.objects.create(
            name="pu-api-grant",
            actions=["delete", "view"],
            constraints={"site__slug": "no-such-site"},
        )
        grant.object_types.add(ContentType.objects.get_for_model(Device))
        grant.users.add(viewer)
        api = APIClient()
        api.force_authenticate(viewer)
        url = reverse(
            "plugins-api:forward_netbox-api:forwardsync-prune-unmanaged",
            kwargs={"pk": self.sync.pk},
        )

        with patch("forward_netbox.utilities.sync_facade.enqueue_button_job") as queue:
            response = api.post(
                url, {"device_pks": [self.devices[0].pk]}, format="json"
            )

        self.assertEqual(response.status_code, 403)
        queue.assert_not_called()
