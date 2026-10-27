from types import SimpleNamespace
from unittest import TestCase as UnitTestCase
from unittest.mock import create_autospec
from unittest.mock import patch

from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.test import TestCase
from forward_sdk._sync.services._generated.network_maps import NetworkMapsService
from forward_sdk.errors import ForwardAuthError
from forward_sdk.errors import ForwardPermissionError
from forward_sdk.models import Disp2NetworkMapDirectories
from forward_sdk.models import Disp2NetworkMapList

from forward_netbox.exceptions import ForwardConnectivityError
from forward_netbox.exceptions import ForwardLicenseTierError
from forward_netbox.models import ForwardSource
from forward_netbox.template_content import ForwardSiteNetworkMapsPanel
from forward_netbox.utilities import forward_api_impl
from forward_netbox.utilities.crypto import encrypt_secret
from forward_netbox.utilities.forward_api import ForwardClient
from forward_netbox.utilities.forward_api import get_network_maps
from forward_netbox.utilities.forward_client_errors import translate_client_exception
from forward_netbox.utilities.network_maps import map_url
from forward_netbox.utilities.network_maps import site_network_maps


def _listed(*rows):
    """The list response, built by the SDK's own model from Forward's JSON."""
    return Disp2NetworkMapList.model_validate({"maps": list(rows)})


def _folders(*directories):
    return Disp2NetworkMapDirectories.model_validate({"directories": list(directories)})


def _refused():
    """What a real 403 becomes after the client's own error translation."""
    return translate_client_exception(
        ForwardPermissionError(
            "request failed with HTTP 403: denied", status=403, text="denied"
        )
    )


class GetNetworkMapsTest(UnitTestCase):
    def setUp(self):
        shared_cache = forward_api_impl._shared_read_cache()
        if hasattr(shared_cache, "clear"):
            shared_cache.clear()
        self.client = ForwardClient(
            SimpleNamespace(
                pk=None,
                type="saas",
                url="https://fwd.app",
                parameters={
                    "username": "user@example.com",
                    "password": encrypt_secret("secret"),
                    "verify": True,
                },
            ),
            timeout=5,
            retries=0,
        )
        # Autospecced on the SDK's own service class, so a method the SDK does
        # not have (or one called with an argument it does not take) fails here.
        self.maps_api = create_autospec(NetworkMapsService, instance=True)
        self.client._sdk_client.network_maps = self.maps_api

    def test_lists_maps_with_their_folder_and_attribution(self):
        self.maps_api.get_network_maps.return_value = _listed(
            {
                "id": "m1",
                "name": "Branch 12",
                "pinned": True,
                "createdBy": "ann",
                "updatedAt": "2026-10-01T12:00:00Z",
            },
            {"id": "m2", "name": "Loose map"},
        )
        self.maps_api.get_network_map_directories.return_value = _folders(
            {"path": "/Branches", "mapIds": ["m1"]}
        )

        result = get_network_maps(self.client, "net-1")

        self.assertEqual(result["state"], "ok")
        self.assertTrue(result["directories_available"])
        self.assertEqual(
            result["maps"][0],
            {
                "id": "m1",
                "name": "Branch 12",
                "pinned": True,
                "directory": "/Branches",
                "created_by": "ann",
                "updated_at": "2026-10-01T12:00:00+00:00",
            },
        )
        self.assertEqual(result["maps"][1]["directory"], "")
        self.assertFalse(result["maps"][1]["pinned"])
        self.maps_api.get_network_maps.assert_called_once_with(network_id="net-1")

    def test_an_empty_list_from_a_successful_read_is_ok_not_refused(self):
        self.maps_api.get_network_maps.return_value = _listed()
        self.maps_api.get_network_map_directories.return_value = _folders()

        result = get_network_maps(self.client, "net-1")

        self.assertEqual(result["state"], "ok")
        self.assertEqual(result["maps"], [])

    def test_a_403_is_refused_and_a_listing_error_is_an_error(self):
        self.maps_api.get_network_maps.side_effect = _refused()
        self.assertEqual(get_network_maps(self.client, "net-1")["state"], "refused")

        self.maps_api.get_network_maps.side_effect = ForwardLicenseTierError("tier")
        self.assertEqual(get_network_maps(self.client, "net-3")["state"], "refused")

        self.maps_api.get_network_maps.side_effect = ForwardConnectivityError("down")
        result = get_network_maps(self.client, "net-2")
        self.assertEqual(result["state"], "error")
        self.assertIn("down", result["detail"])

    def test_a_401_is_a_credential_failure_not_a_maps_refusal(self):
        self.maps_api.get_network_maps.side_effect = translate_client_exception(
            ForwardAuthError(
                "request failed with HTTP 401: bad login", status=401, text="bad"
            )
        )

        self.assertEqual(get_network_maps(self.client, "net-1")["state"], "error")

    def test_an_unexpected_exception_is_cached_as_an_error_not_raised(self):
        self.maps_api.get_network_maps.side_effect = RuntimeError("odd")

        first = get_network_maps(self.client, "net-1")
        second = get_network_maps(self.client, "net-1")

        self.assertEqual(first["state"], "error")
        self.assertEqual(second["state"], "error")
        self.maps_api.get_network_maps.assert_called_once()

    def test_a_changed_response_shape_degrades_to_an_error(self):
        self.maps_api.get_network_maps.side_effect = ValueError("bad shape")

        result = get_network_maps(self.client, "net-1")

        self.assertEqual(result["state"], "error")

    def test_the_maps_still_list_when_directories_are_unavailable(self):
        self.maps_api.get_network_maps.return_value = _listed(
            {"id": "m1", "name": "Branch 12"}
        )
        self.maps_api.get_network_map_directories.side_effect = _refused()

        result = get_network_maps(self.client, "net-1")

        self.assertEqual(result["state"], "ok")
        self.assertFalse(result["directories_available"])
        self.assertEqual(result["maps"][0]["directory"], "")

    def test_one_list_call_serves_repeat_reads_and_failures_are_cached_too(self):
        self.maps_api.get_network_maps.return_value = _listed()
        self.maps_api.get_network_map_directories.return_value = _folders()
        get_network_maps(self.client, "net-1")
        get_network_maps(self.client, "net-1")
        self.maps_api.get_network_maps.assert_called_once()
        self.maps_api.get_network_map_directories.assert_called_once()

        self.maps_api.get_network_maps.reset_mock()
        self.maps_api.get_network_maps.side_effect = ForwardConnectivityError("down")
        get_network_maps(self.client, "net-2")
        get_network_maps(self.client, "net-2")
        self.maps_api.get_network_maps.assert_called_once()

    def test_rows_without_an_id_or_name_are_dropped(self):
        self.maps_api.get_network_maps.return_value = _listed(
            {"id": "", "name": "No id"},
            {"id": "m2", "name": " "},
            {"id": "m3", "name": "Kept"},
        )
        self.maps_api.get_network_map_directories.return_value = _folders()

        result = get_network_maps(self.client, "net-1")

        self.assertEqual([item["id"] for item in result["maps"]], ["m3"])


class PageClientSettingsTest(UnitTestCase):
    def _source(self, **parameters):
        return SimpleNamespace(
            pk=None,
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": encrypt_secret("secret"),
                "verify": True,
                **parameters,
            },
        )

    def test_the_page_overrides_win_and_turn_pacing_off(self):
        client = ForwardClient(
            self._source(timeout=1200, retries=3, api_requests_per_minute=30),
            timeout=5,
            retries=0,
            paced=False,
        )

        self.assertEqual(client.timeout, 5)
        self.assertEqual(client.retries, 0)
        self.assertEqual(client.api_requests_per_minute, 0)
        self.assertEqual(client._api_request_min_interval, 0.0)

    def test_without_overrides_the_source_settings_still_apply(self):
        client = ForwardClient(
            self._source(timeout=1200, retries=3, api_requests_per_minute=30)
        )

        self.assertEqual(client.timeout, 1200)
        self.assertEqual(client.retries, 3)
        self.assertEqual(client.api_requests_per_minute, 30)
        self.assertEqual(client._api_request_min_interval, 2.0)


class MapUrlTest(UnitTestCase):
    def test_builds_the_forward_app_route_with_the_network_and_map(self):
        source = SimpleNamespace(url="https://fwd.app/")
        self.assertEqual(
            map_url(source, "net-1", "m 1"),
            "https://fwd.app/network-maps?networkId=net-1&id=m+1",
        )

    def test_no_link_when_any_part_is_missing(self):
        source = SimpleNamespace(url="https://fwd.app")
        self.assertEqual(map_url(source, "", "m1"), "")
        self.assertEqual(map_url(source, "net-1", ""), "")
        self.assertEqual(map_url(SimpleNamespace(url=""), "net-1", "m1"), "")


class SiteNetworkMapsPanelTest(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser("maps-admin")
        self.viewer = user_model.objects.create_user("maps-viewer")
        self.site = Site.objects.create(name="Branch 12", slug="branch-12")
        self.source = ForwardSource.objects.create(
            name="maps-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "u@example.com",
                "password": "p",
                "verify": True,
                "network_id": "net-1",
            },
        )
        self.lookup = patch(
            "forward_netbox.utilities.network_maps.get_network_maps",
            return_value={
                "state": "ok",
                "detail": "",
                "directories_available": True,
                "maps": [
                    {
                        "id": "m1",
                        "name": "BRANCH 12",
                        "pinned": False,
                        "directory": "/Branches",
                        "created_by": "ann",
                        "updated_at": "",
                    },
                    {
                        "id": "m2",
                        "name": "Branch 120",
                        "pinned": False,
                        "directory": "",
                        "created_by": "",
                        "updated_at": "",
                    },
                ],
            },
        )
        self.get_network_maps = self.lookup.start()
        self.addCleanup(self.lookup.stop)

    def _entries(self, user=None):
        return site_network_maps(self.site, user or self.admin)

    def test_matches_only_the_exact_name_ignoring_case(self):
        entries = self._entries()

        self.assertEqual(len(entries), 1)
        self.assertEqual([m["id"] for m in entries[0]["matches"]], ["m1"])
        self.assertEqual(entries[0]["total"], 2)
        self.assertEqual(
            entries[0]["matches"][0]["url"],
            "https://fwd.app/network-maps?networkId=net-1&id=m1",
        )

    def test_a_site_without_a_map_shows_nothing_when_forward_answered(self):
        other = Site.objects.create(name="Branch 99", slug="branch-99")

        self.assertEqual(site_network_maps(other, self.admin), [])

    def test_a_refusal_is_kept_so_the_operator_is_told_why(self):
        self.get_network_maps.return_value = {
            "state": "refused",
            "detail": "HTTP 403",
            "directories_available": False,
            "maps": [],
        }

        entries = self._entries()

        self.assertEqual(entries[0]["state"], "refused")
        self.assertEqual(entries[0]["matches"], [])

    def test_a_source_the_user_cannot_view_is_never_asked(self):
        self.assertEqual(self._entries(self.viewer), [])
        self.get_network_maps.assert_not_called()

    def test_an_unauthenticated_request_is_never_asked(self):
        self.assertEqual(site_network_maps(self.site, None), [])
        self.get_network_maps.assert_not_called()

    def test_a_source_without_a_network_is_skipped(self):
        self.source.parameters = {**self.source.parameters, "network_id": ""}
        self.source.save()

        self.assertEqual(self._entries(), [])
        self.get_network_maps.assert_not_called()

    def test_an_unexpected_failure_in_the_lookup_renders_a_note_not_a_500(self):
        self.get_network_maps.side_effect = RuntimeError("boom")

        entries = self._entries()

        self.assertEqual(entries[0]["state"], "error")
        self.assertEqual(entries[0]["matches"], [])

    def test_the_extension_renders_the_matched_map_and_links_it(self):
        request = SimpleNamespace(user=self.admin)
        panel = ForwardSiteNetworkMapsPanel(
            context={"object": self.site, "request": request}
        )

        rendered = panel.right_page()

        self.assertIn("Forward Network Maps", rendered)
        self.assertIn("BRANCH 12", rendered)
        self.assertIn("/Branches", rendered)
        self.assertIn(
            "https://fwd.app/network-maps?networkId=net-1&amp;id=m1", rendered
        )
        self.assertIn("Matched by exact name", rendered)
        self.assertNotIn("Branch 120", rendered)

    def test_the_extension_renders_nothing_without_a_match(self):
        other = Site.objects.create(name="Branch 99", slug="branch-99")
        request = SimpleNamespace(user=self.admin)
        panel = ForwardSiteNetworkMapsPanel(
            context={"object": other, "request": request}
        )

        self.assertEqual(panel.right_page(), "")

    def test_the_extension_renders_the_error_note_with_its_detail(self):
        self.get_network_maps.return_value = {
            "state": "error",
            "detail": "HTTP 502",
            "directories_available": False,
            "maps": [],
        }
        request = SimpleNamespace(user=self.admin)
        panel = ForwardSiteNetworkMapsPanel(
            context={"object": self.site, "request": request}
        )

        rendered = panel.right_page()

        self.assertIn("could not be read", rendered)
        self.assertIn("HTTP 502", rendered)

    @override_settings(PLUGINS_CONFIG={"forward_netbox": {"network_maps_panel": False}})
    def test_the_setting_turns_the_panel_off_without_calling_forward(self):
        self.assertEqual(self._entries(), [])
        self.get_network_maps.assert_not_called()

    def test_a_user_with_view_permission_on_the_source_is_asked(self):
        from django.contrib.contenttypes.models import ContentType
        from users.models import ObjectPermission

        permission = ObjectPermission.objects.create(
            name="view maps source", actions=["view"]
        )
        permission.object_types.set([ContentType.objects.get_for_model(ForwardSource)])
        permission.users.add(self.viewer)

        entries = self._entries(self.viewer)

        self.assertEqual([m["id"] for m in entries[0]["matches"]], ["m1"])

    def test_the_extension_names_the_missing_permission_on_a_refusal(self):
        self.get_network_maps.return_value = {
            "state": "refused",
            "detail": "",
            "directories_available": False,
            "maps": [],
        }
        request = SimpleNamespace(user=self.admin)
        panel = ForwardSiteNetworkMapsPanel(
            context={"object": self.site, "request": request}
        )

        rendered = panel.right_page()

        self.assertIn("Forward refused the maps request", rendered)
        self.assertIn("license", rendered)
