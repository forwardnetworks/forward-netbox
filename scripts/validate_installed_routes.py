#!/usr/bin/env python3
"""Render every plugin menu destination from the installed wheel."""
import json
import os
import sys

sys.path.insert(0, "/opt/netbox/netbox")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "netbox.settings")

import django  # noqa: E402

django.setup()

from dcim.models import Device  # noqa: E402
from dcim.models import DeviceRole  # noqa: E402
from dcim.models import DeviceType  # noqa: E402
from dcim.models import Manufacturer  # noqa: E402
from dcim.models import Site  # noqa: E402
from django.contrib.auth import get_user_model  # noqa: E402
from django.contrib.contenttypes.models import ContentType  # noqa: E402
from django.test import Client  # noqa: E402
from django.urls import reverse  # noqa: E402

from forward_netbox.choices import ForwardSyncStatusChoices  # noqa: E402
from forward_netbox.models import ForwardDeviceAnalysis  # noqa: E402
from forward_netbox.models import ForwardDriftPolicy  # noqa: E402
from forward_netbox.models import ForwardIngestion  # noqa: E402
from forward_netbox.models import ForwardIngestionIssue  # noqa: E402
from forward_netbox.models import ForwardNQEMap  # noqa: E402
from forward_netbox.models import ForwardSource  # noqa: E402
from forward_netbox.models import ForwardSync  # noqa: E402
from forward_netbox.models import ForwardValidationRun  # noqa: E402


MENU_ROUTES = (
    (
        "plugins:forward_netbox:forwardsource_list",
        "Artifact route smoke source",
    ),
    (
        "plugins:forward_netbox:forwardsync_list",
        "Artifact route smoke sync",
    ),
    (
        "plugins:forward_netbox:forwardingestion_list",
        "Artifact route smoke sync",
    ),
    (
        "plugins:forward_netbox:forwardvalidationrun_list",
        "Artifact route smoke sync",
    ),
    (
        "plugins:forward_netbox:forwarddeviceanalysis_list",
        "artifact-route-smoke-device",
    ),
    (
        "plugins:forward_netbox:forwardnqemap_list",
        "Artifact route smoke map",
    ),
    (
        "plugins:forward_netbox:forwarddriftpolicy_list",
        "Artifact route smoke policy",
    ),
    (
        "plugins:forward_netbox:forwardingestionissue_list",
        # The exception class, not the message: the list truncates `message`
        # by design, so asserting on it would be asserting on the truncation.
        "ArtifactRouteSmoke",
    ),
)


def _menu_route_names():
    """Every list route the plugin's own menu links to.

    `MENU_ROUTES` is hand-written, because each entry also asserts that a
    fixture row rendered - which cannot be derived. What CAN be derived is
    the set of routes the menu offers, and the two drifting apart is exactly
    what this file's docstring promises does not happen: a menu item was added
    for the ingestion-issue list and this probe went on rendering the same
    seven destinations, so an artifact could ship a menu entry no one had ever
    loaded. The fixture for that list was already here, unused.
    """
    from forward_netbox.navigation import menu

    names = []
    for group in menu.groups:
        # NetBox normalises the (label, items) tuples a plugin declares into
        # `MenuGroup` objects, so this reads the attribute and falls back to
        # unpacking rather than assuming either shape. Assuming the declared
        # one is what made this raise TypeError inside the artifact.
        items = getattr(group, "items", None)
        if items is None:
            _label, items = group
        for item in items:
            link = getattr(item, "link", None)
            if link:
                names.append(link)
    return names


def _require_menu_coverage():
    declared = set(_menu_route_names())
    probed = {route for route, _text in MENU_ROUTES}
    missing = sorted(declared - probed)
    if missing:
        raise SystemExit(
            "the plugin menu offers destinations this probe never renders: "
            f"{missing}. Add each to MENU_ROUTES with a fixture row, so an "
            "artifact cannot ship a menu entry that has never been loaded."
        )
    stale = sorted(probed - declared)
    if stale:
        raise SystemExit(f"MENU_ROUTES names routes the menu no longer offers: {stale}")


def main():
    user = get_user_model().objects.create_user(
        username="forward-netbox-artifact-route-smoke",
        password=None,
    )
    user.is_superuser = True
    user.save()
    source = ForwardSource.objects.create(
        name="Artifact route smoke source",
        type="saas",
        url="https://example.invalid",
        status="ready",
    )
    sync = ForwardSync.objects.create(
        name="Artifact route smoke sync",
        source=source,
        status=ForwardSyncStatusChoices.COMPLETED,
        parameters={"snapshot_id": "artifact-route-smoke"},
    )
    policy = ForwardDriftPolicy.objects.create(name="Artifact route smoke policy")
    validation_run = ForwardValidationRun.objects.create(
        sync=sync,
        policy=policy,
        snapshot_id="artifact-route-smoke",
    )
    ingestion = ForwardIngestion.objects.create(
        sync=sync,
        validation_run=validation_run,
        snapshot_id="artifact-route-smoke",
        baseline_ready=True,
    )
    nqe_map = ForwardNQEMap.objects.create(
        name="Artifact route smoke map",
        netbox_model=ContentType.objects.get(app_label="dcim", model="device"),
        query_id="artifact-route-smoke-query",
    )
    manufacturer = Manufacturer.objects.create(
        name="Artifact route smoke manufacturer",
        slug="artifact-route-smoke-manufacturer",
    )
    device_type = DeviceType.objects.create(
        manufacturer=manufacturer,
        model="Artifact route smoke device type",
        slug="artifact-route-smoke-device-type",
    )
    device_role = DeviceRole.objects.create(
        name="Artifact route smoke role",
        slug="artifact-route-smoke-role",
    )
    site = Site.objects.create(
        name="Artifact route smoke site",
        slug="artifact-route-smoke-site",
    )
    device = Device.objects.create(
        name="artifact-route-smoke-device",
        device_type=device_type,
        role=device_role,
        site=site,
    )
    analysis = ForwardDeviceAnalysis.objects.create(
        sync=sync,
        device=device,
        snapshot_id="artifact-route-smoke",
    )
    ingestion_issue = ForwardIngestionIssue.objects.create(
        ingestion=ingestion,
        message="artifact route smoke issue",
        exception="ArtifactRouteSmoke",
    )

    _require_menu_coverage()

    client = Client()
    client.force_login(user)
    rendered = []
    for route_name, expected_text in MENU_ROUTES:
        url = reverse(route_name)
        response = client.get(url)
        if response.status_code != 200:
            raise SystemExit(
                f"installed route {route_name} returned HTTP {response.status_code}"
            )
        if expected_text.encode() not in response.content:
            raise SystemExit(
                f"installed route {route_name} did not render its fixture row"
            )
        rendered.append({"route": route_name, "status_code": response.status_code})

    # Every registered detail-scoped view, not only the menu lists. The
    # ingestion-issue list route was registered against a view that did not
    # exist and no menu led to it, so the menu probe reported the artifact
    # clean while a click from the ingestion page raised. Enumerating the URL
    # resolver catches that class at the artifact rather than in a customer's
    # browser. A GET refused with 405 is a POST-only action, and a 302 is an
    # action view bouncing back to its object with a message; both count as
    # registered. Anything else must render.
    fixtures = {
        "forwardsource": source.pk,
        "forwardsync": sync.pk,
        "forwardingestion": ingestion.pk,
        "forwardingestionissue": ingestion_issue.pk,
        "forwardvalidationrun": validation_run.pk,
        "forwarddeviceanalysis": analysis.pk,
        "forwardnqemap": nqe_map.pk,
        "forwarddriftpolicy": policy.pk,
    }
    detail = []
    for route_name, pk in _detail_routes(fixtures):
        url = reverse(route_name, kwargs={"pk": pk})
        response = client.get(url)
        if response.status_code not in (200, 302, 405):
            raise SystemExit(
                f"installed detail route {route_name} returned HTTP "
                f"{response.status_code}"
            )
        detail.append({"route": route_name, "status_code": response.status_code})

    print(
        json.dumps(
            {"installed_menu_routes": rendered, "installed_detail_routes": detail},
            sort_keys=True,
        )
    )


def fixture_model_for(route_short_name, fixtures):
    """The fixture a route name belongs to: the LONGEST matching model prefix.

    `forwardingestionissue` also starts with `forwardingestion`, so a
    first-match rule binds the issue's routes to the ingestion's pk and asks
    for an issue that does not exist. The probe then reports HTTP 404 and it
    reads as a broken route rather than a mis-addressed request.
    """
    return max(
        (candidate for candidate in fixtures if route_short_name.startswith(candidate)),
        key=len,
        default=None,
    )


def _detail_routes(fixtures):
    """Every `plugins:forward_netbox:<model>*` route that takes exactly a pk.

    Namespaced names are not in the root resolver's `reverse_dict`; they live
    in the nested resolver two namespaces down, keyed by their short name.
    """
    from django.urls import get_resolver

    _prefix, plugins = get_resolver().namespace_dict["plugins"]
    _prefix, plugin = plugins.namespace_dict["forward_netbox"]
    routes = set()
    for short in plugin.reverse_dict:
        if not isinstance(short, str):
            continue
        model = fixture_model_for(short, fixtures)
        if model is None:
            continue
        for (
            possibilities,
            _pattern,
            _defaults,
            _converters,
        ) in plugin.reverse_dict.getlist(short):
            if any(set(params) == {"pk"} for _result, params in possibilities):
                routes.add((f"plugins:forward_netbox:{short}", fixtures[model]))
    return sorted(routes)


if __name__ == "__main__":
    main()
