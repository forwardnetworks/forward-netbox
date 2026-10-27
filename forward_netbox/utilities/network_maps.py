# Forward Network Maps shown on a NetBox Site page.
#
# A Forward map is a saved, named diagram that Forward keeps current with every
# snapshot. The list carries a name, a folder and who changed it, but no site, so
# the only match to a NetBox Site made here is an exact, case-insensitive name
# match; the panel says so rather than implying the map was found by its devices.
#
# Nothing is stored. The panel makes one list call per source (cached for the
# shared read cache's lifetime, failures included) with a short per-stage timeout,
# no retries and no request pacing, so a slow or refused Forward costs a page one
# note rather than a wait behind a running sync. `PLUGINS_CONFIG["forward_netbox"]
# ["network_maps_panel"] = False` turns the panel off.
import logging
from urllib.parse import urlencode

from netbox.plugins import get_plugin_config
from rq.timeouts import JobTimeoutException

from .forward_api import ForwardClient
from .forward_api import get_network_maps
from .forward_api import NETWORK_MAPS_ERROR
from .forward_api import NETWORK_MAPS_OK

logger = logging.getLogger(__name__)

PANEL_TIMEOUT_SECONDS = 5
PANEL_DETAIL_FALLBACK = "Forward could not be reached."


def map_url(source, network_id, map_id):
    """The Forward app's page for one map.

    Built from the route Forward's own app declares (`/network-maps` with
    `networkId` and `id` query parameters); it is not part of Forward's
    documented API, so this is the one place to change if that route moves.
    """
    base = (getattr(source, "url", "") or "").rstrip("/")
    if not base or not network_id or not map_id:
        return ""
    return f"{base}/network-maps?{urlencode({'networkId': network_id, 'id': map_id})}"


def _matches_site(site, forward_map):
    return site.name.strip().casefold() == forward_map["name"].strip().casefold()


def _source_entry(source, site):
    network_id = source.network_id
    entry = {
        "source": source,
        "network_id": network_id,
        "state": NETWORK_MAPS_OK,
        "detail": "",
        "total": 0,
        "matches": [],
    }
    try:
        client = ForwardClient(
            source, timeout=PANEL_TIMEOUT_SECONDS, retries=0, paced=False
        )
        result = get_network_maps(client, network_id)
    except JobTimeoutException:
        raise
    except Exception:  # noqa: BLE001 - a page must render whatever Forward does
        logger.warning(
            "Forward network maps lookup failed for source %s", source.pk, exc_info=True
        )
        entry["state"] = NETWORK_MAPS_ERROR
        entry["detail"] = PANEL_DETAIL_FALLBACK
        return entry
    entry["state"] = result["state"]
    entry["detail"] = result["detail"]
    entry["total"] = len(result["maps"])
    for forward_map in result["maps"]:
        if _matches_site(site, forward_map):
            entry["matches"].append(
                {**forward_map, "url": map_url(source, network_id, forward_map["id"])}
            )
    return entry


def site_network_maps(site, user):
    """The panel's per-source entries for one Site, or [] when it has nothing to say.

    Only sources the user may view are asked. A source that answered with no map
    for this Site is left out, so a Site without a diagram shows no panel; one
    that was refused or failed stays in, so the operator is told why a map they
    expect is missing.
    """
    from forward_netbox.models import ForwardSource

    if not get_plugin_config("forward_netbox", "network_maps_panel"):
        return []
    if user is None or not getattr(user, "is_authenticated", False):
        return []
    entries = []
    for source in ForwardSource.objects.restrict(user, "view"):
        if not source.network_id:
            continue
        entry = _source_entry(source, site)
        if entry["matches"] or entry["state"] != NETWORK_MAPS_OK:
            entries.append(entry)
    return entries
