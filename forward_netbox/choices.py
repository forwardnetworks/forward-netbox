from django.conf import settings
from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet


FORWARD_BGP_MODELS = (
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfinstance",
    "netbox_routing.ospfarea",
    "netbox_routing.ospfinterface",
    "netbox_peering_manager.peeringsession",
)

FORWARD_SUPPORTED_MODELS = (
    "dcim.site",
    "dcim.manufacturer",
    "dcim.devicerole",
    "dcim.platform",
    "dcim.devicetype",
    "dcim.device",
    "dcim.virtualchassis",
    "extras.taggeditem",
    "dcim.interface",
    "dcim.cable",
    "dcim.macaddress",
    "ipam.vlan",
    "ipam.vrf",
    "ipam.prefix",
    "ipam.ipaddress",
    "dcim.inventoryitem",
    "dcim.module",
    *FORWARD_BGP_MODELS,
)

FORWARD_OPTIONAL_MODELS = {
    "dcim.module",
    *FORWARD_BGP_MODELS,
}


def forward_plugin_settings():
    return (getattr(settings, "PLUGINS_CONFIG", {}) or {}).get("forward_netbox", {})


def forward_bgp_sync_enabled():
    return bool(forward_plugin_settings().get("enable_bgp_sync", False))


def forward_configured_models():
    if forward_bgp_sync_enabled():
        return FORWARD_SUPPORTED_MODELS
    return tuple(
        model_string
        for model_string in FORWARD_SUPPORTED_MODELS
        if model_string not in FORWARD_BGP_MODELS
    )


class ForwardSourceStatusChoices(ChoiceSet):
    NEW = "new"
    SYNCING = "syncing"
    READY = "ready"
    FAILED = "failed"

    CHOICES = (
        (NEW, _("New"), "gray"),
        (SYNCING, _("Syncing"), "cyan"),
        (READY, _("Ready"), "green"),
        (FAILED, _("Failed"), "red"),
    )


class ForwardSourceDeploymentChoices(ChoiceSet):
    SAAS = "saas"
    CUSTOM = "custom"

    CHOICES = (
        (SAAS, _("Forward SaaS"), "blue"),
        (CUSTOM, _("Custom Forward deployment"), "cyan"),
    )


class ForwardSyncStatusChoices(ChoiceSet):
    NEW = "new"
    QUEUED = "queued"
    SYNCING = "syncing"
    READY_TO_MERGE = "ready_to_merge"
    MERGING = "merging"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"

    CHOICES = (
        (NEW, _("New"), "blue"),
        (QUEUED, _("Queued"), "orange"),
        (SYNCING, _("Syncing"), "cyan"),
        (READY_TO_MERGE, _("Ready to merge"), "purple"),
        (MERGING, _("Merging"), "cyan"),
        (COMPLETED, _("Completed"), "green"),
        (FAILED, _("Failed"), "red"),
        (TIMEOUT, _("Timeout"), "pink"),
    )


class ForwardExecutionBackendChoices(ChoiceSet):
    BRANCHING = "branching"
    FAST_BOOTSTRAP = "fast_bootstrap"

    CHOICES = (
        (BRANCHING, _("Branching"), "blue"),
        (FAST_BOOTSTRAP, _("Fast bootstrap"), "cyan"),
    )


class ForwardIngestionPhaseChoices(ChoiceSet):
    SYNC = "sync"
    MERGE = "merge"

    CHOICES = (
        (SYNC, _("Sync"), "blue"),
        (MERGE, _("Merge"), "purple"),
    )


class ForwardValidationStatusChoices(ChoiceSet):
    QUEUED = "queued"
    RUNNING = "running"
    PASSED = "passed"
    BLOCKED = "blocked"
    FAILED = "failed"

    CHOICES = (
        (QUEUED, _("Queued"), "orange"),
        (RUNNING, _("Running"), "cyan"),
        (PASSED, _("Passed"), "green"),
        (BLOCKED, _("Blocked"), "red"),
        (FAILED, _("Failed"), "red"),
    )


class ForwardDriftPolicyBaselineChoices(ChoiceSet):
    LATEST_MERGED = "latest_merged"
    NONE = "none"

    CHOICES = (
        (LATEST_MERGED, _("Latest merged ingestion"), "blue"),
        (NONE, _("No baseline"), "gray"),
    )
