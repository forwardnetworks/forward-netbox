from django.utils.translation import gettext_lazy as _
from utilities.choices import ChoiceSet


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
    "dcim.cablebundle",
    "dcim.cable",
    "dcim.macaddress",
    "ipam.vlan",
    "ipam.vrf",
    "ipam.prefix",
    "ipam.ipaddress",
    "dcim.inventoryitem",
)


class ForwardSourceStatusChoices(ChoiceSet):
    NEW = "new"
    READY = "ready"
    FAILED = "failed"

    CHOICES = (
        (NEW, _("New"), "gray"),
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
