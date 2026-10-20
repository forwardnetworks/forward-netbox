# A change's device scope is a Forward workspace: a network forked from the
# source's network that holds only the devices the change touches. Queries then
# run on a handful of devices against the workspace's own baseline, instead of on
# the whole estate.
#
# Everything that talks to Forward goes through an `api` object with four small
# methods, so the rules here are tested without a network and the real
# implementation (`ForwardWorkspaceApi`) is the only part that is not.
from dataclasses import dataclass

from django.db import transaction
from django.utils import timezone

from .actions import _journal
from .choices import ForwardChangeStateChoices as State

DEFAULT_RETENTION_DAYS = 14

# A workspace's baseline is the snapshot it was forked or imported with; later
# snapshots in it are collections made after the change.
BASELINE_TRIGGERS = frozenset({"IMPORT", "FORK"})
COLLECTION_TRIGGER = "COLLECTION"

ORIGIN_CREATED = "created"
ORIGIN_ADOPTED = "adopted"


@dataclass(frozen=True)
class WorkspaceOutcome:
    ok: bool
    message: str = ""

    def __bool__(self):
        return self.ok


def _refuse(message):
    return WorkspaceOutcome(False, message)


class ForwardWorkspaceApi:
    """The real Forward calls, for one source. Not unit-tested: it needs Forward."""

    def __init__(self, source):
        self.client = source.get_client()

    def _sdk(self, call, *args, **kwargs):
        return self.client._call_sdk(call, *args, **kwargs)

    def network(self, network_id):
        for network in self._sdk(self.client._sdk_client.networks.list) or []:
            if str(network.id) == str(network_id):
                return {
                    "id": str(network.id),
                    "name": network.name,
                    "parent_id": network.parent_id,
                }
        return None

    def create_workspace(self, parent_id, *, name, devices, retention_days):
        created = self._sdk(
            self.client._sdk_client.networks.create_workspace,
            parent_id,
            name=name,
            retention_days=retention_days,
            devices=list(devices),
        )
        return str(created.id)

    def snapshots(self, network_id):
        rows = []
        for item in self._sdk(self.client._sdk_client.snapshots.list, network_id) or []:
            rows.append(
                {
                    "id": str(item.id),
                    "state": str(getattr(item.state, "value", item.state) or ""),
                    "trigger": str(
                        getattr(
                            item.processing_trigger, "value", item.processing_trigger
                        )
                        or ""
                    ),
                    "created_at": item.created_at or "",
                }
            )
        return rows

    def collection_access(self, network_id):
        """The account's Forward roles, read-only: ``{org_roles, network_roles}``.

        Forward has sent the org roles as a list and as a bare string, so both are
        read; ``unknown`` is true when it sent no role information at all, which is
        not the same as an account that holds none.
        """
        me = self._sdk(self.client._sdk_client.user_accounts.get_current_user)
        roles = getattr(me, "roles", None)
        if hasattr(roles, "model_dump"):
            roles = roles.model_dump()
        roles = roles or {}
        org = roles.get("org")
        network = roles.get("network")
        if isinstance(org, str):
            org = [org]
        return {
            "unknown": org is None and not network,
            "org_roles": [str(r).strip().upper() for r in (org or [])],
            "network_roles": {
                str(k): str(v).strip().upper() for k, v in (network or {}).items()
            },
        }

    def start_collection(self, network_id):
        """Queue a collection on the network and return its task id."""
        run = self._sdk(
            self.client._sdk_client.snapshots.collect, network_id, wait=False
        )
        return str(getattr(run, "task_id", "") or "")

    def run_query(self, query_id, commit_id, network_id, snapshot_id):
        from ..utilities.forward_api import run_nqe_query

        return run_nqe_query(
            self.client,
            query_id=query_id,
            commit_id=commit_id,
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )

    def capabilities(self, network_id):
        """Predict and Forward AI availability, from two read-only SDK calls."""
        from .capabilities import _probe

        sdk = self.client._sdk_client
        return {
            "predict": _probe(lambda: self._sdk(sdk.change_sets.list, network_id)),
            "ai": _probe(lambda: self._sdk(sdk.ai.list)),
        }

    def resolve_query(self, query_path, commit_id=None):
        """``(query_id, commit_id)`` an org query path resolves to, via the SDK."""
        from ..utilities.forward_api import resolve_nqe_query_reference

        resolved = resolve_nqe_query_reference(
            self.client,
            repository="org",
            query_path=query_path,
            commit_id=commit_id or None,
        )
        query_id = str(resolved.get("queryId") or resolved.get("query_id") or "")
        commit = str(resolved.get("commitId") or resolved.get("commit_id") or "")
        return query_id, commit

    def checks(self, snapshot_id):
        """Forward's intent checks on a snapshot, as plain dicts, via the SDK."""
        rows = []
        for item in (
            self._sdk(
                self.client._sdk_client.checks.get_checks, snapshot_id=snapshot_id
            )
            or []
        ):
            definition = getattr(item, "definition", None)
            rows.append(
                {
                    "name": str(getattr(item, "name", "") or ""),
                    "type": str(
                        getattr(
                            getattr(definition, "check_type", None),
                            "value",
                            getattr(definition, "check_type", ""),
                        )
                        or ""
                    ),
                    "status": str(
                        getattr(getattr(item, "status", None), "value", item.status)
                        or ""
                    ),
                    "priority": str(
                        getattr(
                            getattr(item, "priority", None),
                            "value",
                            getattr(item, "priority", ""),
                        )
                        or ""
                    ),
                    "violations": int(getattr(item, "num_violations", 0) or 0),
                }
            )
        return rows

    def estate_device_names(self, network_id):
        """Every device name on the network's latest processed snapshot, in one call."""
        from ..utilities.forward_api import get_latest_processed_snapshot_id
        from ..utilities.forward_api import run_nqe_query

        snapshot_id = get_latest_processed_snapshot_id(self.client, network_id)
        rows = run_nqe_query(
            self.client,
            query="foreach d in network.devices select {name: d.name}",
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )
        return [row["name"] for row in rows or [] if row.get("name")]

    def device_collection(self, network_id, snapshot_id):
        """``{device name: collection time}`` for the devices the snapshot has."""
        from ..utilities.forward_api import run_nqe_query

        rows = run_nqe_query(
            self.client,
            query=(
                "foreach d in network.devices "
                "where d.snapshotInfo.result == DeviceSnapshotResult.completed "
                "select {name: d.name, collected: d.snapshotInfo.collectionTime}"
            ),
            network_id=network_id,
            snapshot_id=snapshot_id,
            fetch_all=True,
        )
        return {row["name"]: row.get("collected") for row in rows or []}


def workspace_device_names(change):
    """The classic-device names Forward scopes a workspace by, sorted.

    `create_workspace(devices=...)` takes Forward's own device names, which are kept
    on the change's device rows. There is no fallback to the NetBox name: the verify
    gate checks completeness by the Forward name, so a device scoped by one name and
    checked by another would be in the workspace and never checked.
    """
    return sorted(
        {
            (row.forward_device_key or "").strip()
            for row in change.devices.all()
            if (row.forward_device_key or "").strip()
        }
    )


def unresolved_devices(change):
    """Device rows with no Forward name yet, which the gate would skip."""
    return sorted(
        str(row.device)
        for row in change.devices.select_related("device")
        if not (row.forward_device_key or "").strip()
    )


def _workspace_name(change):
    label = (change.ref or "").strip() or f"change-{change.pk}"
    return f"{label}-{change.pk}"[:100]


def _source_network(change):
    return (change.source.parameters or {}).get("network_id") or ""


# A workspace is forked from the network as it is NOW, so it can only be attached
# before the change is applied: a baseline taken afterwards already contains the
# change.
SCOPABLE_STATES = frozenset(
    {
        State.DRAFT,
        State.SCOPED,
        State.BASELINED,
        State.STAGED,
        State.PREDICTED,
        State.APPROVED,
    }
)


def _open(change):
    return change.state in SCOPABLE_STATES


def _open_api(change, api):
    """The Forward API, or the reason it could not be built (bad source)."""
    if api is not None:
        return api, None
    try:
        return ForwardWorkspaceApi(change.source), None
    except Exception as exc:  # noqa: BLE001
        return None, f"The Forward source could not be used: {exc}"


def create_workspace(change, user, *, api=None, retention_days=DEFAULT_RETENTION_DAYS):
    """Fork the source's network into a workspace holding this change's devices.

    A write to Forward, so it is a user action on a change and never a background
    job; the change is journaled with who asked, because Forward records only the
    source's service account. The row is locked across the check and the call, so a
    double click cannot fork two workspaces and orphan the first.
    """
    from ..models import ForwardChange

    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().get(pk=change.pk)
        if locked.workspace_network_id:
            return _refuse("This change already has a workspace.")
        if not _open(locked):
            return _refuse(
                "A workspace can only be created before the change is applied."
            )
        unresolved = unresolved_devices(locked)
        if unresolved:
            return _refuse(
                f"{len(unresolved)} device(s) have no Forward name yet, so the "
                f"verify gate could not check them: {', '.join(unresolved[:5])}."
            )
        names = workspace_device_names(locked)
        if not names:
            return _refuse(
                "Add at least one device to the change before creating a workspace."
            )
        parent = _source_network(locked)
        if not parent:
            return _refuse("The source has no network selected.")
        api, error = _open_api(locked, api)
        if error:
            return _refuse(error)
        try:
            network_id = api.create_workspace(
                parent,
                name=_workspace_name(locked),
                devices=names,
                retention_days=retention_days,
            )
        except Exception as exc:  # noqa: BLE001 - reported, never raised past the page
            return _refuse(f"Forward could not create the workspace: {exc}")
        locked.workspace_network_id = network_id
        locked.workspace_origin = ORIGIN_CREATED
        locked.save(update_fields=["workspace_network_id", "workspace_origin"])
        _journal(
            locked,
            user,
            f"Created Forward workspace {network_id} for {len(names)} device(s), "
            f"kept {retention_days} day(s).",
        )
    change.refresh_from_db()
    return WorkspaceOutcome(True, network_id)


def adopt_workspace(change, network_id, user, *, api=None):
    """Use a workspace an operator already made as this change's scope.

    It must be a workspace of THIS source's network. A network with no parent is
    the estate itself, and scoping a change to it would run its checks against
    production. One workspace belongs to one change.
    """
    from ..models import ForwardChange

    network_id = (network_id or "").strip()
    if not network_id:
        return _refuse("Name the workspace network to adopt.")
    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().get(pk=change.pk)
        if locked.workspace_network_id:
            return _refuse("This change already has a workspace.")
        if not _open(locked):
            return _refuse(
                "A workspace can only be attached before the change is applied."
            )
        if (
            ForwardChange.objects.filter(workspace_network_id=network_id)
            .exclude(pk=locked.pk)
            .exists()
        ):
            return _refuse("Another change already uses that workspace.")
        api, error = _open_api(locked, api)
        if error:
            return _refuse(error)
        try:
            network = api.network(network_id)
        except Exception as exc:  # noqa: BLE001
            return _refuse(f"Forward could not be read: {exc}")
        if network is None:
            return _refuse(f"Forward has no network {network_id}.")
        if not network.get("parent_id"):
            return _refuse("That network is not a workspace; it has no parent network.")
        if str(network["parent_id"]) != str(_source_network(locked)):
            return _refuse("That workspace was not forked from this source's network.")
        locked.workspace_network_id = network_id
        locked.workspace_origin = ORIGIN_ADOPTED
        locked.save(update_fields=["workspace_network_id", "workspace_origin"])
        _journal(locked, user, f"Adopted Forward workspace {network_id}.")
    change.refresh_from_db()
    return WorkspaceOutcome(True, network_id)


def _created(snapshot):
    from datetime import datetime

    raw = str(snapshot.get("created_at") or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else timezone.make_aware(parsed)


def refresh_snapshots(change, *, api=None):
    """Record the workspace's baseline and, once applied, its after snapshot.

    The baseline is the workspace's processed import snapshot and is recorded once:
    approvals are anchored to it, so a later import must not silently replace it.
    The after snapshot is the newest processed collection made after the change was
    applied; before that there is none, because a collection of an unchanged network
    would pass every check for the wrong reason.
    """
    if not change.workspace_network_id:
        return _refuse("This change has no workspace.")
    api, error = _open_api(change, api)
    if error:
        return _refuse(error)
    try:
        snapshots = api.snapshots(change.workspace_network_id)
    except Exception as exc:  # noqa: BLE001
        return _refuse(f"Forward could not be read: {exc}")
    processed = [s for s in snapshots if str(s.get("state")).upper() == "PROCESSED"]
    updates = []
    if not change.before_snapshot_id:
        baselines = sorted(
            (
                s
                for s in processed
                if str(s.get("trigger")).upper() in BASELINE_TRIGGERS
            ),
            key=lambda s: _created(s) or timezone.now(),
        )
        if baselines:
            change.before_snapshot_id = baselines[0]["id"]
            updates.append("before_snapshot_id")
    # Only while the change can still be verified: once a verdict exists it was
    # computed on the snapshot then pinned, and moving the pin would let reviewers
    # close on a verdict that was never computed for the snapshot they see.
    if change.applied_at and change.state in (State.APPLIED, State.COLLECTED):
        after = sorted(
            (
                s
                for s in processed
                if str(s.get("trigger")).upper() == COLLECTION_TRIGGER
                and (_created(s) or change.applied_at) > change.applied_at
            ),
            key=lambda s: _created(s),
        )
        if after and change.after_snapshot_id != after[-1]["id"]:
            change.after_snapshot_id = after[-1]["id"]
            updates.append("after_snapshot_id")
    if updates:
        change.save(update_fields=updates)
    return WorkspaceOutcome(True, ", ".join(updates) or "nothing new")


# A collection logs in to the live devices through the organisation's collector.
COLLECTABLE_STATES = frozenset({State.APPLIED, State.COLLECTED})

COLLECTION_WARNING = (
    "Collecting starts a live collection of the workspace's devices. Forward "
    "logs in to each device through your collector, which takes device and "
    "collector capacity, and the collection carries on in Forward if you stop "
    "waiting here. Only collect once the change has really been applied."
)


@dataclass(frozen=True)
class CollectionAccess:
    # True: an administrator. False: certainly not allowed. None: Forward does not
    # publish which roles may collect, so it is left to Forward to decide.
    allowed: object
    reason: str = ""


def collection_access(change, *, api=None):
    """May this source's Forward account start a collection on the workspace?

    Read-only. Refuses only what is certain (no role at all); an administrator is
    allowed; any other role is left to Forward, and the page says so.
    """
    api, error = _open_api(change, api)
    if error:
        return CollectionAccess(None, error)
    try:
        roles = api.collection_access(change.workspace_network_id)
    except Exception as exc:  # noqa: BLE001
        return CollectionAccess(
            None, f"Forward would not say what this account may do ({exc})."
        )
    if roles.get("unknown"):
        return CollectionAccess(
            None, "Forward returned no role information for this account."
        )
    org = roles.get("org_roles") or []
    network = roles.get("network_roles") or {}
    on_network = [
        role
        for key, role in network.items()
        if key in (str(change.workspace_network_id), str(_source_network(change)))
    ]
    if "ADMIN" in org or "ADMIN" in on_network:
        return CollectionAccess(True, "This account is an administrator in Forward.")
    if not org and not on_network:
        return CollectionAccess(
            False, "This account has no role on the workspace in Forward."
        )
    held = ", ".join(sorted({*org, *on_network}))
    return CollectionAccess(
        None,
        f"This account's Forward roles ({held}) could not be confirmed to allow "
        "collecting; Forward will refuse it if they do not.",
    )


def collect_snapshot(change, user, *, confirmed=False, api=None):
    """Start a collection in the change's workspace, after the change is applied.

    A write to Forward that touches live devices, so it needs the operator's
    confirmation every time and an account that may collect. It does not wait: the
    snapshot is picked up by `refresh_snapshots` once Forward has made it.
    """
    from ..models import ForwardChange

    if not confirmed:
        return _refuse(
            "Confirm that you understand what a collection does before starting one."
        )
    with transaction.atomic():
        locked = ForwardChange.objects.select_for_update().get(pk=change.pk)
        if not locked.workspace_network_id:
            return _refuse("This change has no workspace.")
        if locked.state not in COLLECTABLE_STATES:
            return _refuse(
                "A snapshot is collected after the change is applied; this change "
                f"is {locked.state}."
            )
        api, error = _open_api(locked, api)
        if error:
            return _refuse(error)
        access = collection_access(locked, api=api)
        if access.allowed is False:
            return _refuse(access.reason)
        try:
            task_id = api.start_collection(locked.workspace_network_id)
        except Exception as exc:  # noqa: BLE001 - reported, never raised past the page
            return _refuse(f"Forward would not start the collection: {exc}")
        # The collection is already running in Forward here; if the journal write
        # failed the transaction would roll back and leave it unrecorded, so the
        # journal is the last thing that can fail.
        devices = locked.devices.count()
        _journal(
            locked,
            user,
            f"Started a Forward collection (task {task_id or 'unknown'}) in "
            f"workspace {locked.workspace_network_id} for {devices} device(s).",
        )
    return WorkspaceOutcome(
        True,
        f"Collection started (task {task_id or 'unknown'}). Refresh snapshots once "
        "Forward has finished it.",
    )
