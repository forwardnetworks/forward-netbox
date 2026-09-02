"""Back up device configurations from Forward into a git data source.

Forward already holds every device's running configuration, collected per
snapshot, for the exact device set this plugin syncs. This module moves those
configurations into the git repository behind a NetBox ``core.DataSource`` so
tools that read config backups from a data source (Validity's golden-config
checks being the motivating one) get snapshot-consistent configs with no device
credentials and no polling.

Design constraints this module carries (see the plan for the reasoning):

- The repository connection is the DATA SOURCE'S. Url, username, password and
  branch are read from the operator's existing git data source - the object
  the consumer reads and NetBox already encrypts - so this plugin stores a
  pointer, never a secret.
- All git work is object-level dulwich. The runtime has no git binary, and a
  working-tree checkout of a multi-gigabyte config repo per run is waste: the
  remote head is fetched into a temporary bare repository, tree entries are
  rewritten for changed devices only, and one commit is pushed.
- The NQE fetch is manually paged with a SMALL page size and each page is
  folded into the tree and discarded. The client's ceilings count rows, not
  bytes, and a config row averages half a megabyte - ``fetch_all`` on this
  query is a worker-memory incident.
- Rows are keyed by the FORWARD device name; the file is written under the
  NETBOX device name resolved through ``ForwardDeviceIdentity``. That mapping
  is the authoritative one and already absorbs aliasing, which is why the
  query needs no alias variant.
- Configuration text never reaches logs, ingestion issues, or support
  bundles. Results are counts and durations only.

Files for devices that later leave Forward are deliberately left in place:
the repository is the operator's config history, and pruning it is their
decision, not a side effect of scope.
"""

import time
from dataclasses import dataclass
from dataclasses import field
from urllib.parse import quote
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardSyncError

CONFIG_BACKUP_PARAMETER_NAME = "config_backup_data_source"
CONFIG_BACKUP_QUERY_FILENAME = "forward_config_backup.nqe"
# ~100 rows at the measured average of ~560 KB keeps a page around 55 MB. The
# one measured outlier (20 MB) cannot repeat often enough per page to matter.
CONFIG_BACKUP_PAGE_SIZE = 100
CONFIG_BACKUP_REPO_PREFIX = "configs"
# Where configurations for devices this sync does NOT manage go, when the
# operator opts in. Kept apart from `configs/` on purpose: Validity's golden-
# config checks read `configs/<netbox device name>.cfg`, and an unmanaged
# device has no NetBox row for such a check to bind to. These files are named
# by their Forward name, because that is the only name they have.
UNMANAGED_BACKUP_REPO_PREFIX = "unmanaged"
_COMMIT_AUTHOR = b"Forward NetBox Plugin <forward-netbox-plugin@localhost>"
_COMMIT_MESSAGE_PREFIX = "Forward config backup: snapshot "


@dataclass
class ConfigBackupResult:
    snapshot_id: str = ""
    pages: int = 0
    rows: int = 0
    written: int = 0
    unchanged: int = 0
    unmapped: int = 0
    unmanaged_written: int = 0
    unmanaged_unchanged: int = 0
    skipped_reason: str = ""
    commit: str = ""
    pushed: bool = False
    data_source_synced: bool = False
    duration_seconds: float = 0.0
    warnings: list = field(default_factory=list)

    def as_dict(self):
        return {
            "snapshot_id": self.snapshot_id,
            "pages": self.pages,
            "rows": self.rows,
            "written": self.written,
            "unchanged": self.unchanged,
            "unmapped": self.unmapped,
            "unmanaged_written": self.unmanaged_written,
            "unmanaged_unchanged": self.unmanaged_unchanged,
            "skipped_reason": self.skipped_reason,
            "commit": self.commit,
            "pushed": self.pushed,
            "data_source_synced": self.data_source_synced,
            "duration_seconds": round(self.duration_seconds, 1),
            "warnings": list(self.warnings),
        }


def config_backup_data_source(sync):
    """The configured git data source, or None when the feature is off.

    Fails loudly on a configured-but-wrong value: a silently skipped backup is
    how an operator discovers at audit time that six months of configs are
    missing.
    """
    raw = (getattr(sync.source, "parameters", None) or {}).get(
        CONFIG_BACKUP_PARAMETER_NAME
    )
    if raw in (None, "", 0):
        return None
    from core.models import DataSource

    try:
        data_source = DataSource.objects.get(pk=int(raw))
    except (TypeError, ValueError, DataSource.DoesNotExist) as exc:
        raise ForwardSyncError(
            "config_backup_data_source does not name an existing data source."
        ) from exc
    if data_source.type != "git":
        raise ForwardSyncError(
            "config_backup_data_source must reference a git data source."
        )
    return data_source


def _load_backup_query():
    from .query_registry import QUERY_DIR

    return (QUERY_DIR / CONFIG_BACKUP_QUERY_FILENAME).read_text(encoding="utf-8")


def _authenticated_url(data_source):
    """The data source's url with its HTTP(S) credentials embedded.

    dulwich's porcelain accepts credentials most portably in the url. The
    value exists only inside this process for the duration of the push and is
    never logged; the assembled url must not be placed on any result or
    message.
    """
    url = data_source.source_url
    parameters = data_source.parameters or {}
    username = parameters.get("username") or ""
    password = parameters.get("password") or ""
    if not username and not password:
        return url
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        # ssh remotes authenticate with keys; embedding is neither needed nor
        # meaningful.
        return url
    credentials = quote(str(username), safe="")
    if password:
        credentials += ":" + quote(str(password), safe="")
    host = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit(
        (parts.scheme, f"{credentials}@{host}", parts.path, parts.query, "")
    )


def _branch_ref(data_source, remote_refs=None):
    """The ref to write, honouring the remote's own default when unset.

    An explicit ``branch`` parameter on the data source wins. Without one this
    used to assume ``main``, which is a guess, and it is wrong in the case that
    matters most: a freshly initialised repository whose ``HEAD`` still points
    at ``master``. The push then succeeds against a branch nobody reads, NetBox
    clones ``HEAD``, finds nothing, and the data source syncs ZERO files - a
    backup that reports success and delivers nothing.

    So when the operator has not named a branch, follow the remote's ``HEAD``
    and write where the remote itself says its default is. Only when the remote
    offers no opinion at all - a genuinely empty repository - is a default
    invented, and then it matches the initial branch git and dulwich create.
    """
    branch = (data_source.parameters or {}).get("branch")
    if branch:
        return ("refs/heads/" + str(branch)).encode("ascii")
    symrefs = getattr(remote_refs, "symrefs", None) or {}
    target = symrefs.get(b"HEAD")
    if target:
        return target
    refs = getattr(remote_refs, "refs", None) or {}
    head = refs.get(b"HEAD")
    if head:
        for name, value in refs.items():
            if name.startswith(b"refs/heads/") and value == head:
                return name
    return b"refs/heads/main"


def _identity_name_map(sync):
    """Forward device name -> NetBox device name, from the identity table."""
    from ..models import ForwardDeviceIdentity

    return {
        source_key: device_name
        for source_key, device_name in ForwardDeviceIdentity.objects.filter(
            sync=sync
        ).values_list("source_device_key", "device__name")
        if device_name
    }


def _safe_file_name(device_name):
    """A single path segment for the device's file, or None if unusable.

    Device names are operator data; a separator or a traversal token in one
    must never become repository structure.
    """
    name = str(device_name or "").strip()
    if not name or name in (".", "..") or "/" in name or "\\" in name or "\x00" in name:
        return None
    return name + ".cfg"


def _fetch_remote(repo, url):
    """Fetch the remote into `repo` and return what it advertised."""
    from dulwich import porcelain

    try:
        return porcelain.fetch(repo, url)
    except JobTimeoutException:
        raise
    except Exception as exc:
        raise ForwardSyncError(
            f"config backup could not fetch the data source repository "
            f"({type(exc).__name__})."
        ) from exc


def _remote_head(remote_refs, branch_ref):
    refs = getattr(remote_refs, "refs", None) or {}
    return refs.get(branch_ref)


def _tree_entries(repo, tree_sha):
    tree = repo.object_store[tree_sha]
    return {name: (mode, sha) for name, mode, sha in tree.iteritems()}


def run_config_backup(sync, *, snapshot_id, logger=None):
    """Fetch configs for this snapshot and push one commit of the changes."""
    import tempfile

    from dulwich import porcelain
    from dulwich.objects import Blob
    from dulwich.objects import Commit
    from dulwich.objects import Tree
    from dulwich.repo import Repo

    started = time.monotonic()
    result = ConfigBackupResult(snapshot_id=str(snapshot_id or ""))

    data_source = config_backup_data_source(sync)
    if data_source is None:
        result.skipped_reason = "not configured"
        return result
    if not snapshot_id:
        result.skipped_reason = "no snapshot id"
        return result

    url = _authenticated_url(data_source)
    name_map = _identity_name_map(sync)
    if not name_map:
        # No identities means this sync manages no devices yet. Fetching would
        # return the whole estate for an unscoped shard-key list and write
        # none of it.
        result.skipped_reason = "no device identities for this sync"
        return result
    client = sync.source.get_client()
    network_id = (sync.source.parameters or {}).get("network_id")
    query = _load_backup_query()

    with tempfile.TemporaryDirectory(prefix="fwd-config-backup-") as workdir:
        repo = Repo.init_bare(workdir)
        try:
            remote_refs = _fetch_remote(repo, url)
            branch_ref = _branch_ref(data_source, remote_refs)
            head = _remote_head(remote_refs, branch_ref)

            # Fast path: the head commit says it already holds this snapshot.
            # Even without it the run is write-free - every blob would match -
            # but this saves the whole fetch.
            if head is not None:
                head_commit = repo.object_store[head]
                if head_commit.message.decode(
                    "utf-8", "replace"
                ).strip() == _COMMIT_MESSAGE_PREFIX + str(snapshot_id):
                    result.skipped_reason = "snapshot already backed up"
                    return result

            if head is not None:
                root_entries = _tree_entries(repo, head_commit.tree)
            else:
                root_entries = {}
            prefix = CONFIG_BACKUP_REPO_PREFIX.encode("ascii")
            if prefix in root_entries:
                config_entries = _tree_entries(repo, root_entries[prefix][1])
            else:
                config_entries = {}
            unmanaged_prefix = UNMANAGED_BACKUP_REPO_PREFIX.encode("ascii")
            if unmanaged_prefix in root_entries:
                unmanaged_entries = _tree_entries(
                    repo, root_entries[unmanaged_prefix][1]
                )
            else:
                unmanaged_entries = {}
            # The product question the 2.9.0 plan left open - whether devices
            # Forward collected but this sync does not manage should be
            # archived too - is answered as an opt-in. Off, the fetch stays
            # scoped to this sync's devices and the surplus is never
            # transferred. On, the fetch is the whole collected estate and the
            # surplus lands under its own prefix, apart from the files
            # Validity binds to NetBox devices.
            include_unmanaged = bool(
                (sync.source.parameters or {}).get("config_backup_include_unmanaged")
            )

            offset = 0
            # Scope the FETCH, not just the write. The identity table is this
            # sync's device scope - the same tag scope every other query is
            # narrowed to - so passing its keys as shard keys means Forward
            # returns configurations only for devices that have somewhere to
            # go. Unscoped, the query returns the whole collected estate and
            # the surplus is transferred only to be discarded.
            shard_keys = [] if include_unmanaged else sorted(name_map)
            while True:
                rows = client.run_nqe_query(
                    query=query,
                    network_id=network_id,
                    snapshot_id=snapshot_id,
                    parameters={"forward_netbox_shard_keys": shard_keys},
                    limit=CONFIG_BACKUP_PAGE_SIZE,
                    offset=offset,
                )
                result.pages += 1
                result.rows += len(rows)
                for row in rows:
                    forward_name = str(row.get("name") or "")
                    text = row.get("config")
                    netbox_name = name_map.get(forward_name)
                    file_name = _safe_file_name(netbox_name)
                    if not netbox_name or not file_name or text is None:
                        if (
                            include_unmanaged
                            and text is not None
                            and not netbox_name
                            and _safe_file_name(forward_name)
                        ):
                            unmanaged_blob = Blob.from_string(str(text).encode("utf-8"))
                            unmanaged_name = _safe_file_name(forward_name).encode(
                                "utf-8"
                            )
                            existing = unmanaged_entries.get(unmanaged_name)
                            if (
                                existing is not None
                                and existing[1] == unmanaged_blob.id
                            ):
                                result.unmanaged_unchanged += 1
                                continue
                            repo.object_store.add_object(unmanaged_blob)
                            unmanaged_entries[unmanaged_name] = (
                                0o100644,
                                unmanaged_blob.id,
                            )
                            result.unmanaged_written += 1
                            continue
                        result.unmapped += 1
                        continue
                    blob = Blob.from_string(str(text).encode("utf-8"))
                    entry_name = file_name.encode("utf-8")
                    existing = config_entries.get(entry_name)
                    if existing is not None and existing[1] == blob.id:
                        result.unchanged += 1
                        continue
                    # Written immediately rather than accumulated. A held list
                    # of every changed Blob for the run is fine at fixture
                    # scale and is not fine at fleet scale: measured on 3,400
                    # synthetic devices (~1.9 GB of configs), holding them all
                    # until the fetch loop finished cost 4.2 GB of peak RSS -
                    # roughly 2.2x the payload, because each Blob object and
                    # its zlib buffer live alongside the raw text. The comment
                    # that sized the NQE page at 100 rows reasoned about fetch
                    # memory; it did not reason about this accumulation, which
                    # dominates peak memory on any run that touches most of
                    # the fleet - a first backup being exactly that case.
                    repo.object_store.add_object(blob)
                    config_entries[entry_name] = (0o100644, blob.id)
                    result.written += 1
                if len(rows) < CONFIG_BACKUP_PAGE_SIZE:
                    break
                offset += CONFIG_BACKUP_PAGE_SIZE

            if result.rows == 0:
                # An empty result cannot be told from a failed fetch, and a
                # backup that commits emptiness on a fault destroys nothing but
                # proves nothing either. Refuse loudly.
                raise ForwardSyncError(
                    "config backup fetched no configurations; refusing to "
                    "commit an empty snapshot."
                )
            if result.written == 0 and result.unmanaged_written == 0:
                result.skipped_reason = "no configuration changed"
                return result

            config_tree = Tree()
            for entry_name in sorted(config_entries):
                mode, sha = config_entries[entry_name]
                config_tree.add(entry_name, mode, sha)
            repo.object_store.add_object(config_tree)
            root_tree = Tree()
            root_entries[prefix] = (0o040000, config_tree.id)
            if unmanaged_entries:
                unmanaged_tree = Tree()
                for entry_name in sorted(unmanaged_entries):
                    mode, sha = unmanaged_entries[entry_name]
                    unmanaged_tree.add(entry_name, mode, sha)
                repo.object_store.add_object(unmanaged_tree)
                root_entries[unmanaged_prefix] = (0o040000, unmanaged_tree.id)
            for entry_name in sorted(root_entries):
                mode, sha = root_entries[entry_name]
                root_tree.add(entry_name, mode, sha)
            repo.object_store.add_object(root_tree)

            commit = Commit()
            commit.tree = root_tree.id
            commit.parents = [head] if head is not None else []
            commit.author = commit.committer = _COMMIT_AUTHOR
            commit.author_time = commit.commit_time = int(time.time())
            commit.author_timezone = commit.commit_timezone = 0
            commit.encoding = b"UTF-8"
            commit.message = (_COMMIT_MESSAGE_PREFIX + str(snapshot_id)).encode("utf-8")
            repo.object_store.add_object(commit)
            repo.refs[branch_ref] = commit.id
            result.commit = commit.id.decode("ascii")

            try:
                porcelain.push(repo, url, [branch_ref + b":" + branch_ref])
            except JobTimeoutException:
                raise
            except Exception as exc:
                raise ForwardSyncError(
                    f"config backup could not push to the data source "
                    f"repository ({type(exc).__name__})."
                ) from exc
            result.pushed = True
        finally:
            repo.close()

    # The push succeeded; everything after is best-effort convenience and must
    # not fail the backup.
    try:
        data_source.refresh_from_db()
        data_source.sync()
        result.data_source_synced = True
    except JobTimeoutException:
        raise
    except Exception as exc:  # noqa: BLE001 - recorded, never fatal
        result.warnings.append(
            f"data source sync did not complete ({type(exc).__name__}); "
            "NetBox will pick the commit up on its own schedule."
        )

    result.duration_seconds = time.monotonic() - started
    if logger is not None:
        unmanaged_note = (
            f", {result.unmanaged_written} unmanaged written, "
            f"{result.unmanaged_unchanged} unmanaged unchanged"
            if result.unmanaged_written or result.unmanaged_unchanged
            else ""
        )
        logger.log_info(
            f"Config backup: {result.written} written, {result.unchanged} "
            f"unchanged, {result.unmapped} unmapped{unmanaged_note} across "
            f"{result.rows} Forward rows ({result.pages} pages)."
        )
    return result
