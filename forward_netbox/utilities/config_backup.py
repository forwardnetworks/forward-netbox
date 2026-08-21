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

from ..exceptions import ForwardSyncError

CONFIG_BACKUP_PARAMETER_NAME = "config_backup_data_source"
CONFIG_BACKUP_QUERY_FILENAME = "forward_config_backup.nqe"
# ~100 rows at the measured average of ~560 KB keeps a page around 55 MB. The
# one measured outlier (20 MB) cannot repeat often enough per page to matter.
CONFIG_BACKUP_PAGE_SIZE = 100
CONFIG_BACKUP_REPO_PREFIX = "configs"
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


def _branch_ref(data_source):
    branch = (data_source.parameters or {}).get("branch") or "main"
    return ("refs/heads/" + branch).encode("ascii")


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


def _fetch_remote_head(repo, url, branch_ref):
    """Fetch the remote branch tip into `repo`; None for an empty remote."""
    from dulwich import porcelain

    try:
        result = porcelain.fetch(repo, url)
    except Exception as exc:
        raise ForwardSyncError(
            f"config backup could not fetch the data source repository "
            f"({type(exc).__name__})."
        ) from exc
    refs = getattr(result, "refs", None) or {}
    head = refs.get(branch_ref)
    if head is None and refs:
        head = refs.get(b"HEAD")
    return head


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
    branch_ref = _branch_ref(data_source)
    name_map = _identity_name_map(sync)
    client = sync.source.get_client()
    network_id = (sync.source.parameters or {}).get("network_id")
    query = _load_backup_query()

    with tempfile.TemporaryDirectory(prefix="fwd-config-backup-") as workdir:
        repo = Repo.init_bare(workdir)
        try:
            head = _fetch_remote_head(repo, url, branch_ref)

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

            new_blobs = []
            offset = 0
            while True:
                rows = client.run_nqe_query(
                    query=query,
                    network_id=network_id,
                    snapshot_id=snapshot_id,
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
                        result.unmapped += 1
                        continue
                    blob = Blob.from_string(str(text).encode("utf-8"))
                    entry_name = file_name.encode("utf-8")
                    existing = config_entries.get(entry_name)
                    if existing is not None and existing[1] == blob.id:
                        result.unchanged += 1
                        continue
                    new_blobs.append(blob)
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
            if result.written == 0:
                result.skipped_reason = "no configuration changed"
                return result

            for blob in new_blobs:
                repo.object_store.add_object(blob)
            config_tree = Tree()
            for entry_name in sorted(config_entries):
                mode, sha = config_entries[entry_name]
                config_tree.add(entry_name, mode, sha)
            repo.object_store.add_object(config_tree)
            root_tree = Tree()
            root_entries[prefix] = (0o040000, config_tree.id)
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
    except Exception as exc:  # noqa: BLE001 - recorded, never fatal
        result.warnings.append(
            f"data source sync did not complete ({type(exc).__name__}); "
            "NetBox will pick the commit up on its own schedule."
        )

    result.duration_seconds = time.monotonic() - started
    if logger is not None:
        logger.log_info(
            f"Config backup: {result.written} written, {result.unchanged} "
            f"unchanged, {result.unmapped} unmapped across {result.rows} "
            f"Forward rows ({result.pages} pages)."
        )
    return result
