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

import re
import time
from dataclasses import dataclass
from dataclasses import field
from urllib.parse import urlsplit

from rq.timeouts import JobTimeoutException

from ..exceptions import ForwardSyncError
from .config_backup_path import config_backup_path_prefix
from .config_backup_path import path_segments


CONFIG_BACKUP_STAGES = (
    "resolve",
    "fetch_remote",
    "branch",
    "nqe_fetch",
    "build",
    "push",
    "datasource_sync",
)


class ConfigBackupError(ForwardSyncError):
    """A config-backup failure whose OWN message is already operator-safe.

    Every raise site in this module hand-writes a static, actionable
    sentence, or interpolates only `_remote_failure_reason(exc)` - which is
    built specifically to be safe (never a URL, never credentials). The job
    wrapper (`_run_forward_config_backup_work`) recognizes this subclass and
    preserves `str(exc)` verbatim instead of collapsing it to a bare
    exception-name classifier via `safe_operation_failure` - the classifier
    exists for exceptions whose text is NOT known to be safe, and collapsing
    an already-safe, already-actionable message down to "ForwardSyncError"
    was itself the reason a customer's config-backup failure needed a
    diagnostic script to explain at all.

    `stage` names WHERE in the pipeline this failed - one of
    `CONFIG_BACKUP_STAGES` - and is prepended to the message as `[stage]`.
    Every real job failure this module has produced so far completed in
    under a second, which only rules out the stages that read from Forward
    (`nqe_fetch` can legitimately take minutes on a large fleet); the
    message alone could not say which of the fast ones it was without a
    diagnostic script reproducing each step by hand.
    """

    def __init__(self, message, *, stage=None, category=None):
        self.stage = stage
        # A closed, value-free token (`CONFIG_BACKUP_FAILURE_CATEGORIES`) for
        # the job data and the support bundle, which redact free text.
        self.category = category
        super().__init__(f"[{stage}] {message}" if stage else message)


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
    # Devices the fetch was scoped to (the sync's identities) and how many of
    # them Forward returned NO configuration for - the number that answers
    # "why are there fewer files than devices".
    scoped_devices: int = 0
    scoped_without_config: int = 0
    # Files in the backup folder of the repository head after this run.
    files_in_folder: int = 0
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
            "scoped_devices": self.scoped_devices,
            "scoped_without_config": self.scoped_without_config,
            "files_in_folder": self.files_in_folder,
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
        raise ConfigBackupError(
            "config_backup_data_source does not name an existing data source.",
            stage="resolve",
        ) from exc
    if data_source.type != "git":
        raise ConfigBackupError(
            "config_backup_data_source must reference a git data source.",
            stage="resolve",
        )
    return data_source


def _load_backup_query():
    from .query_registry import QUERY_DIR

    return (QUERY_DIR / CONFIG_BACKUP_QUERY_FILENAME).read_text(encoding="utf-8")


@dataclass
class _RemoteConnection:
    """How to reach the data source's repository, the way NetBox itself does.

    The url is the data source's own, never rewritten: credentials travel as
    dulwich ``username``/``password`` arguments, not embedded in the url. An
    embedded url used to reach dulwich's success line on the worker's stderr
    (`porcelain.push` writes the remote location there) with the password in
    it. The proxy is NetBox's: `DataSource.get_backend()` resolves
    `HTTP_PROXIES`/`PROXY_ROUTERS` for this url exactly as NetBox's own git
    sync does - config backup used to ignore it, which is why a data source
    that synced fine could not be fetched by this module.
    """

    url: str
    scheme: str
    username: str | None = None
    password: str | None = None
    proxy: str | None = None
    socks_proxy: str | None = None

    def transport_kwargs(self):
        kwargs = {}
        if self.username:
            kwargs["username"] = self.username
            if self.password:
                kwargs["password"] = self.password
        if self.socks_proxy:
            from utilities.socks import ProxyPoolManager

            kwargs["pool_manager"] = ProxyPoolManager(self.socks_proxy)
        return kwargs


def _url_scheme(url):
    return (urlsplit(str(url or "")).scheme or "").lower()


def _url_has_credentials(url):
    return "@" in (urlsplit(str(url or "")).netloc or "")


def _remote_connection(data_source):
    """The `_RemoteConnection` for a git data source."""
    from django.core.exceptions import ImproperlyConfigured

    url = data_source.source_url
    scheme = _url_scheme(url)
    parameters = data_source.parameters or {}
    username = password = None
    # Like NetBox's GitBackend: credentials only for HTTP(S), and never on top
    # of credentials the url already carries.
    if scheme in ("http", "https") and not _url_has_credentials(url):
        username = str(parameters.get("username") or "") or None
        password = str(parameters.get("password") or "") or None
    proxy = socks_proxy = None
    try:
        backend = data_source.get_backend()
    except JobTimeoutException:
        raise
    except ImproperlyConfigured as exc:
        raise ConfigBackupError(
            "config backup cannot use NetBox's proxy settings for this data "
            "source: the proxy scheme is not one NetBox's git backend supports.",
            stage="resolve",
            category="proxy_config",
        ) from exc
    config = getattr(backend, "config", None)
    if config is not None:
        try:
            value = config.get((b"http",), b"proxy")
        except KeyError:
            value = None
        if value:
            proxy = value.decode() if isinstance(value, bytes) else str(value)
    socks_proxy = getattr(backend, "socks_proxy", None) or None
    return _RemoteConnection(
        url=url,
        scheme=scheme,
        username=username,
        password=password,
        proxy=proxy,
        socks_proxy=socks_proxy,
    )


def _apply_proxy_to_repo(repo, connection):
    """Write NetBox's proxy into the temporary repo's config.

    `porcelain.push` reads transport settings from the repository's config
    stack and accepts no config argument; the fetch is handed the same stack,
    so both directions go through the one proxy NetBox would use.
    """
    if not connection.proxy:
        return
    config = repo.get_config()
    config.set((b"http",), b"proxy", connection.proxy.encode())
    config.write_to_path()


def _branch_ref(data_source, remote_refs=None):
    """The ref to write, honouring the remote's own default when unset.

    An explicit ``branch`` parameter on the data source wins. Without one this
    used to assume ``main``, which is a guess, and it is wrong in the case that
    matters most: a freshly initialised repository whose ``HEAD`` still points
    at ``master``. The push then succeeds against a branch nobody reads, NetBox
    clones ``HEAD``, finds nothing, and the data source syncs ZERO files - a
    backup that reports success and delivers nothing.

    So when the operator has not named a branch, follow the remote's ``HEAD``
    and write where the remote itself says its default is. When the remote
    offers no opinion at all - an empty repository over smart HTTP advertises
    nothing - return None and let the caller refuse: guessing `main` against a
    `master` default is the silent no-delivery this function exists to stop.
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
    # Nothing advertised at all. Over smart HTTP an empty repository sends no
    # refs and no HEAD symref, and this used to invent `main` - a server whose
    # default is `master` then received a branch NetBox never reads, a backup
    # that reports success and delivers nothing. Refuse with the remedy.
    return None


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


# The categories a git failure is reduced to, with the sentence an operator
# reads. Phrases deliberately contain the needles `diagnostics.failure_reason`
# already turns into slugs (HTTP NNN, connection refused, certificate verify
# failed, timed out, name or service not known), so the job's own error
# summary carries them too. Never the url, a header, or a response body:
# dulwich's messages carry the host and path, and a non-git response body is
# customer content.
CONFIG_BACKUP_FAILURE_CATEGORIES = {
    "auth_401": "the data source credentials were refused, HTTP 401",
    "proxy_auth_407": "the proxy refused the credentials, HTTP 407",
    "not_found_404": "the remote answered HTTP 404: no git repository at that url",
    "tls": "the TLS connection failed (certificate verify failed or handshake)",
    "proxy_connect": "could not connect through the proxy",
    "dns": "the repository host name did not resolve (name or service not known)",
    "connection_refused": "connection refused or host unreachable",
    "timeout": "the connection timed out",
    "non_git_response": (
        "the server answered with something that is not a git repository - "
        "usually a proxy or single-sign-on page"
    ),
    "redirect": ("the remote redirected without serving git - usually to a login page"),
    "push_rejected": "the remote refused the branch update",
    "proxy_config": "NetBox's proxy setting for this url is not usable",
}
_HTTP_STATUS_RE = re.compile(r"unexpected http resp (\d{3})\b")


def _failure_chain(exc):
    """`exc`, its causes, and urllib3's retry reasons, outermost first."""
    chain, seen, pending = [], set(), [exc]
    while pending:
        current = pending.pop(0)
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        chain.append(current)
        pending.extend(
            (getattr(current, "reason", None), current.__cause__, current.__context__)
        )
    return chain


def _classify_remote_failure(exc):
    """``(category, operator sentence)`` for a failed git exchange."""
    chain = _failure_chain(exc)
    names = [type(item).__name__ for item in chain]
    texts = [str(item) for item in chain]

    def named(*candidates):
        return any(name in candidates for name in names)

    if named("HTTPUnauthorized"):
        category = "auth_401"
    elif named("HTTPProxyUnauthorized"):
        category = "proxy_auth_407"
    elif named("NotGitRepository"):
        category = "not_found_404"
    else:
        status = next(
            (m.group(1) for m in map(_HTTP_STATUS_RE.search, texts) if m), None
        )
        if status is not None:
            return f"http_{status}", f"the remote answered HTTP {status}"
        if named("SSLError", "SSLCertVerificationError", "CertificateError"):
            category = "tls"
        elif named("ProxyError"):
            category = "proxy_connect"
        elif named("NameResolutionError", "gaierror"):
            category = "dns"
        elif named("NewConnectionError", "ConnectionRefusedError"):
            category = "connection_refused"
        elif named("ConnectTimeoutError", "ReadTimeoutError", "TimeoutError"):
            category = "timeout"
        elif any("info/refs format" in text for text in texts) or any(
            "Invalid content-type from server" in text for text in texts
        ):
            category = "non_git_response"
        elif any("without info/refs" in text for text in texts):
            category = "redirect"
        else:
            return f"other:{type(exc).__name__}", type(exc).__name__
    return category, CONFIG_BACKUP_FAILURE_CATEGORIES[category]


def _remote_failure_reason(exc):
    """The operator sentence for a failed git exchange (see the classifier)."""
    return _classify_remote_failure(exc)[1]


def _remote_failure(message_prefix, exc, *, stage, connection=None):
    category, reason = _classify_remote_failure(exc)
    message = f"{message_prefix} ({reason})."
    if (
        connection is not None
        and connection.proxy
        and category
        in (
            "non_git_response",
            "redirect",
            "proxy_connect",
            "http_403",
        )
    ):
        message += " NetBox applies a proxy to this url; check the proxy allows it."
    return ConfigBackupError(message, stage=stage, category=category)


def _fetch_remote(repo, connection):
    """Fetch the remote into `repo` and return what it advertised.

    Through the transport directly, not `porcelain.fetch`, which accepts
    neither a config nor a pool manager - the only way to hand it NetBox's
    proxy, HTTP or SOCKS.
    """
    from dulwich.client import get_transport_and_path

    try:
        client, path = get_transport_and_path(
            connection.url,
            config=repo.get_config_stack(),
            **connection.transport_kwargs(),
        )
        return client.fetch(path, repo)
    except JobTimeoutException:
        raise
    except Exception as exc:
        raise _remote_failure(
            "config backup could not fetch the data source repository",
            exc,
            stage="fetch_remote",
            connection=connection,
        ) from exc


def _remote_head(remote_refs, branch_ref):
    refs = getattr(remote_refs, "refs", None) or {}
    return refs.get(branch_ref)


def _tree_entries(repo, tree_sha):
    tree = repo.object_store[tree_sha]
    return {name: (mode, sha) for name, mode, sha in tree.iteritems()}


_TREE_MODE = 0o040000


def _entries_at(repo, entries, segments, folder):
    """The file entries of the folder at ``segments``, or none if it is new.

    Walks down from the root entries one segment at a time. A segment that is a
    FILE in the repository is a conflict, not a folder to replace: writing a
    tree over it would delete a file nobody asked us to touch.
    """
    for segment in segments:
        entry = entries.get(segment)
        if entry is None:
            return {}
        if entry[0] != _TREE_MODE:
            raise ConfigBackupError(
                f"a file named `{segment.decode('utf-8', 'replace')}` is where "
                f"the config backup folder `{folder}` needs a folder. Move or "
                "rename it in the repository, or choose another folder in the "
                "source's settings.",
                stage="build",
            )
        entries = _tree_entries(repo, entry[1])
    return entries


def _replace_at(repo, entries, segments, leaf_entries, Tree):
    """Root entries with the folder at ``segments`` replaced by ``leaf_entries``.

    Every folder on the way down is rebuilt, and every sibling of the path is
    carried over untouched, so a nested folder changes only its own files.
    """
    entries = dict(entries)
    head, rest = segments[0], segments[1:]
    if rest:
        child = entries.get(head)
        child_entries = (
            _tree_entries(repo, child[1])
            if child is not None and child[0] == _TREE_MODE
            else {}
        )
        folder_entries = _replace_at(repo, child_entries, rest, leaf_entries, Tree)
    else:
        folder_entries = leaf_entries
    tree = Tree()
    for name in sorted(folder_entries):
        mode, sha = folder_entries[name]
        tree.add(name, mode, sha)
    repo.object_store.add_object(tree)
    entries[head] = (_TREE_MODE, tree.id)
    return entries


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
    try:
        path_prefix = config_backup_path_prefix(sync.source.parameters)
    except ValueError as exc:
        raise ConfigBackupError(
            f"the source's config backup folder is not usable: {exc} Correct it "
            "in the source's settings.",
            stage="resolve",
        ) from exc
    folder_segments = path_segments(path_prefix)

    connection = _remote_connection(data_source)
    name_map = _identity_name_map(sync)
    if not name_map:
        # No identities means this sync manages no devices yet. Fetching would
        # return the whole estate for an unscoped shard-key list and write
        # none of it.
        result.skipped_reason = "no device identities for this sync"
        return result
    result.scoped_devices = len(name_map)
    # Forward names for which a configuration came back, among the devices this
    # sync manages. The difference is the devices Forward has no collected
    # configuration for.
    configured_names = set()
    client = sync.source.get_client()
    network_id = (sync.source.parameters or {}).get("network_id")
    query = _load_backup_query()

    with tempfile.TemporaryDirectory(prefix="fwd-config-backup-") as workdir:
        repo = Repo.init_bare(workdir)
        try:
            _apply_proxy_to_repo(repo, connection)
            remote_refs = _fetch_remote(repo, connection)
            branch_ref = _branch_ref(data_source, remote_refs)
            if branch_ref is None:
                raise ConfigBackupError(
                    "config backup cannot choose a branch: the data source "
                    "repository is empty and advertises no default branch. Set "
                    "the data source's `branch` parameter, or make an initial "
                    "commit on the branch NetBox should read.",
                    stage="branch",
                )
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
            config_entries = _entries_at(
                repo, root_entries, folder_segments, path_prefix
            )
            default_folder = CONFIG_BACKUP_REPO_PREFIX.encode("ascii")
            if (
                path_prefix != CONFIG_BACKUP_REPO_PREFIX
                and root_entries.get(default_folder, (None,))[0] == _TREE_MODE
            ):
                # Changing the folder leaves the old files where they were.
                # Validity may still be reading them, and they are no longer
                # updated, so say so instead of leaving stale configs unmarked.
                result.warnings.append(
                    f"the repository still holds a `{CONFIG_BACKUP_REPO_PREFIX}/` "
                    f"folder from before the config backup folder became "
                    f"`{path_prefix}/`; those files are no longer updated. "
                    "Remove them, or point Validity at the new folder."
                )
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
                    configured_names.add(forward_name)
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

            result.scoped_without_config = max(
                0, result.scoped_devices - len(configured_names)
            )
            result.files_in_folder = len(config_entries)
            if result.rows == 0:
                # An empty result cannot be told from a failed fetch, and a
                # backup that commits emptiness on a fault destroys nothing but
                # proves nothing either. Refuse loudly.
                raise ConfigBackupError(
                    "config backup fetched no configurations; refusing to "
                    "commit an empty snapshot.",
                    stage="nqe_fetch",
                )
            if result.written == 0 and result.unmanaged_written == 0:
                result.skipped_reason = "no configuration changed"
                return result

            root_tree = Tree()
            root_entries = _replace_at(
                repo, root_entries, folder_segments, config_entries, Tree
            )
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
                push_result = porcelain.push(
                    repo,
                    connection.url,
                    [branch_ref + b":" + branch_ref],
                    # dulwich writes the remote location to stderr by default.
                    errstream=porcelain.NoneStream(),
                    **connection.transport_kwargs(),
                )
            except JobTimeoutException:
                raise
            except Exception as exc:
                raise _remote_failure(
                    "config backup could not push to the data source repository",
                    exc,
                    stage="push",
                    connection=connection,
                ) from exc
            # A per-ref refusal (a protected branch, a non-fast-forward) is not
            # raised; it is only reported here. It used to count as pushed.
            ref_status = getattr(push_result, "ref_status", None)
            if ref_status is not None and ref_status.get(branch_ref):
                raise ConfigBackupError(
                    "config backup could not push to the data source repository "
                    f"({CONFIG_BACKUP_FAILURE_CATEGORIES['push_rejected']}; is the "
                    "branch protected?).",
                    stage="push",
                    category="push_rejected",
                )
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
