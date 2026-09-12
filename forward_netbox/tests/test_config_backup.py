"""Config backup: Forward's collected configs into a git data source.

The git half runs end-to-end against a LOCAL bare repository - dulwich speaks
filesystem remotes - so what is asserted is the actual object graph a remote
would receive: tree layout, blob content, commit parentage, and the absence of
a commit when nothing changed. The Forward half is a fake client paging
canned rows, which also pins that the fetch is paged rather than fetch_all.

The negative space matters most here:

  - identical content produces NO second commit (the repo's history is the
    config-change history, so a no-change run must leave no mark);
  - an empty fetch REFUSES rather than committing emptiness (an empty result
    cannot be told from a failed fetch);
  - a device name carrying path separators never becomes repository
    structure;
  - configuration text never appears in the result payload that lands on the
    job.
"""

import tempfile
from types import SimpleNamespace

from core.models import DataSource
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase
from django.test import TestCase
from extras.models import CustomField

from forward_netbox.exceptions import ForwardSyncError
from forward_netbox.utilities.config_backup import _authenticated_url
from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.config_backup import CONFIG_BACKUP_PAGE_SIZE
from forward_netbox.utilities.config_backup import run_config_backup


class _FakeClient:
    """Pages canned rows the way the real client does for fetch_all=False."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def run_nqe_query(
        self, *, query, network_id, snapshot_id, parameters, limit, offset
    ):
        self.calls.append({"limit": limit, "offset": offset, "parameters": parameters})
        return self.rows[offset : offset + limit]


def _read_config_blob(repo_path, file_name):
    from dulwich.repo import Repo

    with Repo(repo_path) as repo:
        head = repo.refs[repo.refs.follow(b"HEAD")[0][1]]
        commit = repo.object_store[head]
        root = repo.object_store[commit.tree]
        _mode, configs_sha = dict(
            (name, (mode, sha)) for name, mode, sha in root.iteritems()
        )[b"configs"]
        configs = repo.object_store[configs_sha]
        entries = {name: sha for name, _mode, sha in configs.iteritems()}
        if file_name.encode() not in entries:
            return None, sorted(entries)
        blob = repo.object_store[entries[file_name.encode()]]
        return blob.data.decode(), sorted(entries)


def _head_and_message(repo_path):
    from dulwich.repo import Repo

    with Repo(repo_path) as repo:
        head = repo.refs[repo.refs.follow(b"HEAD")[0][1]]
        commit = repo.object_store[head]
        return head, commit.message.decode(), list(commit.parents)


class ConfigBackupTest(TestCase):
    def setUp(self):
        from dulwich.repo import Repo

        self.tmp = tempfile.TemporaryDirectory(prefix="cfg-backup-remote-")
        self.addCleanup(self.tmp.cleanup)
        Repo.init_bare(self.tmp.name).close()

        self.data_source = DataSource.objects.create(
            name="config-backups",
            type="git",
            source_url=self.tmp.name,
        )
        user = get_user_model().objects.create_user(username="config-backup-owner")
        source = ForwardSource.objects.create(
            name="config-backup-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-1",
                "config_backup_data_source": self.data_source.pk,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="config-backup-sync", source=source, user=user
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1"
        )

        site = Site.objects.create(name="CB Site", slug="cb-site")
        mfr = Manufacturer.objects.create(name="CB Mfr", slug="cb-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="CB DT", slug="cb-dt")
        role = DeviceRole.objects.create(name="CB Role", slug="cb-role")
        for forward_name, netbox_name in (
            ("fwd-router-1", "router-1"),
            ("fwd-router-2", "router-2"),
        ):
            device = Device.objects.create(
                name=netbox_name, site=site, device_type=dtype, role=role
            )
            ForwardDeviceIdentity.objects.create(
                sync=self.sync,
                ingestion=ingestion,
                source_device_key=forward_name,
                device=device,
            )

    def _run(self, rows, snapshot_id="snap-1"):
        from unittest.mock import patch

        client = _FakeClient(rows)
        with patch.object(ForwardSource, "get_client", return_value=client):
            return run_config_backup(self.sync, snapshot_id=snapshot_id), client

    def test_first_backup_writes_mapped_devices_and_pushes(self):
        result, client = self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1\n"},
                {"name": "fwd-router-2", "config": "hostname router-2\n"},
                {"name": "fwd-unknown", "config": "hostname mystery\n"},
            ]
        )

        self.assertTrue(result.pushed)
        self.assertEqual(result.written, 2)
        self.assertEqual(result.unmapped, 1)
        text, entries = _read_config_blob(self.tmp.name, "router-1.cfg")
        self.assertEqual(text, "hostname router-1\n")
        self.assertEqual(entries, [b"router-1.cfg", b"router-2.cfg"])
        _head, message, parents = _head_and_message(self.tmp.name)
        self.assertEqual(message.strip(), "Forward config backup: snapshot snap-1")
        self.assertEqual(parents, [])
        # Paged, never fetch_all.
        self.assertTrue(
            all(call["limit"] == CONFIG_BACKUP_PAGE_SIZE for call in client.calls)
        )
        # And scoped at the FETCH: Forward is asked only for the devices this
        # sync holds identities for, so out-of-scope configurations are never
        # transferred just to be discarded.
        self.assertEqual(
            client.calls[0]["parameters"],
            {"forward_netbox_shard_keys": ["fwd-router-1", "fwd-router-2"]},
        )

    def test_unchanged_content_produces_no_second_commit(self):
        rows = [{"name": "fwd-router-1", "config": "hostname router-1\n"}]
        self._run(rows)
        first_head, _msg, _parents = _head_and_message(self.tmp.name)

        result, _client = self._run(rows, snapshot_id="snap-2")

        self.assertFalse(result.pushed)
        self.assertEqual(result.skipped_reason, "no configuration changed")
        head, _msg, _parents = _head_and_message(self.tmp.name)
        self.assertEqual(head, first_head)

    def test_a_changed_config_commits_only_that_file_on_top(self):
        self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1\n"},
                {"name": "fwd-router-2", "config": "hostname router-2\n"},
            ]
        )
        first_head, _msg, _parents = _head_and_message(self.tmp.name)

        result, _client = self._run(
            [
                {
                    "name": "fwd-router-1",
                    "config": "hostname router-1\nntp server 192.0.2.1\n",
                },
                {"name": "fwd-router-2", "config": "hostname router-2\n"},
            ],
            snapshot_id="snap-2",
        )

        self.assertTrue(result.pushed)
        self.assertEqual(result.written, 1)
        self.assertEqual(result.unchanged, 1)
        _head, message, parents = _head_and_message(self.tmp.name)
        self.assertEqual(message.strip(), "Forward config backup: snapshot snap-2")
        self.assertEqual(parents, [first_head])
        text, _entries = _read_config_blob(self.tmp.name, "router-1.cfg")
        self.assertIn("ntp server 192.0.2.1", text)

    def test_the_same_snapshot_is_not_fetched_twice(self):
        rows = [{"name": "fwd-router-1", "config": "hostname router-1\n"}]
        self._run(rows)

        result, client = self._run(rows, snapshot_id="snap-1")

        self.assertEqual(result.skipped_reason, "snapshot already backed up")
        self.assertEqual(client.calls, [], "the fetch must be skipped entirely")

    def test_no_identities_means_no_fetch(self):
        """An unscoped shard-key list would pull the whole collected estate.

        With no identities there is nothing to write, so the run must not ask
        Forward for anything at all - passing an empty scope would read as
        "unscoped" and transfer every configuration in the network.
        """
        ForwardDeviceIdentity.objects.filter(sync=self.sync).delete()

        result, client = self._run(
            [{"name": "fwd-router-1", "config": "hostname router-1\n"}]
        )

        self.assertEqual(result.skipped_reason, "no device identities for this sync")
        self.assertEqual(client.calls, [])

    def test_an_empty_fetch_refuses_rather_than_committing_emptiness(self):
        with self.assertRaises(ForwardSyncError):
            self._run([])

    def test_a_device_name_never_becomes_repository_structure(self):
        from dcim.models import Site as _Site

        site = _Site.objects.get(slug="cb-site")
        device = Device.objects.create(
            name="../escape",
            site=site,
            device_type=DeviceType.objects.get(slug="cb-dt"),
            role=DeviceRole.objects.get(slug="cb-role"),
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ForwardIngestion.objects.get(snapshot_id="snap-1"),
            source_device_key="fwd-escape",
            device=device,
        )

        result, _client = self._run(
            [
                {"name": "fwd-escape", "config": "oops\n"},
                {"name": "fwd-router-1", "config": "hostname router-1\n"},
            ]
        )

        self.assertEqual(result.unmapped, 1)
        _text, entries = _read_config_blob(self.tmp.name, "router-1.cfg")
        self.assertEqual(entries, [b"router-1.cfg"])

    def _read_root_tree(self):
        from dulwich.repo import Repo

        with Repo(self.tmp.name) as repo:
            head = repo.refs[repo.refs.follow(b"HEAD")[0][1]]
            commit = repo.object_store[head]
            root = repo.object_store[commit.tree]
            entries = {}
            for name, _mode, sha in root.iteritems():
                tree = repo.object_store[sha]
                entries[name.decode()] = sorted(
                    child.decode() for child, _m, _s in tree.iteritems()
                )
            return entries

    def test_unmanaged_devices_are_not_transferred_by_default(self):
        # The fetch is scoped to this sync's devices; the stray row here is
        # what a real estate returns only when the scope is lifted.
        result, client = self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1"},
                {"name": "fwd-unmanaged", "config": "hostname unmanaged"},
            ]
        )
        self.assertEqual(
            client.calls[0]["parameters"]["forward_netbox_shard_keys"],
            ["fwd-router-1", "fwd-router-2"],
        )
        self.assertEqual(result.unmapped, 1)
        self.assertEqual(result.unmanaged_written, 0)
        self.assertNotIn("unmanaged", self._read_root_tree())

    def test_opting_in_archives_unmanaged_devices_under_their_own_prefix(self):
        """The product question the 2.9.0 plan left open, answered as an opt-in.

        Off is the default because the fetch is otherwise scoped. On, the fetch
        is the whole estate and the surplus lands under `unmanaged/`, named by
        Forward name - apart from `configs/`, which Validity binds to NetBox
        device names an unmanaged device does not have.
        """
        self.sync.source.parameters["config_backup_include_unmanaged"] = True
        self.sync.source.save()
        result, client = self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1"},
                {"name": "fwd-unmanaged", "config": "hostname unmanaged"},
            ]
        )
        self.assertEqual(client.calls[0]["parameters"]["forward_netbox_shard_keys"], [])
        self.assertEqual(result.written, 1)
        self.assertEqual(result.unmanaged_written, 1)
        self.assertEqual(result.unmapped, 0)
        tree = self._read_root_tree()
        self.assertEqual(tree["configs"], ["router-1.cfg"])
        self.assertEqual(tree["unmanaged"], ["fwd-unmanaged.cfg"])
        self.assertIn("unmanaged_written", result.as_dict())

        # A second run with the same content commits nothing new.
        again, _client = self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1"},
                {"name": "fwd-unmanaged", "config": "hostname unmanaged"},
            ],
            snapshot_id="snap-2",
        )
        self.assertEqual(again.unmanaged_unchanged, 1)
        self.assertEqual(again.skipped_reason, "no configuration changed")

    def test_an_unmanaged_name_never_becomes_repository_structure_either(self):
        self.sync.source.parameters["config_backup_include_unmanaged"] = True
        self.sync.source.save()
        result, _client = self._run(
            [
                {"name": "fwd-router-1", "config": "hostname router-1"},
                {"name": "../etc/passwd", "config": "x"},
            ]
        )
        self.assertEqual(result.unmanaged_written, 0)
        self.assertEqual(result.unmapped, 1)

    def test_the_result_payload_never_carries_configuration_text(self):
        marker = "SECRET-CONFIG-LINE-DO-NOT-EXPORT"
        result, _client = self._run(
            [{"name": "fwd-router-1", "config": f"hostname router-1\n{marker}\n"}]
        )

        import json

        self.assertNotIn(marker, json.dumps(result.as_dict()))

    def test_the_push_follows_the_remote_default_branch(self):
        """A backup that pushes where nobody reads is a silent data-loss bug.

        `Repo.init_bare` leaves HEAD pointing at refs/heads/master. Assuming
        `main` when the data source names no branch meant the push succeeded,
        NetBox cloned HEAD, found nothing, and the data source synced ZERO
        files - success reported, nothing delivered. Found by running the
        whole chain against Validity, not by any unit test here.
        """
        from dulwich.repo import Repo

        with Repo(self.tmp.name) as repo:
            head_target = repo.refs.follow(b"HEAD")[0][1]
        self.assertEqual(
            head_target,
            b"refs/heads/master",
            "fixture assumption: a bare repo's HEAD is master, which is what "
            "makes this test meaningful",
        )

        result, _client = self._run(
            [{"name": "fwd-router-1", "config": "hostname router-1\n"}]
        )

        self.assertTrue(result.pushed)
        with Repo(self.tmp.name) as repo:
            refs = repo.get_refs()
        self.assertIn(
            b"refs/heads/master",
            refs,
            "the commit must land on the branch the remote calls default, "
            f"not on an assumed one; refs are {sorted(refs)}",
        )

    def test_an_explicit_branch_parameter_still_wins(self):
        self.data_source.parameters = {"branch": "backups"}
        self.data_source.save()

        result, _client = self._run(
            [{"name": "fwd-router-1", "config": "hostname router-1\n"}]
        )

        self.assertTrue(result.pushed)
        from dulwich.repo import Repo

        with Repo(self.tmp.name) as repo:
            self.assertIn(b"refs/heads/backups", repo.get_refs())

    def test_a_non_git_data_source_is_refused(self):
        local = DataSource.objects.create(
            name="local-files", type="local", source_url="file:///tmp"
        )
        self.sync.source.parameters["config_backup_data_source"] = local.pk
        self.sync.source.save()

        with self.assertRaises(ForwardSyncError):
            self._run([{"name": "fwd-router-1", "config": "x\n"}])

    def test_changed_blobs_are_written_as_produced_not_accumulated(self):
        """Peak memory on a large fleet depends on this, not on page size.

        Measured on 3,400 synthetic devices (~1.9 GB of configs): holding
        every changed `Blob` in a list until the fetch loop finished cost 4.2
        GB of peak RSS - the whole fleet's blobs plus dulwich's own overhead,
        resident at once. Writing each blob to the object store as soon as
        it is produced cut that to 2.4 GB on the identical run. The NQE page
        size bounds fetch memory; it says nothing about this accumulation,
        which dominates on any run touching most of the fleet - a first
        backup being exactly that case.

        A count of `add_object` calls cannot tell "batched afterward" from
        "written as produced" - both call it once per row, just at different
        times. This crosses a real page boundary and records fetches and
        writes on one shared timeline: fixed, page one's writes land before
        the second fetch; accumulated, every write lands after the last
        fetch. An earlier version of this test asserted only the count and
        passed against the accumulating code it exists to catch - the
        negative control that should have failed it, did not.
        """
        from unittest.mock import patch

        from dulwich.object_store import DiskObjectStore

        # More devices than one page, mapped through this test's own sync so
        # the boundary is guaranteed to fall mid-fleet.
        site = Site.objects.create(name="Order Site", slug="order-site")
        mfr = Manufacturer.objects.create(name="Order Mfr", slug="order-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="Order DT", slug="order-dt"
        )
        role = DeviceRole.objects.create(name="Order Role", slug="order-role")
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-order"
        )
        count = CONFIG_BACKUP_PAGE_SIZE + 5
        rows = []
        for i in range(count):
            device = Device.objects.create(
                name=f"order-{i:03d}", site=site, device_type=dtype, role=role
            )
            ForwardDeviceIdentity.objects.create(
                sync=self.sync,
                ingestion=ingestion,
                source_device_key=f"fwd-order-{i:03d}",
                device=device,
            )
            rows.append(
                {"name": f"fwd-order-{i:03d}", "config": f"hostname order-{i}\n"}
            )

        timeline = []
        original_add_object = DiskObjectStore.add_object

        def recording_add_object(self, obj):
            if type(obj).__name__ == "Blob":
                timeline.append("write")
            return original_add_object(self, obj)

        client = _FakeClient(rows)
        original_fetch = client.run_nqe_query

        def recording_fetch(**kwargs):
            timeline.append("fetch")
            return original_fetch(**kwargs)

        client.run_nqe_query = recording_fetch
        with patch.object(
            ForwardSource, "get_client", return_value=client
        ), patch.object(DiskObjectStore, "add_object", recording_add_object):
            run_config_backup(self.sync, snapshot_id="snap-order")

        self.assertEqual(
            timeline.count("fetch"), 2, f"expected two pages, got {timeline}"
        )
        self.assertEqual(timeline.count("write"), count, "one write per changed row")
        second_fetch_index = [i for i, e in enumerate(timeline) if e == "fetch"][1]
        writes_before_second_fetch = timeline[:second_fetch_index].count("write")
        self.assertGreater(
            writes_before_second_fetch,
            0,
            "no write happened before the second page was fetched - every "
            "blob was accumulated and written in one batch afterward, which "
            f"is the regression this test exists to catch: timeline={timeline}",
        )


class ValidityReadsWhatWeWriteTest(TestCase):
    """The chain, end to end, through the consumer that motivated the feature.

    Every step of this succeeds independently while delivering nothing if
    another is wrong, which is exactly how the branch bug hid: the backup
    reported `pushed=True` with a commit SHA while the data source synced zero
    files. Only running the whole chain - push, sync, and Validity's own path
    resolution - shows whether a configuration actually arrives.

    Validity binds a device to its file through the data source's
    `device_config_path` custom field, rendered as Jinja2 with `device` in
    context. That contract is asserted here rather than assumed, so a Validity
    release that changes it fails this test instead of silently orphaning
    every backup.

    Skipped when Validity is not installed; it is an optional integration.
    """

    def setUp(self):
        from django.apps import apps as django_apps

        if not django_apps.is_installed("validity"):
            self.skipTest("netbox-validity is not installed")

        from dulwich.repo import Repo

        self.tmp = tempfile.TemporaryDirectory(prefix="cfg-validity-")
        self.addCleanup(self.tmp.cleanup)
        Repo.init_bare(self.tmp.name).close()

        self.data_source = DataSource.objects.create(
            name="validity-config-backups", type="git", source_url=self.tmp.name
        )
        cf, _created = CustomField.objects.get_or_create(
            name="device_config_path",
            defaults={"type": "text", "label": "Device config path"},
        )
        cf.object_types.set([ContentType.objects.get_for_model(DataSource)])
        cf.save()
        self.data_source.custom_field_data["device_config_path"] = (
            "configs/{{device.name}}.cfg"
        )
        self.data_source.save()

        user = get_user_model().objects.create_user(username="validity-owner")
        source = ForwardSource.objects.create(
            name="validity-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-1",
                "config_backup_data_source": self.data_source.pk,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="validity-sync", source=source, user=user
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-v1"
        )
        site = Site.objects.create(name="V Site", slug="v-site")
        mfr = Manufacturer.objects.create(name="V Mfr", slug="v-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="V DT", slug="v-dt")
        role = DeviceRole.objects.create(name="V Role", slug="v-role")
        self.device = Device.objects.create(
            name="validity-edge-1", site=site, device_type=dtype, role=role
        )
        ForwardDeviceIdentity.objects.create(
            sync=self.sync,
            ingestion=ingestion,
            source_device_key="fwd-validity-edge-1",
            device=self.device,
        )

    def test_a_backed_up_config_is_readable_by_validity(self):
        from unittest.mock import patch

        from validity.models import VDataSource
        from validity.models import VDevice

        config = "hostname validity-edge-1\n!\nntp server 192.0.2.1\n!\nend\n"
        client = _FakeClient([{"name": "fwd-validity-edge-1", "config": config}])
        with patch.object(ForwardSource, "get_client", return_value=client):
            result = run_config_backup(self.sync, snapshot_id="snap-v1")
        self.assertTrue(result.pushed)

        self.data_source.refresh_from_db()
        self.data_source.sync()
        self.assertTrue(
            self.data_source.datafiles.exists(),
            "the data source synced no files - the commit landed somewhere "
            "NetBox does not read, which is the failure this test exists for",
        )

        vds = VDataSource.objects.get(pk=self.data_source.pk)
        vdev = VDevice.objects.get(pk=self.device.pk)
        rendered = vds.get_config_path(vdev)

        self.assertEqual(rendered, f"configs/{self.device.name}.cfg")
        data_file = vds.datafiles.filter(path=rendered).first()
        self.assertIsNotNone(
            data_file,
            f"Validity resolves {rendered} but no data file has that path; "
            f"available: {sorted(vds.datafiles.values_list('path', flat=True))}",
        )
        self.assertEqual(data_file.data_as_string, config)


class AuthenticatedUrlTest(SimpleTestCase):
    """The one function that handles a credential, and it had no tests.

    Every other test in this file pushes to a local bare repository, where a
    filesystem path carries no credentials at all - so the whole HTTP(S)
    embedding path, the characters that must be escaped in it, and the
    property that matters most (the assembled url never leaves this function)
    were unexercised.
    """

    def _source(self, url, **parameters):
        return SimpleNamespace(source_url=url, parameters=parameters or {})

    def test_no_credentials_returns_the_url_unchanged(self):
        source = self._source("https://git.example.com/configs.git")
        self.assertEqual(
            _authenticated_url(source), "https://git.example.com/configs.git"
        )

    def test_username_and_password_are_embedded(self):
        source = self._source(
            "https://git.example.com/configs.git", username="svc", password="s3cret"
        )
        self.assertEqual(
            _authenticated_url(source),
            "https://svc:s3cret@git.example.com/configs.git",
        )

    def test_special_characters_are_percent_encoded(self):
        # An unescaped `@` or `/` in a password splits the authority and the
        # push goes to a host nobody configured - or silently authenticates as
        # a different user. `safe=""` is what makes that impossible.
        source = self._source(
            "https://git.example.com/configs.git",
            username="svc@corp",
            password="p@ss/w:rd?",
        )
        url = _authenticated_url(source)
        self.assertEqual(
            url,
            "https://svc%40corp:p%40ss%2Fw%3Ard%3F@git.example.com/configs.git",
        )
        # The host survived intact: the credential did not eat it.
        self.assertTrue(url.endswith("@git.example.com/configs.git"))

    def test_an_existing_credential_in_the_url_is_replaced_not_appended(self):
        # Two `@` in the authority is not a valid url, and appending would
        # produce one.
        source = self._source(
            "https://old:stale@git.example.com/configs.git",
            username="svc",
            password="new",
        )
        self.assertEqual(
            _authenticated_url(source),
            "https://svc:new@git.example.com/configs.git",
        )

    def test_ssh_remotes_are_left_alone(self):
        # ssh authenticates with keys; embedding is neither needed nor
        # meaningful, and would corrupt a scp-style remote.
        source = self._source(
            "ssh://git@git.example.com/configs.git", username="svc", password="p"
        )
        self.assertEqual(
            _authenticated_url(source), "ssh://git@git.example.com/configs.git"
        )

    def test_a_username_with_no_password_omits_the_colon(self):
        source = self._source("https://git.example.com/configs.git", username="token")
        self.assertEqual(
            _authenticated_url(source),
            "https://token@git.example.com/configs.git",
        )

    def test_the_query_string_survives_and_the_fragment_is_dropped(self):
        source = self._source(
            "https://git.example.com/configs.git?depth=1#frag",
            username="svc",
            password="p",
        )
        self.assertEqual(
            _authenticated_url(source),
            "https://svc:p@git.example.com/configs.git?depth=1",
        )


def _seed_initial_commit(repo_path):
    """One empty commit on the repository's default branch."""
    import time

    from dulwich.objects import Commit
    from dulwich.objects import Tree
    from dulwich.repo import Repo

    with Repo(repo_path) as repo:
        tree = Tree()
        repo.object_store.add_object(tree)
        commit = Commit()
        commit.tree = tree.id
        commit.author = commit.committer = b"Fixture <fixture@example.invalid>"
        commit.author_time = commit.commit_time = int(time.time())
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"initial"
        repo.object_store.add_object(commit)
        repo.refs[repo.refs.follow(b"HEAD")[0][1]] = commit.id


class _GitHttpRemote:
    """A smart-HTTP git remote in this process, with basic auth.

    dulwich serves its own protocol over WSGI, so the credentialed path is
    exercised end to end - client credential embedding, the server's 401,
    the push over HTTP - with no container, no network beyond loopback and no
    git binary, which the runtime does not have either. `refuse_push=True`
    makes the remote answer every receive-pack with 403, the shape of a
    remote that rejects the push after accepting the fetch.
    """

    def __init__(self, path, *, username, password, refuse_push=False):
        import base64
        import threading
        from wsgiref.simple_server import WSGIRequestHandler
        from wsgiref.simple_server import make_server

        from dulwich.repo import Repo
        from dulwich.server import DictBackend
        from dulwich.web import make_wsgi_chain

        expected = "Basic " + base64.b64encode(
            f"{username}:{password}".encode()
        ).decode("ascii")
        self.repo = Repo(path)
        app = make_wsgi_chain(DictBackend({"/": self.repo}))

        def guarded(environ, start_response):
            if environ.get("HTTP_AUTHORIZATION") != expected:
                start_response(
                    "401 Unauthorized",
                    [("WWW-Authenticate", 'Basic realm="configs"')],
                )
                return [b"auth required"]
            if refuse_push and "git-receive-pack" in environ.get("PATH_INFO", ""):
                start_response("403 Forbidden", [("Content-Type", "text/plain")])
                return [b"push refused"]
            return app(environ, start_response)

        class _Quiet(WSGIRequestHandler):
            def log_message(self, *args):  # noqa: D401 - keep test output clean
                return

        self.server = make_server("127.0.0.1", 0, guarded, handler_class=_Quiet)
        self.url = f"http://127.0.0.1:{self.server.server_port}/"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.repo.close()


class ConfigBackupOverHttpTest(TestCase):
    """The credentialed path: an authenticated push, a 401, and a refusal."""

    USERNAME = "svc-backup"
    PASSWORD = "p@ss/w:rd?"

    def setUp(self):
        from dulwich.repo import Repo

        self.tmp = tempfile.TemporaryDirectory(prefix="cfg-backup-http-")
        self.addCleanup(self.tmp.cleanup)
        Repo.init_bare(self.tmp.name).close()
        # Over smart HTTP an EMPTY repository advertises no refs and no HEAD
        # symref, so the client has nothing to follow and invents `main`. A
        # remote someone actually configures has a first commit (a README at
        # least), and that is the shape that lets the push follow the remote's
        # own default branch - so the fixture seeds one.
        _seed_initial_commit(self.tmp.name)
        user = get_user_model().objects.create_user(username="http-backup-owner")
        site = Site.objects.create(name="HB Site", slug="hb-site")
        mfr = Manufacturer.objects.create(name="HB Mfr", slug="hb-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="HB DT", slug="hb-dt")
        role = DeviceRole.objects.create(name="HB Role", slug="hb-role")
        self.device = Device.objects.create(
            name="router-1", site=site, device_type=dtype, role=role
        )
        self.user = user

    def _remote(self, **kwargs):
        remote = _GitHttpRemote(
            self.tmp.name, username=self.USERNAME, password=self.PASSWORD, **kwargs
        )
        self.addCleanup(remote.close)
        return remote

    def _sync(self, url, *, password):
        data_source = DataSource.objects.create(
            name="http-config-backups",
            type="git",
            source_url=url,
            parameters={"username": self.USERNAME, "password": password},
        )
        source = ForwardSource.objects.create(
            name="http-backup-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-1",
                "config_backup_data_source": data_source.pk,
            },
        )
        sync = ForwardSync.objects.create(
            name="http-backup-sync", source=source, user=self.user
        )
        ingestion = ForwardIngestion.objects.create(sync=sync, snapshot_id="snap-1")
        ForwardDeviceIdentity.objects.create(
            sync=sync,
            ingestion=ingestion,
            source_device_key="fwd-router-1",
            device=self.device,
        )
        return sync

    def _run(self, sync):
        from unittest.mock import patch

        client = _FakeClient([{"name": "fwd-router-1", "config": "hostname r1\n"}])
        with patch.object(ForwardSource, "get_client", return_value=client):
            return run_config_backup(sync, snapshot_id="snap-1")

    def test_an_authenticated_push_lands_on_the_remote(self):
        remote = self._remote()
        result = self._run(self._sync(remote.url, password=self.PASSWORD))

        self.assertTrue(result.pushed)
        text, _entries = _read_config_blob(self.tmp.name, "router-1.cfg")
        self.assertEqual(text, "hostname r1\n")

    def test_refused_credentials_are_named_and_never_echoed(self):
        remote = self._remote()
        sync = self._sync(remote.url, password="wrong")

        with self.assertRaises(ForwardSyncError) as caught:
            self._run(sync)

        message = str(caught.exception)
        self.assertIn("credentials were refused, HTTP 401", message)
        # The url carries the credential; neither may reach the message.
        self.assertNotIn("wrong", message)
        self.assertNotIn(self.USERNAME, message)
        self.assertNotIn(remote.url, message)
        self.assertNotIn("127.0.0.1", message)

    def test_an_empty_remote_with_no_default_branch_is_refused_with_the_remedy(self):
        # Over smart HTTP an empty repository advertises nothing; inventing
        # `main` against a `master` default delivered a backup nobody reads.
        from dulwich.repo import Repo

        empty = tempfile.TemporaryDirectory(prefix="cfg-backup-empty-")
        self.addCleanup(empty.cleanup)
        Repo.init_bare(empty.name).close()
        remote = _GitHttpRemote(
            empty.name, username=self.USERNAME, password=self.PASSWORD
        )
        self.addCleanup(remote.close)
        sync = self._sync(remote.url, password=self.PASSWORD)

        with self.assertRaises(ForwardSyncError) as caught:
            self._run(sync)

        self.assertIn("advertises no default branch", str(caught.exception))
        self.assertIn("`branch` parameter", str(caught.exception))

    def test_an_explicit_branch_covers_an_empty_remote(self):
        from dulwich.repo import Repo

        empty = tempfile.TemporaryDirectory(prefix="cfg-backup-empty2-")
        self.addCleanup(empty.cleanup)
        Repo.init_bare(empty.name).close()
        remote = _GitHttpRemote(
            empty.name, username=self.USERNAME, password=self.PASSWORD
        )
        self.addCleanup(remote.close)
        sync = self._sync(remote.url, password=self.PASSWORD)
        data_source = DataSource.objects.get(name="http-config-backups")
        data_source.parameters["branch"] = "configs"
        data_source.save()

        result = self._run(sync)

        self.assertTrue(result.pushed)
        with Repo(empty.name) as repo:
            self.assertIn(b"refs/heads/configs", repo.refs.keys())

    def test_a_remote_that_refuses_the_push_is_reported_without_the_url(self):
        remote = self._remote(refuse_push=True)
        sync = self._sync(remote.url, password=self.PASSWORD)

        with self.assertRaises(ForwardSyncError) as caught:
            self._run(sync)

        message = str(caught.exception)
        self.assertIn("could not push", message)
        self.assertNotIn(self.PASSWORD, message)
        self.assertNotIn("p%40ss", message)
        self.assertNotIn(remote.url, message)
        self.assertNotIn("127.0.0.1", message)
        # Nothing was written to the remote.
        with self.assertRaises(Exception):
            _read_config_blob(self.tmp.name, "router-1.cfg")


class ValidityRoundTripCannotSkipSilentlyTest(SimpleTestCase):
    """The round trip above skips when Validity is absent. That is right for
    an operator's deployment and wrong for ours: the integration is pinned in
    `constraints.txt`, so an image built without it would turn the only
    end-to-end proof into a skip nobody reads. This is the guard."""

    def test_a_pinned_validity_is_installed_in_the_test_runtime(self):
        from pathlib import Path

        from django.apps import apps as django_apps

        constraints = (
            Path(__file__).resolve().parents[2] / "constraints.txt"
        ).read_text(encoding="utf-8")
        pinned = any(
            line.strip().startswith("netbox-validity==")
            for line in constraints.splitlines()
        )
        if not pinned:
            self.skipTest("netbox-validity is not a pinned integration")
        self.assertTrue(
            django_apps.is_installed("validity"),
            "netbox-validity is pinned in constraints.txt but not installed in "
            "this runtime, so ValidityReadsWhatWeWriteTest skipped instead of "
            "proving the round trip",
        )


class ValidityRoundTripVariantsTest(ValidityReadsWhatWeWriteTest):
    """The same chain under the bindings an operator actually configures."""

    def _backup(self, config, *, unmanaged=None):
        from unittest.mock import patch

        rows = [{"name": "fwd-validity-edge-1", "config": config}]
        if unmanaged:
            rows.append({"name": unmanaged, "config": "hostname stray\n"})
        client = _FakeClient(rows)
        with patch.object(ForwardSource, "get_client", return_value=client):
            result = run_config_backup(self.sync, snapshot_id="snap-v1")
        self.assertTrue(result.pushed)
        self.data_source.refresh_from_db()
        self.data_source.sync()
        return result

    def test_a_tenant_bound_data_source_still_resolves(self):
        from tenancy.models import Tenant
        from validity.models import VDataSource
        from validity.models import VDevice

        tenant = Tenant.objects.create(name="V Tenant", slug="v-tenant")
        Tenant.objects.filter(pk=tenant.pk).update(
            custom_field_data={"data_source": self.data_source.pk}
        )
        Device.objects.filter(pk=self.device.pk).update(tenant=tenant)
        config = "hostname validity-edge-1\n"
        self._backup(config)

        vds = VDataSource.objects.get(pk=self.data_source.pk)
        rendered = vds.get_config_path(VDevice.objects.get(pk=self.device.pk))
        data_file = vds.datafiles.filter(path=rendered).first()
        self.assertIsNotNone(data_file)
        self.assertEqual(data_file.data_as_string, config)

    def test_a_non_default_config_path_template_is_honoured_by_both_sides(self):
        # The operator may template on more than the name; what matters is
        # that Validity's rendering and our file layout meet. A template that
        # renders to our path with a different expression still resolves.
        from validity.models import VDataSource
        from validity.models import VDevice

        self.data_source.custom_field_data["device_config_path"] = (
            "{{ 'configs' }}/{{ device.name | lower }}.cfg"
        )
        self.data_source.save()
        config = "hostname validity-edge-1\n"
        self._backup(config)

        vds = VDataSource.objects.get(pk=self.data_source.pk)
        rendered = vds.get_config_path(VDevice.objects.get(pk=self.device.pk))
        self.assertEqual(rendered, "configs/validity-edge-1.cfg")
        self.assertIsNotNone(vds.datafiles.filter(path=rendered).first())

    def test_unmanaged_files_are_synced_but_never_resolve_to_a_device(self):
        from validity.models import VDataSource

        self.sync.source.parameters["config_backup_include_unmanaged"] = True
        self.sync.source.save()
        self._backup("hostname validity-edge-1\n", unmanaged="fwd-stray")

        vds = VDataSource.objects.get(pk=self.data_source.pk)
        paths = sorted(vds.datafiles.values_list("path", flat=True))
        self.assertIn("unmanaged/fwd-stray.cfg", paths)
        self.assertIn("configs/validity-edge-1.cfg", paths)
        # No NetBox device named `fwd-stray` exists, so nothing can resolve
        # to the unmanaged file: it is an archive, not a compliance input.
        self.assertFalse(Device.objects.filter(name="fwd-stray").exists())
