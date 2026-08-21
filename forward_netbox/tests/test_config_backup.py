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

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import DataSource
from forward_netbox.exceptions import ForwardSyncError
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

    def run_nqe_query(self, *, query, network_id, snapshot_id, limit, offset):
        self.calls.append({"limit": limit, "offset": offset})
        return self.rows[offset : offset + limit]


def _read_config_blob(repo_path, file_name):
    from dulwich.repo import Repo

    with Repo(repo_path) as repo:
        head = repo.refs[b"refs/heads/main"]
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
        head = repo.refs[b"refs/heads/main"]
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

    def test_the_result_payload_never_carries_configuration_text(self):
        marker = "SECRET-CONFIG-LINE-DO-NOT-EXPORT"
        result, _client = self._run(
            [{"name": "fwd-router-1", "config": f"hostname router-1\n{marker}\n"}]
        )

        import json

        self.assertNotIn(marker, json.dumps(result.as_dict()))

    def test_a_non_git_data_source_is_refused(self):
        local = DataSource.objects.create(
            name="local-files", type="local", source_url="file:///tmp"
        )
        self.sync.source.parameters["config_backup_data_source"] = local.pk
        self.sync.source.save()

        with self.assertRaises(ForwardSyncError):
            self._run([{"name": "fwd-router-1", "config": "x\n"}])
