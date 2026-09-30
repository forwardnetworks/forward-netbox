"""The config backup folder: nested, changeable, and never destructive.

The git half runs against a LOCAL bare repository, so what is asserted is the
object graph a remote would receive. The negative space carries the weight: a
changed folder must not delete the old one, a sibling of a nested folder must
survive a write, a FILE where a folder is needed must stop the run rather than
be overwritten, and an unusable stored folder must fail with a sentence that
says where to fix it.
"""

import tempfile
import time
from unittest.mock import patch

from core.models import DataSource
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.test import TestCase

from forward_netbox.models import ForwardDeviceIdentity
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.config_backup import ConfigBackupError
from forward_netbox.utilities.config_backup import run_config_backup
from forward_netbox.utilities.health import config_backup_delivery_bundle_payload
from forward_netbox.utilities.health import config_backup_delivery_state

ROWS = [
    {"name": "fwd-router-1", "config": "hostname router-1\n"},
    {"name": "fwd-router-2", "config": "hostname router-2\n"},
]


class _FakeClient:
    def __init__(self, rows):
        self.rows = rows

    def run_nqe_query(
        self, *, query, network_id, snapshot_id, parameters, limit, offset
    ):
        return self.rows[offset : offset + limit]


def _walk(repo_path):
    """Every path in the head commit's tree, folders marked with a slash."""
    from dulwich.repo import Repo

    paths = []
    with Repo(repo_path) as repo:
        head = repo.refs[repo.refs.follow(b"HEAD")[0][1]]

        def visit(tree_sha, base):
            for name, mode, sha in repo.object_store[tree_sha].iteritems():
                path = base + name.decode()
                if mode == 0o040000:
                    paths.append(path + "/")
                    visit(sha, path + "/")
                else:
                    paths.append(path)

        visit(repo.object_store[head].tree, "")
    return sorted(paths)


def _commit_file(repo_path, name, data):
    """Put a plain FILE at the repository root, as a hand-made commit."""
    from dulwich.objects import Blob
    from dulwich.objects import Commit
    from dulwich.objects import Tree
    from dulwich.repo import Repo

    with Repo(repo_path) as repo:
        blob = Blob.from_string(data)
        repo.object_store.add_object(blob)
        tree = Tree()
        tree.add(name.encode(), 0o100644, blob.id)
        repo.object_store.add_object(tree)
        commit = Commit()
        commit.tree = tree.id
        commit.parents = []
        commit.author = commit.committer = b"test <test@localhost>"
        commit.author_time = commit.commit_time = int(time.time())
        commit.author_timezone = commit.commit_timezone = 0
        commit.encoding = b"UTF-8"
        commit.message = b"a file where a folder is wanted"
        repo.object_store.add_object(commit)
        repo.refs[repo.refs.follow(b"HEAD")[0][1]] = commit.id


class FolderTestBase(TestCase):
    def setUp(self):
        from dulwich.repo import Repo

        self.tmp = tempfile.TemporaryDirectory(prefix="cfg-folder-remote-")
        self.addCleanup(self.tmp.cleanup)
        Repo.init_bare(self.tmp.name).close()
        self.data_source = DataSource.objects.create(
            name="folder-backups", type="git", source_url=self.tmp.name
        )
        user = get_user_model().objects.create_user(username="folder-owner")
        self.source = ForwardSource.objects.create(
            name="folder-source",
            type="saas",
            url="https://fwd.app",
            parameters={
                "network_id": "net-1",
                "config_backup_data_source": self.data_source.pk,
            },
        )
        self.sync = ForwardSync.objects.create(
            name="folder-sync", source=self.source, user=user
        )
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="snap-1"
        )
        site = Site.objects.create(name="F Site", slug="f-site")
        mfr = Manufacturer.objects.create(name="F Mfr", slug="f-mfr")
        dtype = DeviceType.objects.create(manufacturer=mfr, model="F DT", slug="f-dt")
        role = DeviceRole.objects.create(name="F Role", slug="f-role")
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

    def _folder(self, value):
        parameters = dict(self.source.parameters)
        parameters["config_backup_path_prefix"] = value
        self.source.parameters = parameters
        self.source.save()
        self.sync.refresh_from_db()

    def _run(self, snapshot_id="snap-1", rows=ROWS):
        with patch.object(ForwardSource, "get_client", return_value=_FakeClient(rows)):
            return run_config_backup(self.sync, snapshot_id=snapshot_id)


class WriteTest(FolderTestBase):
    def test_the_default_folder_is_configs(self):
        result = self._run()

        self.assertTrue(result.pushed)
        self.assertEqual(
            _walk(self.tmp.name),
            ["configs/", "configs/router-1.cfg", "configs/router-2.cfg"],
        )

    def test_a_nested_folder_holds_the_files_and_nothing_else_is_at_the_root(self):
        self._folder("net/configs")

        result = self._run()

        self.assertTrue(result.pushed)
        self.assertEqual(result.written, 2)
        self.assertEqual(
            _walk(self.tmp.name),
            [
                "net/",
                "net/configs/",
                "net/configs/router-1.cfg",
                "net/configs/router-2.cfg",
            ],
        )

    def test_a_second_run_with_the_same_content_makes_no_new_commit(self):
        self._folder("net/configs")
        self._run()

        again = self._run(snapshot_id="snap-2")

        self.assertFalse(again.pushed)
        self.assertEqual(again.skipped_reason, "no configuration changed")
        self.assertEqual(again.unchanged, 2)

    def test_a_sibling_of_a_nested_folder_survives_a_write(self):
        self._folder("net/configs")
        self._run()
        self._folder("net/archive")

        self._run(snapshot_id="snap-2")

        self.assertEqual(
            _walk(self.tmp.name),
            [
                "net/",
                "net/archive/",
                "net/archive/router-1.cfg",
                "net/archive/router-2.cfg",
                "net/configs/",
                "net/configs/router-1.cfg",
                "net/configs/router-2.cfg",
            ],
        )

    def test_changing_the_folder_leaves_the_old_files_and_says_so(self):
        self._run()
        self._folder("net/configs")

        result = self._run(snapshot_id="snap-2")

        self.assertEqual(
            _walk(self.tmp.name),
            [
                "configs/",
                "configs/router-1.cfg",
                "configs/router-2.cfg",
                "net/",
                "net/configs/",
                "net/configs/router-1.cfg",
                "net/configs/router-2.cfg",
            ],
        )
        self.assertEqual(len(result.warnings), 1)
        self.assertIn("still holds a `configs/` folder", result.warnings[0])
        self.assertIn("`net/configs/`", result.warnings[0])

    def test_no_warning_when_the_default_folder_is_in_use(self):
        self.assertEqual(self._run().warnings, [])

    def test_the_unmanaged_folder_stays_apart_from_a_custom_folder(self):
        self._folder("net/configs")
        parameters = dict(self.source.parameters)
        parameters["config_backup_include_unmanaged"] = True
        self.source.parameters = parameters
        self.source.save()
        self.sync.refresh_from_db()

        self._run(
            rows=ROWS + [{"name": "fwd-unmanaged", "config": "hostname unmanaged\n"}]
        )

        self.assertIn("unmanaged/fwd-unmanaged.cfg", _walk(self.tmp.name))
        self.assertIn("net/configs/router-1.cfg", _walk(self.tmp.name))


class NegativeSpaceTest(FolderTestBase):
    def test_a_file_where_a_folder_is_needed_stops_the_run_and_is_not_overwritten(
        self,
    ):
        _commit_file(self.tmp.name, "net", b"not a folder\n")
        self._folder("net/configs")

        with self.assertRaises(ConfigBackupError) as caught:
            self._run()

        message = str(caught.exception)
        self.assertIn("[build]", message)
        self.assertIn("a file named `net`", message)
        self.assertIn("source's settings", message)
        self.assertEqual(_walk(self.tmp.name), ["net"])

    def test_an_unusable_stored_folder_fails_naming_where_to_fix_it(self):
        self._folder("../escape")

        with self.assertRaises(ConfigBackupError) as caught:
            self._run()

        message = str(caught.exception)
        self.assertIn("[resolve]", message)
        self.assertIn("not usable", message)
        self.assertIn("source's settings", message)
        self.assertEqual(_walk_or_empty(self.tmp.name), [])


def _walk_or_empty(repo_path):
    try:
        return _walk(repo_path)
    except KeyError:
        return []


class HealthTest(FolderTestBase):
    def test_the_validity_path_names_the_configured_folder(self):
        self._folder("net/configs")

        state = config_backup_delivery_state(self.sync)

        self.assertTrue(state["path_prefix_customized"])
        self.assertTrue(state["path_prefix_valid"])

    def test_the_default_folder_is_not_customized(self):
        state = config_backup_delivery_state(self.sync)

        self.assertFalse(state["path_prefix_customized"])

    def test_an_invalid_stored_folder_is_reported_not_raised(self):
        self._folder("../escape")

        state = config_backup_delivery_state(self.sync)

        self.assertFalse(state["path_prefix_valid"])

    def test_the_bundle_says_customized_and_valid_but_never_the_folder(self):
        self._folder("net/configs")

        payload = config_backup_delivery_bundle_payload(self.sync)

        self.assertTrue(payload["path_prefix_customized"])
        self.assertTrue(payload["path_prefix_valid"])
        self.assertNotIn("net/configs", repr(payload))
