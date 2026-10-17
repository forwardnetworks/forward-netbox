from datetime import timedelta

from django.utils import timezone
from extras.models import JournalEntry

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.workspace import adopt_workspace
from forward_netbox.change_control.workspace import create_workspace
from forward_netbox.change_control.workspace import refresh_snapshots
from forward_netbox.change_control.workspace import workspace_device_names
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.tests.test_change_control_actions import ChangeFixture


class FakeApi:
    """Stands in for Forward: records what it was asked and answers from a script."""

    def __init__(self, networks=None, snapshots=None):
        self.networks = networks or {}
        self.snapshots_by_network = snapshots or {}
        self.created = []

    def network(self, network_id):
        return self.networks.get(network_id)

    def create_workspace(self, parent_id, *, name, devices, retention_days):
        self.created.append(
            {
                "parent": parent_id,
                "name": name,
                "devices": list(devices),
                "retention_days": retention_days,
            }
        )
        new_id = f"ws-{len(self.created)}"
        self.networks[new_id] = {"id": new_id, "name": name, "parent_id": parent_id}
        return new_id

    def snapshots(self, network_id):
        return list(self.snapshots_by_network.get(network_id, []))


def snap(snapshot_id, trigger, state="PROCESSED", created=None):
    return {
        "id": snapshot_id,
        "trigger": trigger,
        "state": state,
        "created_at": (created or timezone.now()).isoformat(),
    }


class ScopedChange(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(State.DRAFT, branch=None)
        # The source's network is net-1 (see ChangeFixture).
        for name in ("sw-b", "sw-a"):
            ForwardChangeDevice.objects.create(
                change=self.change_, device=self.device(), forward_device_key=name
            )


class DeviceNamesTest(ScopedChange):
    def test_the_workspace_is_scoped_to_the_changes_devices_by_forward_name(self):
        self.assertEqual(workspace_device_names(self.change_), ["sw-a", "sw-b"])

    def test_a_device_without_a_forward_name_is_not_scoped_by_its_netbox_name(self):
        # The verify gate checks completeness by the Forward name, so scoping by
        # another name would put a device in the workspace that is never checked.
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key=""
        )

        self.assertEqual(workspace_device_names(self.change_), ["sw-a", "sw-b"])

    def test_a_workspace_is_refused_while_a_device_has_no_forward_name(self):
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key=""
        )
        api = FakeApi()

        outcome = create_workspace(self.change_, self.requester, api=api)

        self.assertFalse(outcome.ok)
        self.assertIn("no Forward name", outcome.message)
        self.assertEqual(api.created, [])


class CreatingTest(ScopedChange):
    def test_it_forks_the_sources_network_scoped_to_the_devices(self):
        api = FakeApi()

        outcome = create_workspace(self.change_, self.requester, api=api)

        self.assertTrue(outcome.ok, outcome.message)
        (call,) = api.created
        self.assertEqual(call["parent"], "net-1")
        self.assertEqual(call["devices"], ["sw-a", "sw-b"])
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.workspace_network_id, "ws-1")
        self.assertEqual(self.change_.workspace_origin, "created")

    def test_it_is_journaled_with_who_asked(self):
        create_workspace(self.change_, self.requester, api=FakeApi())

        entry = JournalEntry.objects.filter(assigned_object_id=self.change_.pk).get()
        self.assertEqual(entry.created_by, self.requester)
        self.assertIn("workspace", entry.comments.lower())

    def test_a_change_with_no_devices_gets_no_workspace(self):
        empty = self.change(State.DRAFT, branch=None)
        api = FakeApi()

        outcome = create_workspace(empty, self.requester, api=api)

        self.assertFalse(outcome.ok)
        self.assertEqual(api.created, [])

    def test_a_change_that_already_has_one_does_not_get_a_second(self):
        api = FakeApi()
        create_workspace(self.change_, self.requester, api=api)
        self.change_.refresh_from_db()

        outcome = create_workspace(self.change_, self.requester, api=api)

        self.assertFalse(outcome.ok)
        self.assertEqual(len(api.created), 1)

    def test_an_applied_change_gets_no_workspace(self):
        # A fork taken after the apply already contains the change.
        applied = self.change(State.APPLIED, branch=None)
        ForwardChangeDevice.objects.create(
            change=applied, device=self.device(), forward_device_key="sw-z"
        )
        api = FakeApi()

        self.assertFalse(create_workspace(applied, self.requester, api=api).ok)
        self.assertEqual(api.created, [])

    def test_a_finished_change_gets_no_workspace(self):
        done = self.change(State.CLOSED, branch=None)
        ForwardChangeDevice.objects.create(
            change=done, device=self.device(), forward_device_key="sw-z"
        )
        api = FakeApi()

        self.assertFalse(create_workspace(done, self.requester, api=api).ok)
        self.assertEqual(api.created, [])

    def test_a_forward_failure_is_reported_and_nothing_is_recorded(self):
        class Down(FakeApi):
            def create_workspace(self, *a, **k):
                raise RuntimeError("forward is down")

        outcome = create_workspace(self.change_, self.requester, api=Down())

        self.assertFalse(outcome.ok)
        self.assertIn("forward is down", outcome.message)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.workspace_network_id, "")


class AdoptingTest(ScopedChange):
    def test_it_adopts_a_workspace_of_the_sources_network(self):
        api = FakeApi(
            networks={"ws-9": {"id": "ws-9", "name": "t", "parent_id": "net-1"}}
        )

        outcome = adopt_workspace(self.change_, "ws-9", self.requester, api=api)

        self.assertTrue(outcome.ok, outcome.message)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.workspace_network_id, "ws-9")
        self.assertEqual(self.change_.workspace_origin, "adopted")

    def test_a_workspace_of_another_network_is_refused(self):
        api = FakeApi(
            networks={"ws-9": {"id": "ws-9", "name": "t", "parent_id": "other"}}
        )

        outcome = adopt_workspace(self.change_, "ws-9", self.requester, api=api)

        self.assertFalse(outcome.ok)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.workspace_network_id, "")

    def test_a_network_that_is_not_a_workspace_is_refused(self):
        # The source's own network has no parent: adopting it would scope a
        # change to production.
        api = FakeApi(
            networks={"net-1": {"id": "net-1", "name": "prod", "parent_id": None}}
        )

        self.assertFalse(
            adopt_workspace(self.change_, "net-1", self.requester, api=api).ok
        )

    def test_a_workspace_another_change_uses_is_refused(self):
        other = self.change(State.DRAFT, branch=None, workspace_network_id="ws-9")
        self.assertTrue(other.workspace_network_id)
        api = FakeApi(
            networks={"ws-9": {"id": "ws-9", "name": "t", "parent_id": "net-1"}}
        )

        outcome = adopt_workspace(self.change_, "ws-9", self.requester, api=api)

        self.assertFalse(outcome.ok)
        self.assertIn("Another change", outcome.message)

    def test_an_unknown_network_is_refused(self):
        self.assertFalse(
            adopt_workspace(self.change_, "nope", self.requester, api=FakeApi()).ok
        )


class RefreshingTest(ScopedChange):
    def setUp(self):
        super().setUp()
        self.api = FakeApi()
        create_workspace(self.change_, self.requester, api=self.api)
        self.change_.refresh_from_db()

    def test_the_baseline_is_the_workspaces_processed_import_snapshot(self):
        self.api.snapshots_by_network["ws-1"] = [snap("base", "IMPORT")]

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.before_snapshot_id, "base")

    def test_a_baseline_still_processing_is_not_recorded(self):
        self.api.snapshots_by_network["ws-1"] = [snap("base", "IMPORT", "PROCESSING")]

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.before_snapshot_id, "")

    def test_the_baseline_never_moves_once_recorded(self):
        # Approvals are anchored to it; a later import must not silently replace it.
        self.api.snapshots_by_network["ws-1"] = [snap("base", "IMPORT")]
        refresh_snapshots(self.change_, api=self.api)
        self.api.snapshots_by_network["ws-1"].append(snap("base-2", "IMPORT"))

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.before_snapshot_id, "base")

    def test_the_after_snapshot_is_a_collection_made_after_the_change_was_applied(self):
        applied = timezone.now() - timedelta(hours=2)
        self.change_.applied_at = applied
        self.change_.state = State.APPLIED
        self.change_.save()
        self.api.snapshots_by_network["ws-1"] = [
            snap("base", "IMPORT", created=applied - timedelta(days=1)),
            snap("too-early", "COLLECTION", created=applied - timedelta(hours=1)),
            snap("after", "COLLECTION", created=applied + timedelta(hours=1)),
        ]

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.after_snapshot_id, "after")

    def test_nothing_is_an_after_snapshot_before_the_change_is_applied(self):
        self.api.snapshots_by_network["ws-1"] = [
            snap("base", "IMPORT"),
            snap("c1", "COLLECTION"),
        ]

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.after_snapshot_id, "")

    def test_the_after_snapshot_stops_moving_once_a_verdict_exists(self):
        # A verdict was computed on the pinned snapshot; moving the pin would let
        # reviewers close on a verdict never computed for the snapshot they see.
        applied = timezone.now() - timedelta(hours=2)
        self.change_.applied_at = applied
        self.change_.after_snapshot_id = "c1"
        self.change_.state = State.VERIFIED_PROCEED
        self.change_.save()
        self.api.snapshots_by_network["ws-1"] = [
            snap("base", "IMPORT", created=applied - timedelta(days=1)),
            snap("c2", "COLLECTION", created=applied + timedelta(hours=1)),
        ]

        refresh_snapshots(self.change_, api=self.api)

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.after_snapshot_id, "c1")

    def test_a_change_with_no_workspace_is_left_alone(self):
        bare = self.change(State.DRAFT, branch=None)

        outcome = refresh_snapshots(bare, api=self.api)

        self.assertFalse(outcome.ok)
