from unittest import mock

from django.urls import reverse
from extras.models import JournalEntry
from rest_framework.test import APIClient
from users.models import Token

from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.workspace import collect_snapshot
from forward_netbox.change_control.workspace import collection_access
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.tests.test_change_control_actions import ChangeFixture
from forward_netbox.tests.test_change_control_comments import grant


class FakeForward:
    """Stands in for Forward: the account's roles, and what starting does."""

    def __init__(self, org=(), network=None, fail=None):
        self.org = list(org)
        self.network = network or {}
        self.fail = fail
        self.started = []

    def collection_access(self, network_id):
        return {"org_roles": self.org, "network_roles": self.network}

    def start_collection(self, network_id):
        if self.fail:
            raise self.fail
        self.started.append(network_id)
        return "task-9"


class AppliedChange(ChangeFixture):
    def setUp(self):
        super().setUp()
        self.change_ = self.change(State.APPLIED, workspace_network_id="ws-1")
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key="sw-a"
        )


class CollectSnapshotTest(AppliedChange):
    def test_nothing_starts_without_the_confirmation(self):
        forward = FakeForward(org=["ADMIN"])

        outcome = collect_snapshot(self.change_, self.requester, api=forward)

        self.assertFalse(outcome.ok)
        self.assertEqual(forward.started, [])

    def test_an_administrator_confirmed_starts_one_and_is_journaled(self):
        forward = FakeForward(org=["ADMIN"])

        outcome = collect_snapshot(
            self.change_, self.requester, confirmed=True, api=forward
        )

        self.assertTrue(outcome.ok, outcome.message)
        self.assertEqual(forward.started, ["ws-1"])
        entry = JournalEntry.objects.get(assigned_object_id=self.change_.pk)
        self.assertEqual(entry.created_by, self.requester)
        self.assertIn("task-9", entry.comments)
        self.assertIn("ws-1", entry.comments)

    def test_an_administrator_of_the_workspace_alone_is_enough(self):
        forward = FakeForward(network={"ws-1": "ADMIN"})

        self.assertTrue(
            collect_snapshot(
                self.change_, self.requester, confirmed=True, api=forward
            ).ok
        )

    def test_an_account_with_no_role_is_refused_and_nothing_starts(self):
        forward = FakeForward()

        outcome = collect_snapshot(
            self.change_, self.requester, confirmed=True, api=forward
        )

        self.assertFalse(outcome.ok)
        self.assertIn("no role", outcome.message)
        self.assertEqual(forward.started, [])
        self.assertFalse(JournalEntry.objects.exists())

    def test_a_workspace_operator_can_collect_on_the_workspace(self):
        forward = FakeForward(network={"ws-1": "WORKSPACE_OPERATOR"})

        self.assertTrue(
            collect_snapshot(
                self.change_, self.requester, confirmed=True, api=forward
            ).ok
        )
        self.assertEqual(forward.started, ["ws-1"])

    def test_the_roles_forward_says_cannot_collect_are_refused(self):
        # Forward's RBAC table: Collection Triggering is No for these.
        for role in ("OPERATOR", "READ_ONLY", "LIMITED_READ_ONLY"):
            forward = FakeForward(network={"ws-1": role})

            access = collection_access(self.change_, api=forward)
            outcome = collect_snapshot(
                self.change_, self.requester, confirmed=True, api=forward
            )

            self.assertIs(access.allowed, False, role)
            self.assertIn(role, access.reason)
            self.assertFalse(outcome.ok, role)
            self.assertEqual(forward.started, [], role)
        self.assertFalse(JournalEntry.objects.exists())

    def test_a_role_on_the_parent_alone_is_left_to_forward_even_an_admin(self):
        # net-1 is the source's network (see ChangeFixture). Forward copies parent
        # roles onto workspaces only when an organisation setting is on.
        for role in ("OPERATOR", "ADMIN"):
            access = collection_access(
                self.change_, api=FakeForward(network={"net-1": role})
            )

            self.assertIsNone(access.allowed, role)
            self.assertIn(role, access.reason)

    def test_the_workspace_role_wins_over_the_parents(self):
        forward = FakeForward(network={"net-1": "ADMIN", "ws-1": "READ_ONLY"})

        self.assertIs(collection_access(self.change_, api=forward).allowed, False)

    def test_a_role_name_not_in_the_table_is_left_to_forward_and_says_so(self):
        forward = FakeForward(network={"ws-1": "AUDITOR"})

        access = collection_access(self.change_, api=forward)

        self.assertIsNone(access.allowed)
        self.assertIn("AUDITOR", access.reason)
        self.assertTrue(
            collect_snapshot(
                self.change_, self.requester, confirmed=True, api=forward
            ).ok
        )

    def test_forwards_own_refusal_is_reported_not_raised(self):
        forward = FakeForward(org=["ADMIN"], fail=RuntimeError("not permitted"))

        outcome = collect_snapshot(
            self.change_, self.requester, confirmed=True, api=forward
        )

        self.assertFalse(outcome.ok)
        self.assertIn("not permitted", outcome.message)
        self.assertFalse(JournalEntry.objects.exists())

    def test_a_collected_change_can_collect_again(self):
        change = self.change(State.COLLECTED, workspace_network_id="ws-4")
        forward = FakeForward(org=["ADMIN"])

        self.assertTrue(
            collect_snapshot(change, self.requester, confirmed=True, api=forward).ok
        )

    def test_only_an_applied_change_with_a_workspace_can_collect(self):
        forward = FakeForward(org=["ADMIN"])
        for state in (State.DRAFT, State.STAGED, State.CLOSED):
            change = self.change(state, workspace_network_id="ws-2")
            self.assertFalse(
                collect_snapshot(change, self.requester, confirmed=True, api=forward).ok
            )
        no_workspace = self.change(State.APPLIED)
        self.assertFalse(
            collect_snapshot(
                no_workspace, self.requester, confirmed=True, api=forward
            ).ok
        )
        self.assertEqual(forward.started, [])


class RolesAsForwardSendsThemTest(AppliedChange):
    """The real role reading, against the shapes Forward has been seen to send."""

    def access(self, roles=None, raises=None):
        from forward_netbox.change_control.workspace import ForwardWorkspaceApi

        api = ForwardWorkspaceApi.__new__(ForwardWorkspaceApi)
        client = mock.Mock()
        api.client = client
        client._call_sdk.side_effect = lambda call, *a, **k: call(*a, **k)
        if raises:
            client._sdk_client.user_accounts.get_current_user.side_effect = raises
        else:
            client._sdk_client.user_accounts.get_current_user.return_value = mock.Mock(
                roles=roles
            )
        return collection_access(self.change_, api=api)

    def test_a_bare_string_org_role_is_an_administrator(self):
        self.assertTrue(self.access({"org": "ADMIN", "network": {}}).allowed)

    def test_a_lowercase_role_is_still_an_administrator(self):
        self.assertTrue(self.access({"org": ["admin"], "network": None}).allowed)

    def test_an_empty_answer_is_unknown_not_a_refusal(self):
        for roles in (None, {}, {"org": None, "network": None}):
            access = self.access(roles)
            self.assertIsNone(access.allowed, roles)
            self.assertIn("no role information", access.reason)

    def test_roles_that_are_present_but_empty_are_a_refusal(self):
        self.assertIs(self.access({"org": [], "network": {}}).allowed, False)

    def test_an_account_that_cannot_read_its_roles_is_left_to_forward(self):
        access = self.access(raises=RuntimeError("unavailable"))

        self.assertIsNone(access.allowed)
        self.assertIn("unavailable", access.reason)


class CollectPageTest(AppliedChange):
    def setUp(self):
        super().setUp()
        grant(self.reviewer, type(self.change_), "view", "change")
        self.client.force_login(self.reviewer)
        self.url = reverse(
            "plugins:forward_netbox:forwardchange_workspace_collect",
            args=[self.change_.pk],
        )

    def patched(self, forward):
        return mock.patch(
            "forward_netbox.change_control.workspace.ForwardWorkspaceApi",
            return_value=forward,
        )

    def test_the_page_warns_and_starts_nothing(self):
        forward = FakeForward(org=["ADMIN"])

        with self.patched(forward):
            response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "logs in to each device")
        self.assertContains(response, "administrator")
        self.assertEqual(forward.started, [])

    def test_the_page_offers_no_button_to_an_account_that_cannot_collect(self):
        with self.patched(FakeForward()):
            response = self.client.get(self.url)

        self.assertContains(response, "cannot start a collection")
        self.assertNotContains(response, "Start collection")

    def test_posting_without_the_box_ticked_starts_nothing(self):
        forward = FakeForward(org=["ADMIN"])

        with self.patched(forward):
            self.client.post(self.url, {})

        self.assertEqual(forward.started, [])

    def test_posting_with_the_box_ticked_starts_one(self):
        forward = FakeForward(org=["ADMIN"])

        with self.patched(forward):
            response = self.client.post(self.url, {"confirm": "on"})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(forward.started, ["ws-1"])

    def test_a_user_who_may_only_view_cannot_even_see_the_page(self):
        other = self.requester
        grant(other, type(self.change_), "view")
        self.client.force_login(other)

        forward = FakeForward(org=["ADMIN"])
        with self.patched(forward):
            self.assertEqual(self.client.get(self.url).status_code, 403)
            self.assertEqual(
                self.client.post(self.url, {"confirm": "on"}).status_code, 403
            )

        self.assertEqual(forward.started, [])
        self.assertFalse(JournalEntry.objects.exists())

    def test_a_change_not_yet_applied_is_sent_back(self):
        change = self.change(State.STAGED, workspace_network_id="ws-3")
        url = reverse(
            "plugins:forward_netbox:forwardchange_workspace_collect", args=[change.pk]
        )

        with self.patched(FakeForward(org=["ADMIN"])):
            response = self.client.get(url)

        self.assertEqual(response.status_code, 302)


class CollectApiTest(AppliedChange):
    def setUp(self):
        super().setUp()
        grant(self.reviewer, type(self.change_), "view", "change")
        token = Token.objects.create(user=self.reviewer, write_enabled=True)
        self.api = APIClient()
        self.api.force_authenticate(user=self.reviewer, token=token)
        self.url = reverse(
            "plugins-api:forward_netbox-api:forwardchange-workspace-collect",
            args=[self.change_.pk],
        )

    def post(self, body, forward):
        with mock.patch(
            "forward_netbox.change_control.workspace.ForwardWorkspaceApi",
            return_value=forward,
        ):
            return self.api.post(self.url, body, format="json")

    def test_the_confirmation_is_required_in_the_body(self):
        forward = FakeForward(org=["ADMIN"])

        self.assertEqual(self.post({}, forward).status_code, 400)
        self.assertEqual(forward.started, [])

    def test_a_false_confirmation_starts_nothing(self):
        forward = FakeForward(org=["ADMIN"])

        response = self.post({"confirm": False}, forward)

        self.assertFalse(response.json()["ok"])
        self.assertEqual(forward.started, [])

    def test_a_true_confirmation_starts_one(self):
        forward = FakeForward(org=["ADMIN"])

        response = self.post({"confirm": True}, forward)

        self.assertTrue(response.json()["ok"], response.content)
        self.assertEqual(forward.started, ["ws-1"])
