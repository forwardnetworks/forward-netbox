from forward_netbox.change_control import criteria_admin
from forward_netbox.change_control.choices import ForwardChangeStateChoices as State
from forward_netbox.change_control.criteria import BindResult
from forward_netbox.change_control.scope import add_devices
from forward_netbox.change_control.scope import match_forward_names
from forward_netbox.change_control.scope import parse_device_names
from forward_netbox.change_control.scope import remove_device
from forward_netbox.change_control.scope import resolve_forward_names
from forward_netbox.forms import ForwardChangeCriterionForm
from forward_netbox.forms import ForwardChangeForm
from forward_netbox.models import ForwardChangeCriterion
from forward_netbox.models import ForwardChangeDevice
from forward_netbox.models import ForwardChangeEvidence
from forward_netbox.models import ForwardIngestion
from forward_netbox.tests.test_change_control_actions import ChangeFixture


class EstateApi:
    def __init__(self, names=None, error=None):
        self.names = names or []
        self.error = error
        self.calls = 0

    def estate_device_names(self, network_id):
        self.calls += 1
        if self.error:
            raise self.error
        return list(self.names)


class ParseAndMatchTest(ChangeFixture):
    def test_names_are_split_on_lines_and_commas_and_deduplicated_ignoring_case(self):
        self.assertEqual(
            parse_device_names("Sw-A, sw-b\n\n sw-a \nSW-C"), ["Sw-A", "sw-b", "SW-C"]
        )

    def test_only_an_exact_unique_case_insensitive_match_resolves(self):
        result = match_forward_names(
            ["sw-a", "sw-b", "sw-c"], ["SW-A", "sw-b", "SW-B", "other"]
        )
        self.assertEqual(result["sw-a"], ("SW-A", ""))
        self.assertEqual(result["sw-b"][0], "")  # two Forward devices differ by case
        self.assertIn("several", result["sw-b"][1])
        self.assertEqual(result["sw-c"][0], "")  # no fallback to the NetBox name
        self.assertIn("no device", result["sw-c"][1])


class AddRemoveDevicesTest(ChangeFixture):
    def test_devices_are_added_by_name_and_the_scope_is_journalled(self):
        change = self.change()
        device = self.device()

        outcome = add_devices(change, [device.name.upper()], self.requester)

        self.assertTrue(outcome.ok)
        self.assertEqual(change.devices.get().device, device)

    def test_an_unknown_name_adds_nothing_at_all(self):
        change = self.change()
        device = self.device()

        outcome = add_devices(change, [device.name, "nope"], self.requester)

        self.assertFalse(outcome.ok)
        self.assertEqual(change.devices.count(), 0)
        self.assertIn("nope", " ".join(outcome.problems))

    def test_a_name_held_by_two_devices_is_refused(self):
        change = self.change()
        first = self.device()
        second = self.device()
        second.name = first.name.upper()
        second.save()

        outcome = add_devices(change, [first.name], self.requester)

        self.assertFalse(outcome.ok)
        self.assertEqual(change.devices.count(), 0)

    def test_devices_cannot_change_once_a_workspace_exists(self):
        change = self.change(workspace_network_id="ws-1", workspace_origin="created")
        row = ForwardChangeDevice.objects.create(change=change, device=self.device())

        self.assertFalse(add_devices(change, [self.device().name], self.requester).ok)
        self.assertFalse(remove_device(change, row.pk, self.requester).ok)
        self.assertEqual(change.devices.count(), 1)

    def test_devices_cannot_change_after_the_change_is_applied(self):
        change = self.change(State.APPLIED)

        self.assertFalse(add_devices(change, [self.device().name], self.requester).ok)

    def test_a_device_can_be_removed_before_a_workspace_exists(self):
        change = self.change()
        row = ForwardChangeDevice.objects.create(change=change, device=self.device())

        self.assertTrue(remove_device(change, row.pk, self.requester).ok)
        self.assertEqual(change.devices.count(), 0)


class ResolveNamesTest(ChangeFixture):
    def test_one_call_resolves_every_device_and_unmatched_ones_stay_blank(self):
        change = self.change()
        found, missing = self.device(), self.device()
        for device in (found, missing):
            ForwardChangeDevice.objects.create(change=change, device=device)
        api = EstateApi([found.name.upper(), "unrelated"])

        outcome = resolve_forward_names(change, self.requester, api=api)

        self.assertTrue(outcome.ok)
        self.assertEqual(api.calls, 1)
        keys = {r.device_id: r.forward_device_key for r in change.devices.all()}
        self.assertEqual(keys[found.pk], found.name.upper())
        self.assertEqual(keys[missing.pk], "")
        self.assertIn(missing.name, " ".join(outcome.problems))

    def test_a_name_that_disappears_from_forward_is_cleared(self):
        change = self.change()
        device = self.device()
        ForwardChangeDevice.objects.create(
            change=change, device=device, forward_device_key="gone"
        )

        resolve_forward_names(change, self.requester, api=EstateApi(["other"]))

        self.assertEqual(change.devices.get().forward_device_key, "")

    def test_a_forward_failure_is_reported_and_changes_nothing(self):
        change = self.change()
        ForwardChangeDevice.objects.create(
            change=change, device=self.device(), forward_device_key="keep"
        )

        outcome = resolve_forward_names(
            change, self.requester, api=EstateApi(error=RuntimeError("boom"))
        )

        self.assertFalse(outcome.ok)
        self.assertEqual(change.devices.get().forward_device_key, "keep")

    def test_it_is_refused_once_a_workspace_exists(self):
        change = self.change(workspace_network_id="ws-1")
        ForwardChangeDevice.objects.create(change=change, device=self.device())

        self.assertFalse(
            resolve_forward_names(change, self.requester, api=EstateApi()).ok
        )


class CriteriaAdminTest(ChangeFixture):
    def criterion(self, change, **extra):
        values = {"change": change, "name": "no-isolated-hosts", "query_path": "/a/b"}
        values.update(extra)
        return ForwardChangeCriterion.objects.create(**values)

    def test_binding_pins_the_version_the_path_resolved_to(self):
        change = self.change()
        criterion = self.criterion(change)
        original = criteria_admin.bind_criterion
        criteria_admin.bind_criterion = lambda c, client: BindResult(
            True, query_id="Q1", commit_id="C1", source_sha256="abc"
        )
        self.addCleanup(setattr, criteria_admin, "bind_criterion", original)

        outcome = criteria_admin.bind(
            change, criterion.pk, self.requester, client=object()
        )

        criterion.refresh_from_db()
        self.assertTrue(outcome.ok)
        self.assertEqual((criterion.query_id, criterion.commit_id), ("Q1", "C1"))

    def test_a_failed_bind_leaves_the_criterion_unbound(self):
        change = self.change()
        criterion = self.criterion(change)
        original = criteria_admin.bind_criterion
        criteria_admin.bind_criterion = lambda c, client: BindResult(
            False, error="no such query"
        )
        self.addCleanup(setattr, criteria_admin, "bind_criterion", original)

        outcome = criteria_admin.bind(
            change, criterion.pk, self.requester, client=object()
        )

        criterion.refresh_from_db()
        self.assertFalse(outcome.ok)
        self.assertIn("no such query", outcome.message)
        self.assertFalse(criterion.is_bound)

    def test_a_criterion_with_evidence_cannot_be_rebound_or_deleted(self):
        change = self.change(State.BASELINED)
        criterion = self.criterion(change)
        ForwardChangeEvidence.objects.create(
            criterion=criterion, phase="before", passed=True, snapshot_id="s1"
        )

        self.assertFalse(
            criteria_admin.bind(
                change, criterion.pk, self.requester, client=object()
            ).ok
        )
        self.assertFalse(criteria_admin.delete(change, criterion.pk, self.requester).ok)
        self.assertTrue(ForwardChangeCriterion.objects.filter(pk=criterion.pk).exists())

    def test_criteria_are_frozen_from_baselined_on(self):
        change = self.change(State.BASELINED)
        criterion = self.criterion(change)

        self.assertFalse(criteria_admin.delete(change, criterion.pk, self.requester).ok)

    def test_a_criterion_of_another_change_is_not_reachable(self):
        mine, other = self.change(), self.change()
        criterion = self.criterion(other)

        self.assertFalse(criteria_admin.delete(mine, criterion.pk, self.requester).ok)

    def test_a_criterion_can_be_deleted_while_planning(self):
        change = self.change()
        criterion = self.criterion(change)

        self.assertTrue(criteria_admin.delete(change, criterion.pk, self.requester).ok)

    def test_changing_the_path_of_a_bound_criterion_drops_its_binding(self):
        change = self.change()
        criterion = self.criterion(
            change, query_id="Q1", commit_id="C1", source_sha256="abc"
        )
        form = ForwardChangeCriterionForm(
            data={
                "change": change.pk,
                "name": criterion.name,
                "family": criterion.family,
                "expectation": criterion.expectation,
                "blocking": "on",
                "query_path": "/a/other",
            },
            instance=criterion,
        )

        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()

        self.assertEqual(
            (saved.query_id, saved.commit_id, saved.source_sha256), ("", "", "")
        )

    def test_the_form_refuses_a_criterion_once_evidence_exists(self):
        change = self.change(State.BASELINED)
        criterion = self.criterion(change)
        ForwardChangeEvidence.objects.create(
            criterion=criterion, phase="before", passed=True, snapshot_id="s1"
        )
        form = ForwardChangeCriterionForm(
            data={
                "change": change.pk,
                "name": criterion.name,
                "family": criterion.family,
                "expectation": criterion.expectation,
                "blocking": "on",
                "query_path": "/x",
            },
            instance=criterion,
        )

        self.assertFalse(form.is_valid())

    def test_the_form_requires_a_query_path(self):
        change = self.change()
        form = ForwardChangeCriterionForm(
            data={
                "change": change.pk,
                "name": "n",
                "family": "acceptance",
                "expectation": "no-rows",
                "blocking": "on",
                "query_path": "",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("query_path", form.errors)


class ChangeFormBranchTest(ChangeFixture):
    def data(self, **extra):
        values = {
            "title": "t",
            "source": self.source.pk,
            "priority": 3,
            "branch": self.branch.pk,
        }
        values.update(extra)
        return values

    def test_a_branch_a_sync_owns_cannot_back_a_change(self):
        ForwardIngestion.objects.create(sync=self.sync_for_branch(), branch=self.branch)

        form = ForwardChangeForm(data=self.data())

        self.assertFalse(form.is_valid())
        self.assertIn("branch", form.errors)

    def test_a_free_branch_is_accepted(self):
        self.assertTrue(ForwardChangeForm(data=self.data()).is_valid())

    def test_the_branch_cannot_be_swapped_after_staging(self):
        from netbox_branching.choices import BranchStatusChoices
        from netbox_branching.models import Branch

        change = self.change(State.STAGED, branch=self.branch)
        other = Branch.objects.create(
            name="other-branch",
            schema_id="other_schema_1",
            status=BranchStatusChoices.READY,
        )

        form = ForwardChangeForm(data=self.data(branch=other.pk), instance=change)

        self.assertFalse(form.is_valid())
        self.assertIn("branch", form.errors)

    def sync_for_branch(self):
        from forward_netbox.models import ForwardSync

        return ForwardSync.objects.create(name="s-branch", source=self.source)


class ManualTransitionsTest(ChangeFixture):
    def test_a_person_cannot_move_a_change_to_a_verdict_collected_or_closed(self):
        from forward_netbox.change_control.state_machine import manual_transitions

        for state in (State.APPLIED, State.COLLECTED, State.APPROVED):
            offered = set(manual_transitions(state))
            self.assertFalse(
                offered
                & {
                    State.COLLECTED,
                    State.VERIFIED_PROCEED,
                    State.VERIFIED_HOLD,
                    State.CLOSED,
                    State.APPLIED,
                },
                state,
            )

    def test_a_hold_can_go_back_to_applied_by_hand(self):
        from forward_netbox.change_control.state_machine import manual_transitions

        self.assertIn(State.APPLIED, manual_transitions(State.VERIFIED_HOLD))
        self.assertNotIn(State.CLOSED, manual_transitions(State.VERIFIED_HOLD))


class ScopeViewsTest(ChangeFixture):
    """The buttons on the change page, which only collect the request."""

    def setUp(self):
        super().setUp()
        from django.urls import reverse

        from forward_netbox.tests.test_change_control_comments import grant

        self.reverse = reverse
        self.change_ = self.change(State.SCOPED)
        for action in ("view", "change"):
            grant(self.reviewer, type(self.change_), action)
        for action in ("view", "add", "change", "delete"):
            grant(self.reviewer, ForwardChangeCriterion, action)
        self.client.force_login(self.reviewer)

    def url(self, name, *args):
        return self.reverse(f"plugins:forward_netbox:forwardchange_{name}", args=args)

    def test_removing_a_device_works_from_the_page(self):
        row = ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device()
        )

        response = self.client.post(self.url("devices_remove", self.change_.pk, row.pk))

        self.assertEqual(response.status_code, 302)
        self.assertFalse(ForwardChangeDevice.objects.filter(pk=row.pk).exists())

    def test_deleting_a_criterion_works_from_the_page(self):
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/q"
        )

        response = self.client.post(
            self.url("criterion_delete", self.change_.pk, criterion.pk)
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(
            ForwardChangeCriterion.objects.filter(pk=criterion.pk).exists()
        )

    def test_binding_needs_the_criterion_change_permission(self):
        from forward_netbox.tests.test_change_control_comments import grant

        other = type(self.reviewer).objects.create(username="nobind")
        grant(other, type(self.change_), "view")
        grant(other, type(self.change_), "change")
        self.client.force_login(other)
        criterion = ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/q"
        )

        response = self.client.post(
            self.url("criterion_bind", self.change_.pk, criterion.pk)
        )

        self.assertEqual(response.status_code, 403)

    def test_adding_devices_from_the_page(self):
        device = self.device()

        response = self.client.post(
            self.url("devices_add", self.change_.pk), {"names": device.name}
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.change_.devices.get().device, device)

    def test_the_transition_button_refuses_a_verdict_state(self):
        self.change_.state = State.COLLECTED
        self.change_.save()

        response = self.client.post(
            self.url("transition", self.change_.pk),
            {"target": State.VERIFIED_PROCEED},
        )

        self.assertEqual(response.status_code, 302)
        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.COLLECTED)

    def test_the_transition_button_moves_a_change_that_may_move(self):
        ForwardChangeDevice.objects.create(
            change=self.change_, device=self.device(), forward_device_key="sw"
        )
        ForwardChangeCriterion.objects.create(
            change=self.change_,
            name="c",
            query_path="/q",
            query_id="q",
            commit_id="c",
        )
        self.change_.state = State.DRAFT
        self.change_.save()

        self.client.post(
            self.url("transition", self.change_.pk), {"target": State.SCOPED}
        )

        self.change_.refresh_from_db()
        self.assertEqual(self.change_.state, State.SCOPED)

    def test_the_change_page_renders_with_its_new_cards(self):
        ForwardChangeDevice.objects.create(change=self.change_, device=self.device())
        ForwardChangeCriterion.objects.create(
            change=self.change_, name="c", query_path="/q"
        )

        response = self.client.get(self.change_.get_absolute_url())

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Resolve Forward names")
        self.assertContains(response, "Add criterion")

    def test_the_criterion_add_page_renders(self):
        response = self.client.get(
            self.reverse("plugins:forward_netbox:forwardchangecriterion_add")
            + f"?change={self.change_.pk}"
        )

        self.assertEqual(response.status_code, 200)

    def test_a_criterion_cannot_be_saved_through_the_form_once_staged(self):
        self.change_.state = State.STAGED
        self.change_.save()

        response = self.client.post(
            self.reverse("plugins:forward_netbox:forwardchangecriterion_add"),
            {
                "change": self.change_.pk,
                "name": "late",
                "family": "acceptance",
                "expectation": "no-rows",
                "blocking": "on",
                "query_path": "/q",
            },
        )

        self.assertEqual(response.status_code, 200)  # form re-rendered with the error
        self.assertFalse(ForwardChangeCriterion.objects.filter(name="late").exists())
