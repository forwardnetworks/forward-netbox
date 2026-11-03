# A firewall context reports its chassis's management address, and NetBox allows
# one primary-IP owner per address object. With the opt-in parameter on, each such
# context gets its own copy of the address (role Anycast, tagged) on its own
# same-named interface and that copy is its primary IP. Real branch, synthetic data.
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import RequestFactory
from django.test import TransactionTestCase
from django.urls import reverse
from ipam.models import IPAddress
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.tests.test_primary_ip_integration import provision_branch
from forward_netbox.utilities.context_ip_copies import CONTEXT_COPY_TAG_SLUG
from forward_netbox.utilities.context_ip_copies import exclude_context_copies
from forward_netbox.utilities.merge import merge_branch
from forward_netbox.utilities.primary_ip import apply_primary_ip_from_mgmt_tags
from forward_netbox.utilities.primary_ip import parse_device_reason_lines


class ContextIpCopiesTest(TransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="ctx-copy-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        maker = Manufacturer.objects.create(name="Acme", slug="acme")
        kind = DeviceType.objects.create(manufacturer=maker, model="FW", slug="fw")
        role = DeviceRole.objects.create(name="Firewall", slug="firewall")
        site = Site.objects.create(name="Site 1", slug="site-1")

        def device(name):
            return Device.objects.create(
                name=name, device_type=kind, role=role, site=site, status="active"
            )

        self.parent = device("fw-parent")
        self.ctx1 = device("fw-ctx1")
        self.ctx2 = device("fw-ctx2")
        for each in (self.parent, self.ctx1, self.ctx2):
            Interface.objects.create(device=each, name="mgmt", type="virtual")
        self.original = IPAddress.objects.create(
            address="10.55.0.1/24",
            assigned_object=Interface.objects.get(device=self.parent, name="mgmt"),
        )
        self.source = ForwardSource.objects.create(
            name="ctx-source",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="ctx-sync",
            source=self.source,
            parameters={
                "snapshot_id": "snap-1",
                "set_primary_ip_from_forward_management_ip": True,
                "give_contexts_their_own_management_ip": True,
            },
        )

    def _executor(self, management_ips):
        patcher = patch(
            "forward_netbox.utilities.primary_ip.get_device_management_ips",
            return_value=management_ips,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return Mock(sync=self.sync, client=Mock(), user=self.user, logger=Mock())

    def _every_context_names_the_chassis_address(self):
        return {
            "fw-parent": ["10.55.0.1"],
            "fw-ctx1": ["10.55.0.1"],
            "fw-ctx2": ["10.55.0.1"],
        }

    def _run(self, name="Ctx Branch", branch=None, executor=None):
        branch = branch or provision_branch(user=self.user, name=name)
        executor = executor or self._executor(
            self._every_context_names_the_chassis_address()
        )
        apply_primary_ip_from_mgmt_tags(executor, branch, snapshot_id="snap-1")
        return branch, executor

    def _copies(self):
        return IPAddress.objects.filter(tags__slug=CONTEXT_COPY_TAG_SLUG)

    def test_each_context_gets_its_own_copy_as_its_primary_ip(self):
        branch, _executor = self._run()

        with activate_branch(branch):
            for context in (self.ctx1, self.ctx2):
                fresh = Device.objects.get(pk=context.pk)
                copy = IPAddress.objects.get(pk=fresh.primary_ip4_id)
                self.assertEqual(str(copy.address), "10.55.0.1/24")
                self.assertEqual(copy.role, "anycast")
                self.assertEqual(copy.assigned_object.device_id, context.pk)
                self.assertTrue(copy.tags.filter(slug=CONTEXT_COPY_TAG_SLUG).exists())
            # The owner keeps the synced row, now marked as shared.
            self.assertEqual(IPAddress.objects.get(pk=self.original.pk).role, "anycast")
            self.assertEqual(
                Device.objects.get(pk=self.parent.pk).primary_ip4_id, self.original.pk
            )
            self.assertEqual(self._copies().count(), 2)

    def test_the_copies_are_staged_in_the_branch_and_not_in_main(self):
        branch, _executor = self._run()

        self.assertEqual(self._copies().count(), 0)
        self.assertIsNone(Device.objects.get(pk=self.ctx1.pk).primary_ip4_id)
        ip_type = ContentType.objects.get_for_model(IPAddress)
        creates = [
            change
            for change in branch.get_unmerged_changes().filter(
                changed_object_type=ip_type, action="create"
            )
        ]
        self.assertEqual(len(creates), 2, "each copy is one create, never two")
        # The device pointing at its copy and the original's new role are merge-
        # eligible changes too.
        device_type = ContentType.objects.get_for_model(Device)
        pointed = {
            change.changed_object_id
            for change in branch.get_unmerged_changes().filter(
                changed_object_type=device_type, action="update"
            )
            if (change.postchange_data or {}).get("primary_ip4")
        }
        self.assertTrue({self.ctx1.pk, self.ctx2.pk} <= pointed)
        role_changes = [
            change
            for change in branch.get_unmerged_changes().filter(
                changed_object_type=ip_type,
                changed_object_id=self.original.pk,
                action="update",
            )
            if (change.postchange_data or {}).get("role") == "anycast"
        ]
        self.assertEqual(len(role_changes), 1)

    def test_a_rerun_makes_no_second_copy(self):
        branch, executor = self._run()

        self._run(branch=branch, executor=executor)

        with activate_branch(branch):
            self.assertEqual(self._copies().count(), 2)

    def test_off_by_default_leaves_the_context_without_a_primary_ip(self):
        self.sync.parameters = {
            **self.sync.parameters,
            "give_contexts_their_own_management_ip": False,
        }
        self.sync.save()

        branch, executor = self._run()

        with activate_branch(branch):
            self.assertIsNone(Device.objects.get(pk=self.ctx1.pk).primary_ip4_id)
            self.assertEqual(self._copies().count(), 0)
        logged = [c.args[0] for c in executor.logger.log_info.call_args_list]
        self.assertEqual(
            parse_device_reason_lines(logged)[self.ctx1.pk], ("shared", self.parent.pk)
        )

    def test_a_context_with_no_matching_interface_is_left_and_says_why(self):
        Interface.objects.filter(device=self.ctx2, name="mgmt").update(name="eth9")

        branch, executor = self._run()

        with activate_branch(branch):
            self.assertIsNotNone(Device.objects.get(pk=self.ctx1.pk).primary_ip4_id)
            self.assertIsNone(Device.objects.get(pk=self.ctx2.pk).primary_ip4_id)
        logged = [c.args[0] for c in executor.logger.log_info.call_args_list]
        self.assertEqual(
            parse_device_reason_lines(logged)[self.ctx2.pk][0],
            "context-copy-no-matching-interface",
        )

    def test_the_sync_apply_still_sees_one_row_per_address(self):
        branch, _executor = self._run()

        with activate_branch(branch):
            rows = exclude_context_copies(
                IPAddress.objects.filter(address__net_host="10.55.0.1")
            )
            self.assertEqual([row.pk for row in rows], [self.original.pk])
            # Three rows share the address, yet the sync's lookup is not ambiguous.
            self.assertEqual(
                IPAddress.objects.filter(address__net_host="10.55.0.1").count(), 3
            )
            from forward_netbox.utilities.sync import ForwardSyncRunner
            from forward_netbox.utilities.sync_primitives import get_unique_or_raise

            runner = ForwardSyncRunner(
                sync=self.sync, ingestion=None, client=None, logger_=Mock()
            )
            found = get_unique_or_raise(
                runner, IPAddress, {"address": "10.55.0.1/24", "vrf": None}
            )
            self.assertEqual(found.pk, self.original.pk)

    def test_no_device_name_reaches_the_log(self):
        _branch, executor = self._run()

        for call in executor.logger.log_info.call_args_list:
            self.assertNotIn("fw-ctx", call.args[0])

    def test_the_merge_replays_the_copies_into_main(self):
        # NetBox refuses a duplicate address unless every copy has a shared role,
        # so the original's role change must replay before the copies are created.
        branch, _executor = self._run()
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot",
            branch=branch,
        )

        merge_branch(ingestion, user=self.user)

        self.assertEqual(self._copies().count(), 2)
        for context in (self.ctx1, self.ctx2):
            context.refresh_from_db()
            copy = IPAddress.objects.get(pk=context.primary_ip4_id)
            self.assertEqual(copy.role, "anycast")
            self.assertEqual(copy.assigned_object.device_id, context.pk)
        self.original.refresh_from_db()
        self.assertEqual(self.original.role, "anycast")

    def test_a_primary_ip_set_by_hand_is_never_replaced(self):
        manual = IPAddress.objects.create(
            address="10.99.0.9/24",
            assigned_object=Interface.objects.get(device=self.ctx1, name="mgmt"),
        )
        self.ctx1.primary_ip4 = manual
        self.ctx1.save(update_fields=["primary_ip4"])

        branch, _executor = self._run()

        with activate_branch(branch):
            self.assertEqual(
                Device.objects.get(pk=self.ctx1.pk).primary_ip4_id, manual.pk
            )
            self.assertEqual(self._copies().count(), 1)

    def test_the_original_can_come_back_while_its_copies_exist(self):
        # NetBox refuses a role-less duplicate of an Anycast copy, so a returning
        # original is created Anycast or every run would fail on it.
        from forward_netbox.utilities.sync import ForwardSyncRunner
        from forward_netbox.utilities.sync_ipam import apply_ipam_ipaddress

        branch, _executor = self._run()
        ingestion = ForwardIngestion.objects.create(
            sync=self.sync,
            snapshot_selector="latestProcessed",
            snapshot_id="snapshot",
            branch=branch,
        )
        merge_branch(ingestion, user=self.user)
        IPAddress.objects.filter(pk=self.original.pk).delete()
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=None, client=None, logger_=Mock()
        )

        apply_ipam_ipaddress(
            runner,
            {
                "device": "fw-parent",
                "interface": "mgmt",
                "address": "10.55.0.1/24",
                "status": "active",
                "vrf": None,
            },
        )

        back = exclude_context_copies(
            IPAddress.objects.filter(address__net_host="10.55.0.1")
        ).get()
        self.assertEqual(back.role, "anycast")
        self.assertEqual(self._copies().count(), 2)

    def test_a_role_that_already_repeats_is_left_alone(self):
        IPAddress.objects.filter(pk=self.original.pk).update(role="vip")

        branch, _executor = self._run()

        with activate_branch(branch):
            self.assertEqual(IPAddress.objects.get(pk=self.original.pk).role, "vip")
            self.assertEqual(
                IPAddress.objects.get(
                    pk=Device.objects.get(pk=self.ctx1.pk).primary_ip4_id
                ).role,
                "vip",
            )
