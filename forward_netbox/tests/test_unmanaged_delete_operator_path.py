# What an operator sees when the verified delete of unmanaged devices holds some
# of them back. A customer ran it on 66 devices and nothing happened: the job
# reported success, the page showed nothing and the bundle could not say why.
import uuid
from unittest.mock import Mock
from unittest.mock import patch

from core.choices import JobStatusChoices
from core.models import Job
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.test import Client
from django.test import TestCase
from django.urls import reverse

from forward_netbox import views
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.cleanup_checklist import cleanup_items

OSPF_INTERFACE = "netbox_routing.ospfinterface"


def _ospf_on(device):
    from netbox_routing.models import OSPFArea
    from netbox_routing.models import OSPFInstance
    from netbox_routing.models import OSPFInterface

    interface = Interface.objects.create(device=device, name="Gi0/0", type="1000base-t")
    instance = OSPFInstance.objects.create(
        name=f"{device.name}-ospf", router_id="10.0.0.1", process_id=1, device=device
    )
    area = OSPFArea.objects.create(area_id="0.0.0.0", area_type="standard")
    OSPFInterface.objects.create(instance=instance, area=area, interface=interface)


class _Base(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="op-src",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={"network_id": "net-1"},
        )
        self.sync = ForwardSync.objects.create(
            name="op-sync", source=source, parameters={}
        )
        maker = Manufacturer.objects.create(name="op-mfr", slug="op-mfr")
        self.kind = DeviceType.objects.create(manufacturer=maker, model="op", slug="op")
        self.role = DeviceRole.objects.create(name="op-role", slug="op-role")
        self.site = Site.objects.create(name="op-site", slug="op-site")
        self.admin = get_user_model().objects.create_superuser(
            username="op-admin", password="x"
        )
        self.client = Client()
        self.client.force_login(self.admin)

    def _device(self, name):
        return Device.objects.create(
            name=name, device_type=self.kind, role=self.role, site=self.site
        )

    def _job(self, **data):
        return Job.objects.create(
            job_id=uuid.uuid4(),
            object_type=ContentType.objects.get_for_model(ForwardSync),
            object_id=self.sync.pk,
            name="prune unmanaged devices",
            status=JobStatusChoices.STATUS_COMPLETED,
            data=data,
        )


class ConfirmationPageTest(_Base):
    def setUp(self):
        super().setUp()
        self.url = (
            reverse("plugins:forward_netbox:device_list_prune_unmanaged")
            + f"?forward_sync={self.sync.pk}&forward_list=unclaimed_absent"
        )

    def _post(self, devices, extra=None):
        data = {"pk": [d.pk for d in devices]}
        data.update(extra or {})
        return self.client.post(self.url, data)

    def test_the_page_names_what_holds_a_device_and_offers_the_routing_box(self):
        held = self._device("op-held")
        plain = self._device("op-plain")
        _ospf_on(held)

        content = self._post([held, plain]).content.decode()

        self.assertIn("block the delete", content)
        self.assertIn("netbox_routing.ospfinterface", content)
        self.assertIn('name="release_routing"', content)
        self.assertIn("Held by", content)

    def test_a_selection_nothing_holds_gets_no_box(self):
        content = self._post(
            [self._device("op-a"), self._device("op-b")]
        ).content.decode()

        self.assertNotIn('name="release_routing"', content)
        self.assertNotIn("block the delete", content)

    def test_the_box_sends_release_routing_to_the_job(self):
        held = self._device("op-held")
        _ospf_on(held)
        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_button_job",
            return_value=Mock(pk=5),
        ) as queue:
            self._post(
                [held], {"_confirm": "1", "confirm_count": "1", "release_routing": "1"}
            )

        kwargs = queue.call_args.kwargs["job_kwargs"]
        self.assertEqual(kwargs["restrict_to_device_pks"], [held.pk])
        self.assertTrue(kwargs["release_routing"])

    def test_without_the_box_the_job_is_not_told_to_release_routing(self):
        held = self._device("op-held")
        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_button_job",
            return_value=Mock(pk=5),
        ) as queue:
            self._post([held], {"_confirm": "1", "confirm_count": "1"})

        self.assertNotIn("release_routing", queue.call_args.kwargs["job_kwargs"])

    def test_the_queued_message_says_where_the_result_will_be(self):
        held = self._device("op-held")
        with patch(
            "forward_netbox.utilities.sync_facade.enqueue_button_job",
            return_value=Mock(pk=5),
        ):
            response = self.client.post(
                self.url,
                {"pk": [held.pk], "_confirm": "1", "confirm_count": "1"},
                follow=True,
            )

        self.assertContains(response, "Scope Reconciliation")


class LastResultPayloadTest(_Base):
    def test_no_job_is_not_available(self):
        self.assertFalse(views._unmanaged_delete_payload(self.sync)["available"])

    def test_held_devices_are_reported_with_the_next_step(self):
        held = self._device("op-held")
        self._job(
            pruned_device_count=2,
            protected_device_count=1,
            refused_device_pks_by_reason={"not_absent": [999]},
            protected_by_device={str(held.pk): {OSPF_INTERFACE: 2}},
            release_routing=False,
        )

        payload = views._unmanaged_delete_payload(self.sync, with_names=True)

        self.assertTrue(payload["available"])
        self.assertEqual(payload["values"]["deleted"], 2)
        self.assertEqual(payload["values"]["held_by_routing"], 1)
        self.assertEqual(payload["values"]["still_in_netbox"], 1)
        self.assertEqual(payload["values"]["refused"], 1)
        self.assertEqual(payload["next_step"], "release_routing")
        self.assertEqual(payload["held_rows"][0]["device"], held)

    def test_the_bundle_form_names_pks_and_models_never_devices(self):
        held = self._device("op-held")
        self._job(
            pruned_device_count=0,
            protected_by_device={str(held.pk): {OSPF_INTERFACE: 1}},
        )

        payload = views._unmanaged_delete_payload(self.sync)

        self.assertEqual(
            payload["held"], [{"pk": held.pk, "held_by": {OSPF_INTERFACE: 1}}]
        )
        self.assertEqual(payload["held_rows"], [])
        self.assertNotIn("op-held", str(payload))

    def test_a_job_from_before_the_per_device_record_is_not_read_as_nothing_held(self):
        # Older job data has counts and no protected_by_device. Missing detail is
        # not "nothing was held": the card must say it was not recorded.
        self._job(
            pruned_device_count=0,
            protected_device_count=25,
            protected_by_model={"netbox_routing.bgpscope": 20, OSPF_INTERFACE: 10},
        )

        payload = views._unmanaged_delete_payload(self.sync)

        self.assertTrue(payload["legacy"])
        self.assertEqual(payload["next_step"], "legacy")
        self.assertEqual(payload["legacy_held_count"], 25)
        self.assertNotEqual(payload["values"]["still_in_netbox"], 0)
        self.assertIn("not recorded", str(payload["values"]["held_by_routing"]))
        self.assertEqual(payload["legacy_held_by_model"]["netbox_routing.bgpscope"], 20)

    def test_devices_over_the_cap_are_their_own_figure_not_refusals(self):
        self._job(
            pruned_device_count=100,
            protected_by_device={},
            refused_device_pks_by_reason={"over_cap": [1, 2, 3], "not_absent": [9]},
        )

        payload = views._unmanaged_delete_payload(self.sync)

        self.assertEqual(payload["values"]["over_cap"], 3)
        self.assertEqual(payload["values"]["refused"], 1)

    def test_the_figures_are_in_ui_figures_under_the_cards_labels(self):
        self._job(pruned_device_count=3)

        payload = views._unmanaged_delete_payload(self.sync)

        labels = [item["label"] for item in payload["ui_figures"]]
        self.assertEqual(labels, [label for label, _ in views.UNMANAGED_DELETE_FIGURES])
        self.assertTrue(
            all(
                item["card"] == views.UNMANAGED_DELETE_CARD
                for item in payload["ui_figures"]
            )
        )

    def test_devices_that_are_gone_are_no_longer_held(self):
        gone = self._device("op-gone")
        pk = gone.pk
        self._job(
            pruned_device_count=0, protected_by_device={str(pk): {OSPF_INTERFACE: 1}}
        )
        gone.delete()

        payload = views._unmanaged_delete_payload(self.sync)

        self.assertEqual(payload["values"]["still_in_netbox"], 0)
        self.assertEqual(payload["next_step"], "")

    def test_after_the_routing_option_ran_a_held_device_points_at_other_records(self):
        held = self._device("op-held")
        self._job(
            pruned_device_count=0,
            protected_by_device={str(held.pk): {OSPF_INTERFACE: 1}},
            release_routing=True,
        )

        payload = views._unmanaged_delete_payload(self.sync)

        self.assertEqual(payload["next_step"], "other_records")

    def test_the_page_shows_the_card_the_held_device_and_what_to_do(self):
        held = self._device("op-held")
        self._job(
            pruned_device_count=0,
            protected_by_device={str(held.pk): {OSPF_INTERFACE: 1}},
        )

        response = self.client.get(
            reverse(
                "plugins:forward_netbox:forwardsync_scope_reconciliation",
                kwargs={"pk": self.sync.pk},
            )
        )

        self.assertContains(response, "Unmanaged Delete: Last Result")
        self.assertContains(response, "op-held")
        self.assertContains(response, "tick the box to also delete the routing records")
        self.assertContains(response, "Unmanaged devices the last delete held back")


class ChecklistTest(TestCase):
    def test_held_devices_get_their_own_item_with_the_right_steps(self):
        items = cleanup_items(
            {},
            unmanaged_held={
                "held_devices": 3,
                "freed_by_routing_option": 2,
                "needs_other_records": 1,
            },
        )

        item = next(i for i in items if i["key"] == "unmanaged_delete_held")
        self.assertEqual(item["count"], 3)
        text = " ".join(item["steps"])
        self.assertIn("2 are held only by routing records", text)
        self.assertIn("1 are held by other records", text)

    def test_nothing_held_adds_no_item(self):
        self.assertEqual(
            [
                i
                for i in cleanup_items({}, unmanaged_held={"held_devices": 0})
                if i["key"] == "unmanaged_delete_held"
            ],
            [],
        )

    def test_the_unmanaged_item_tells_the_operator_what_a_stuck_device_means(self):
        items = cleanup_items(
            {"unmanaged": {"unclaimed_absent_device_ids": [1, 2]}}, urls={}
        )

        item = next(i for i in items if i["key"] == "unmanaged_not_in_forward")
        self.assertIn("something holds it", " ".join(item["steps"]))


class BundleSurveyTest(_Base):
    def _survey(self, report):
        with (
            patch.object(views, "_latest_scope_reconciliation_job", return_value=None),
            patch.object(
                views,
                "_scope_reconciliation_payload",
                return_value=(report, None, None),
            ),
        ):
            return views._delete_blocker_survey_bundle_payload(self.sync)

    def test_the_unmanaged_group_is_surveyed_and_its_blockers_are_named(self):
        held = self._device("op-held")
        _ospf_on(held)

        survey = self._survey({"unmanaged": {"unclaimed_absent_device_ids": [held.pk]}})

        group = survey["by_group"]["unmanaged_not_in_forward"]
        self.assertEqual(group["target"], 1)
        self.assertEqual(group["devices_with_blockers"], 1)
        self.assertIn(OSPF_INTERFACE, {k.lower() for k in group["blockers"]})

    def test_a_large_first_group_does_not_use_up_the_unmanaged_samples(self):
        held = self._device("op-held")
        _ospf_on(held)
        others = [self._device(f"op-o{index}").pk for index in range(30)]

        survey = self._survey(
            {
                "out_of_scope_device_ids": others,
                "unmanaged": {"unclaimed_absent_device_ids": [held.pk]},
            }
        )

        self.assertEqual(survey["by_group"]["unmanaged_not_in_forward"]["sampled"], 1)
        self.assertEqual(survey["by_group"]["out_of_scope"]["target"], 30)

    def test_the_unmanaged_group_survives_a_spent_budget(self):
        held = self._device("op-held")
        others = [self._device(f"op-b{index}").pk for index in range(3)]

        with patch.object(views, "_SUPPORT_BUNDLE_BLOCKER_BUDGET_SECONDS", 0.0):
            survey = self._survey(
                {
                    "out_of_scope_device_ids": others,
                    "unmanaged": {"unclaimed_absent_device_ids": [held.pk]},
                }
            )

        # Out of time, but the group is still reported, with its size and why
        # it was not checked, instead of silently missing.
        group = survey["by_group"]["unmanaged_not_in_forward"]
        self.assertEqual(group["target"], 1)
        self.assertTrue(group["stopped_on_time_budget"])

    def test_the_survey_carries_no_device_names(self):
        held = self._device("op-held")
        _ospf_on(held)

        survey = self._survey({"unmanaged": {"unclaimed_absent_device_ids": [held.pk]}})

        self.assertNotIn("op-held", str(survey))


class ApiRoutingOptionTest(_Base):
    def _post(self, data):
        from rest_framework.test import APIRequestFactory
        from rest_framework.test import force_authenticate

        from forward_netbox.api.views import ForwardSyncViewSet

        request = APIRequestFactory().post(
            f"/api/plugins/forward/sync/{self.sync.pk}/prune-unmanaged/",
            data=data,
            format="json",
        )
        force_authenticate(request, user=self.admin)
        return ForwardSyncViewSet.as_view({"post": "prune_unmanaged"})(
            request, pk=self.sync.pk
        )

    def _queued(self, data):
        with patch(
            "forward_netbox.jobs.enqueue_forward_job", return_value=self._job()
        ) as enqueue:
            response = self._post(data)
        return response, enqueue

    def test_release_routing_true_reaches_the_job(self):
        device = self._device("op-api")

        response, enqueue = self._queued(
            {"device_pks": [device.pk], "release_routing": True}
        )

        self.assertEqual(response.status_code, 201)
        self.assertTrue(enqueue.call_args.kwargs["release_routing"])
        self.assertEqual(
            enqueue.call_args.kwargs["restrict_to_device_pks"], [device.pk]
        )

    def test_it_defaults_to_off(self):
        device = self._device("op-api")

        response, enqueue = self._queued({"device_pks": [device.pk]})

        self.assertEqual(response.status_code, 201)
        self.assertNotIn("release_routing", enqueue.call_args.kwargs)

    def test_false_is_the_same_as_leaving_it_out(self):
        device = self._device("op-api")

        _response, enqueue = self._queued(
            {"device_pks": [device.pk], "release_routing": False}
        )

        self.assertNotIn("release_routing", enqueue.call_args.kwargs)

    def test_a_non_boolean_is_rejected_before_anything_is_queued(self):
        device = self._device("op-api")

        response, enqueue = self._queued(
            {"device_pks": [device.pk], "release_routing": "yes"}
        )

        self.assertEqual(response.status_code, 400)
        enqueue.assert_not_called()


class JobRecordTest(_Base):
    def test_the_runner_passes_the_routing_option_to_the_work_function(self):
        from forward_netbox.jobs import PruneUnmanagedDevicesJob

        job = self._job()
        with patch("forward_netbox.jobs._prune_unmanaged_devices_work") as work:
            PruneUnmanagedDevicesJob(job).run(
                restrict_to_device_pks=[4, 5], release_routing=True
            )

        work.assert_called_once_with(
            job, restrict_to_device_pks=[4, 5], release_routing=True
        )

    def test_the_runner_leaves_the_option_off_unless_asked(self):
        from forward_netbox.jobs import PruneUnmanagedDevicesJob

        job = self._job()
        with patch("forward_netbox.jobs._prune_unmanaged_devices_work") as work:
            PruneUnmanagedDevicesJob(job).run(restrict_to_device_pks=[4])

        work.assert_called_once_with(job, restrict_to_device_pks=[4])

    def test_the_job_records_what_held_each_device_by_pk_and_model(self):
        from forward_netbox import jobs

        job = self._job()
        result = {
            "pruned_device_count": 1,
            "pruned_object_count": 3,
            "refusals": {"over_cap": []},
            "ownership_blocked_device_count": 0,
            "protected_device_count": 2,
            "protected_by_model": {OSPF_INTERFACE: 2},
            "protected_by_device": {
                11: {OSPF_INTERFACE: 1},
                12: {"extras.configcontext": 1},
            },
            "cap": 100,
            "release_routing": False,
        }
        with (
            patch(
                "forward_netbox.utilities.scope_reconciliation.compute_scope_reconciliation",
                return_value={},
            ),
            patch(
                "forward_netbox.utilities.scope_reconciliation.prune_unmanaged_devices",
                return_value=result,
            ),
        ):
            jobs._prune_unmanaged_devices_work(job, restrict_to_device_pks=[11, 12])

        data = job.data
        self.assertEqual(data["protected_by_device"]["11"], {OSPF_INTERFACE: 1})
        self.assertEqual(data["held_summary"]["freed_by_routing_option"], 1)
        self.assertEqual(data["held_summary"]["needs_other_records"], 1)
        self.assertFalse(data["release_routing"])
        self.assertNotIn("op-", str(data))
