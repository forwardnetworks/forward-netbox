from unittest.mock import Mock

from dcim.models import Interface
from django.test import TestCase

from forward_netbox.exceptions import ForwardDependencySkipError
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.tests.test_subinterface_apply import SubinterfaceFixture
from forward_netbox.utilities.sync import ForwardSyncRunner
from forward_netbox.utilities.sync_reporting import dependency_skip_reason
from forward_netbox.views import _ingestion_issue_bundle_payload


class SkipReasonCodeTest(TestCase):
    """A skipped row names WHY, not just which model it was waiting on.

    Four different conditions on a subinterface row all recorded "waiting on
    dcim.interface", so a customer's 50 skips could not be told apart without a
    shell on their NetBox.
    """

    def test_an_explicit_reason_wins_over_the_derived_one(self):
        exc = ForwardDependencySkipError(
            "x", dependency="dcim.interface", reason="subinterface-parent-not-found"
        )

        self.assertEqual(dependency_skip_reason(exc), "subinterface-parent-not-found")

    def test_a_raiser_with_no_reason_keeps_the_derived_one(self):
        exc = ForwardDependencySkipError("x", dependency="dcim.interface")

        self.assertEqual(dependency_skip_reason(exc), "missing-interface")


class SubinterfaceSkipReasonsTest(SubinterfaceFixture):
    def setUp(self):
        super().setUp()
        self.ingestion = ForwardIngestion.objects.create(
            sync=self.sync, snapshot_id="s"
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync,
            ingestion=self.ingestion,
            client=None,
            logger_=Mock(),
        )

    def _issues(self):
        return list(
            ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion, model="dcim.interface"
            ).order_by("pk")
        )

    def test_a_missing_parent_is_named(self):
        self._apply([self._child_row()])

        (issue,) = self._issues()
        self.assertEqual(
            issue.coalesce_fields["skip_reason"], "subinterface-parent-not-found"
        )

    def test_an_existing_interface_with_no_parent_is_named_and_identified(self):
        existing = Interface.objects.create(
            device=self.device, name="Ethernet1/52.100", type="virtual"
        )
        Interface.objects.create(
            device=self.device, name="Ethernet1/52", type="100gbase-x-qsfp28"
        )

        self._apply([self._child_row()])

        (issue,) = self._issues()
        self.assertEqual(
            issue.coalesce_fields["skip_reason"],
            "subinterface-name-taken-by-parentless-interface",
        )
        # The pk is what lets an operator open the interface in NetBox.
        self.assertEqual(issue.raw_data["netbox_pk"], str(existing.pk))

    def test_a_top_level_row_in_the_same_batch_is_named(self):
        self._apply(
            [
                self._parent_row(),
                self._top_level_row("Ethernet1/54"),
                self._child_row(name="Ethernet1/54"),
            ]
        )

        (issue,) = self._issues()
        self.assertEqual(
            issue.coalesce_fields["skip_reason"],
            "subinterface-name-taken-by-top-level-row",
        )

    def _top_level_row(self, name):
        return {"device": "dev-s", "name": name, "type": "1000base-t", "enabled": True}


class IssueBundleReasonTest(SubinterfaceFixture):
    def test_the_bundle_rolls_up_issues_by_skip_reason(self):
        ingestion = ForwardIngestion.objects.create(sync=self.sync, snapshot_id="s")
        for reason, count in (("subinterface-parent-not-found", 3), ("other", 1)):
            for _ in range(count):
                ForwardIngestionIssue.objects.create(
                    ingestion=ingestion,
                    model="dcim.interface",
                    exception="ForwardDependencySkipError",
                    message="m",
                    raw_data={},
                    coalesce_fields={"skip_reason": reason},
                )

        payload = _ingestion_issue_bundle_payload(ingestion)

        self.assertEqual(
            payload["summary"]["by_skip_reason"][0],
            {
                "model": "dcim.interface",
                "reason": "subinterface-parent-not-found",
                "count": 3,
            },
        )
        self.assertEqual(
            payload["issues"][0]["skip_reason"], "subinterface-parent-not-found"
        )


class BeyondTheCapTest(SubinterfaceFixture):
    def test_skips_past_the_detail_cap_are_still_counted_by_reason(self):
        from forward_netbox.utilities.sync_reporting import (
            emit_dependency_skip_issue_summary,
        )
        from forward_netbox.utilities.sync_reporting import record_issue

        ingestion = ForwardIngestion.objects.create(sync=self.sync, snapshot_id="s")
        runner = ForwardSyncRunner(
            sync=self.sync, ingestion=ingestion, client=None, logger_=Mock()
        )
        runner.DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT = 2
        for index in range(5):
            record_issue(
                runner,
                "dcim.interface",
                "m",
                {"name": f"x{index}"},
                exception=ForwardDependencySkipError(
                    "x",
                    dependency="dcim.interface",
                    reason="subinterface-parent-not-found",
                ),
                context={"name": f"x{index}"},
            )
        emit_dependency_skip_issue_summary(runner, "dcim.interface")

        summary = _ingestion_issue_bundle_payload(ingestion)["summary"]

        counted = {
            (row["model"], row["reason"]): row["count"]
            for row in summary["by_skip_reason"]
        }
        self.assertEqual(
            counted[("dcim.interface", "subinterface-parent-not-found")], 5
        )


class InterfaceStateBundleTest(SubinterfaceFixture):
    """What NetBox holds that decides whether a subinterface row applies.

    The investigation of a customer's 50 skips needed a shell on their NetBox to
    count parentless dotted interfaces, dotted interfaces that are not virtual,
    and one interface carrying ten addresses. All of it is counts and pks.
    """

    def test_it_counts_dotted_interfaces_by_what_decides_a_skip(self):
        from django.contrib.contenttypes.models import ContentType
        from ipam.models import IPAddress
        from ipam.models import VRF

        from forward_netbox.views import _interface_state_bundle_payload

        parent = Interface.objects.create(
            device=self.device, name="Eth1", type="100gbase-x-qsfp28"
        )
        Interface.objects.create(
            device=self.device, name="Eth1.10", type="virtual", parent=parent
        )
        orphan = Interface.objects.create(
            device=self.device, name="lo.10", type="virtual"
        )
        Interface.objects.create(device=self.device, name="Gi0.5", type="other")
        ct = ContentType.objects.get_for_model(Interface)
        for index in range(5):
            vrf = VRF.objects.create(name=f"v{index}")
            IPAddress.objects.create(
                address=f"10.0.{index}.1/24",
                vrf=vrf,
                assigned_object_type=ct,
                assigned_object_id=parent.pk,
            )
        IPAddress.objects.create(address="10.9.9.9/32")

        payload = _interface_state_bundle_payload()

        self.assertEqual(payload["virtual_dotted_with_parent"], 1)
        self.assertEqual(payload["virtual_dotted_without_parent"], 1)
        self.assertEqual(payload["parentless_interfaces"][0]["interface_pk"], orphan.pk)
        self.assertEqual(payload["non_virtual_dotted"]["total"], 1)
        self.assertEqual(payload["non_virtual_dotted"]["by_type"], {"other": 1})
        crowded = payload["crowded_interfaces"][0]
        self.assertEqual(crowded["interface_pk"], parent.pk)
        self.assertEqual(crowded["device_pk"], self.device.pk)
        self.assertEqual(crowded["addresses"], 5)
        self.assertEqual(crowded["vrfs"], 5)
        self.assertEqual(payload["unassigned_ip_addresses"]["total"], 1)
        # Counts and pks only: no device or interface names anywhere.
        self.assertNotIn("dev-s", str(payload))
        self.assertNotIn("Eth1", str(payload))


class SurvivesTheExportFilterTest(SubinterfaceFixture):
    """Every download goes through `export_safe_payload`; a key it drops is a key
    the operator never sees, whatever the builder returned."""

    def test_the_interface_pks_reach_the_downloaded_bundle(self):
        from forward_netbox.utilities.export_redaction import export_safe_payload
        from forward_netbox.views import _interface_state_bundle_payload

        orphan = Interface.objects.create(
            device=self.device, name="lo.10", type="virtual"
        )

        exported = export_safe_payload(_interface_state_bundle_payload())

        self.assertEqual(
            exported["parentless_interfaces"][0]["interface_pk"], orphan.pk
        )


class ReasonSlugTest(TestCase):
    def test_a_reason_that_is_not_a_slug_is_not_persisted(self):
        exc = ForwardDependencySkipError(
            "x", dependency="dcim.interface", reason="Ethernet1/52 on Secret-Device"
        )

        self.assertEqual(dependency_skip_reason(exc), "missing-interface")


class InterfacePkIsNotADevicePkTest(SubinterfaceFixture):
    def test_an_interface_issue_does_not_name_a_device(self):
        from forward_netbox.utilities.bundle_diagnostics import _issue_references

        ingestion = ForwardIngestion.objects.create(sync=self.sync, snapshot_id="s")
        self.sync.last_ingestion  # noqa: B018 - property resolves to this ingestion
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            model="dcim.interface",
            exception="ForwardDependencySkipError",
            message="Affected NetBox row: pk %d." % self.device.pk,
            raw_data={"netbox_pk": str(self.device.pk)},
        )

        self.assertEqual(_issue_references(self.sync)["devices"], [])
