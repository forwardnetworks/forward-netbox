import uuid
from unittest.mock import Mock
from unittest.mock import patch

from core.models import ObjectChange
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import MACAddress
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.test import RequestFactory
from django.test import TestCase
from django.urls import reverse
from netbox.context_managers import event_tracking
from netbox_branching.models import ChangeDiff
from netbox_branching.utilities import activate_branch

from .test_bulk_merge import CleanTransactionTestCase
from .test_bulk_merge import provision_branch
from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities import apply_engine_decision
from forward_netbox.utilities.apply_engine import select_apply_engine
from forward_netbox.utilities.merge import merge_branch
from forward_netbox.utilities.sync import ForwardSyncRunner


class CopySQLSelectionTest(TestCase):
    def setUp(self):
        source = ForwardSource.objects.create(
            name="copy-sql-selection-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "copy@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "copy-sql-selection",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="copy-sql-selection",
            source=source,
            parameters={
                "snapshot_id": "latestProcessed",
                "enable_bulk_orm": True,
                "enable_copy_sql": True,
                "copy_sql_kill_switches": [],
            },
        )

    def test_unsupported_version_tuple_fails_closed(self):
        cases = (
            # Out of series, not merely a later patch: the gate accepts any
            # 4.6.x and any 1.1.x, so a rejection case has to leave the series.
            (("4.7.0", "1.1.1", ()), "unsupported_netbox_version"),
            (("4.6.5", "1.2.0", ()), "unsupported_branching_version"),
            (
                (
                    "4.6.5",
                    "1.1.1",
                    (("netbox-cisco-aci", "9.9.9"),),
                ),
                "unsupported_optional_plugin_version",
            ),
        )
        for runtime_tuple, reason_code in cases:
            with self.subTest(runtime_tuple=runtime_tuple), patch.object(
                apply_engine_decision,
                "copy_sql_runtime_version_tuple",
                return_value=runtime_tuple,
            ):
                engine = select_apply_engine(
                    sync=self.sync,
                    model_string="dcim.macaddress",
                )
                self.assertEqual(engine.name, "bulk_orm")
                self.assertEqual(
                    engine.decision.rejected_engines[0]["reason_code"],
                    reason_code,
                )

    def test_active_branch_is_required(self):
        engine = select_apply_engine(sync=self.sync, model_string="dcim.macaddress")
        self.assertEqual(engine.name, "bulk_orm")
        self.assertEqual(
            engine.decision.rejected_engines[0]["reason_code"],
            "active_branch_required",
        )

    def test_model_kill_switch_fails_closed(self):
        self.sync.parameters["copy_sql_kill_switches"] = ["dcim.macaddress"]
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
        )
        self.assertEqual(engine.name, "bulk_orm")
        self.assertEqual(
            engine.decision.rejected_engines[0]["reason_code"],
            "model_kill_switch_set",
        )

    def test_default_off_uses_existing_engine(self):
        self.sync.parameters.pop("enable_copy_sql")
        engine = select_apply_engine(
            sync=self.sync,
            model_string="dcim.macaddress",
        )
        self.assertEqual(engine.name, "bulk_orm")


class CopySQLMacSmokeTest(CleanTransactionTestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="copy-sql-user")
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user
        self.source = ForwardSource.objects.create(
            name="copy-sql-source",
            type="saas",
            url="https://fwd.app",
            status="ready",
            parameters={
                "username": "copy@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "copy-sql-test",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="copy-sql-sync",
            source=self.source,
            user=self.user,
            parameters={
                "snapshot_id": "latestProcessed",
                "enable_bulk_orm": True,
                "enable_copy_sql": True,
                "copy_sql_kill_switches": [],
                "dcim.macaddress": True,
            },
        )
        manufacturer = Manufacturer.objects.create(
            name="Copy SQL Manufacturer", slug="copy-sql-manufacturer"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Copy SQL Device Type",
            slug="copy-sql-device-type",
        )
        role = DeviceRole.objects.create(name="Copy SQL Role", slug="copy-sql-role")
        site = Site.objects.create(name="Copy SQL Site", slug="copy-sql-site")
        self.device = Device.objects.create(
            name="copy-sql-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        self.interface = Interface.objects.create(
            device=self.device,
            name="Ethernet1",
            type="1000base-t",
        )

    def _runner(self):
        return ForwardSyncRunner(
            sync=self.sync,
            ingestion=None,
            client=None,
            logger_=Mock(),
        )

    def test_create_writes_native_evidence_without_fallback(self):
        branch = provision_branch(user=self.user, name="COPY SQL create")
        row = {
            "device": self.device.name,
            "interface": self.interface.name,
            "mac": "00:11:22:33:44:55",
        }
        self.request.id = uuid.uuid4()
        runner = self._runner()
        with activate_branch(branch), event_tracking(self.request):
            engine = select_apply_engine(
                sync=self.sync,
                model_string="dcim.macaddress",
            )
            self.assertEqual(engine.name, "copy_sql")
            with patch(
                "forward_netbox.utilities.apply_engine._bulk_orm_apply_simple_models",
                side_effect=AssertionError("COPY/SQL unexpectedly fell back"),
            ):
                engine.apply_plan_item(runner, "dcim.macaddress", [row], [])
            mac = MACAddress.objects.get(mac_address="00:11:22:33:44:55")
            change = ObjectChange.objects.using(branch.connection_name).get(
                request_id=self.request.id,
                changed_object_id=mac.pk,
            )
            diff = ChangeDiff.objects.using(branch.connection_name).get(
                branch=branch,
                object_id=mac.pk,
            )
            self.assertEqual(change.action, "create")
            self.assertIsNone(change.prechange_data)
            self.assertEqual(
                change.postchange_data,
                mac.serialize_object(exclude=["last_updated"]),
            )
            self.assertEqual(diff.action, "create")
            self.assertIsNone(diff.original)
            self.assertEqual(
                diff.modified,
                mac.serialize_object(exclude=["created", "last_updated"]),
            )
            self.assertIsNone(diff.current)
            self.assertIsNone(diff.conflicts)


class CopySQLMacPairedBranchTest(CopySQLMacSmokeTest):
    maxDiff = None

    def _paired_sync(self, *, suffix, enable_copy_sql):
        return ForwardSync.objects.create(
            name=f"copy-sql-paired-{suffix}",
            source=self.source,
            user=self.user,
            parameters={
                "snapshot_id": "latestProcessed",
                "enable_bulk_orm": True,
                "enable_copy_sql": enable_copy_sql,
                "copy_sql_kill_switches": [],
                "dcim.macaddress": True,
            },
        )

    def _paired_ingestion(self, *, sync, branch, suffix):
        return ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id=f"paired-{suffix}",
            branch=branch,
        )

    @staticmethod
    def _normalized_json(value):
        if isinstance(value, list):
            return [CopySQLMacPairedBranchTest._normalized_json(item) for item in value]
        if not isinstance(value, dict):
            return value
        return {
            key: (
                "<timestamp>"
                if key in {"created", "last_updated"} and item is not None
                else CopySQLMacPairedBranchTest._normalized_json(item)
            )
            for key, item in value.items()
        }

    @staticmethod
    def _statistics(logger):
        outcomes = {}
        for call in logger.increment_statistics.call_args_list:
            args, kwargs = call
            if not args or args[0] != "dcim.macaddress":
                continue
            outcome = kwargs.get("outcome")
            outcomes[outcome] = outcomes.get(outcome, 0) + int(kwargs.get("amount", 1))
        return outcomes

    def _capture_branch(self, *, branch, ingestion, request_id, runner, known_ids):
        mac_type = ContentType.objects.get_for_model(MACAddress)
        content_types = {
            content_type.pk: f"{content_type.app_label}.{content_type.model}"
            for content_type in ContentType.objects.all()
        }
        macs = list(MACAddress.objects.order_by("mac_address", "pk"))
        grouped = {}
        for mac in macs:
            grouped.setdefault(str(mac.mac_address), []).append(mac.pk)
        logical_ids = {
            pk: f"{address}#{ordinal}"
            for address, pks in grouped.items()
            for ordinal, pk in enumerate(sorted(pks), start=1)
        }
        for address, pk in known_ids.items():
            logical_ids.setdefault(pk, f"{address}#1")

        targets = [
            {
                "identity": logical_ids[mac.pk],
                "mac_address": str(mac.mac_address),
                "assigned_object_type_id": mac.assigned_object_type_id,
                "assigned_object_id": mac.assigned_object_id,
                "owner_id": mac.owner_id,
                "description": mac.description,
                "comments": mac.comments,
                "custom_fields": mac.custom_field_data,
                "tags": sorted(mac.tags.values_list("name", flat=True)),
            }
            for mac in macs
        ]
        tagged_items = []
        from extras.models import TaggedItem

        for item in TaggedItem.objects.filter(content_type=mac_type).select_related(
            "tag"
        ):
            tagged_items.append(
                (
                    logical_ids.get(item.object_id, f"missing:{item.object_id}"),
                    item.tag.name,
                )
            )
        side_effect_rows = {}
        from extras.models import Bookmark
        from extras.models import JournalEntry
        from extras.models import Subscription

        for label, model, type_field, object_field, extra_fields in (
            ("bookmarks", Bookmark, "object_type", "object_id", ("user_id",)),
            (
                "journal_entries",
                JournalEntry,
                "assigned_object_type",
                "assigned_object_id",
                ("created_by_id", "kind", "comments"),
            ),
            (
                "subscriptions",
                Subscription,
                "object_type",
                "object_id",
                ("user_id",),
            ),
        ):
            object_ids = set(logical_ids)
            rows = model.objects.filter(
                **{type_field: mac_type, f"{object_field}__in": object_ids}
            ).values(object_field, *extra_fields)
            side_effect_rows[label] = sorted(
                [
                    {
                        "object": logical_ids[row.pop(object_field)],
                        **row,
                    }
                    for row in rows
                ],
                key=lambda row: tuple(str(value) for value in row.values()),
            )
        object_changes = []
        for change in (
            ObjectChange.objects.using(branch.connection_name)
            .filter(request_id=request_id)
            .order_by("pk")
        ):
            if change.changed_object_type_id == mac_type.pk:
                object_identity = logical_ids[change.changed_object_id]
            else:
                object_identity = (
                    f"{content_types[change.changed_object_type_id]}"
                    f"#{change.changed_object_id}"
                )
            object_changes.append(
                {
                    "time": "<timestamp>",
                    "user_id": change.user_id,
                    "user_name": change.user_name,
                    "request_id": "<request>",
                    "action": change.action,
                    "changed_object_type_id": change.changed_object_type_id,
                    "object": object_identity,
                    "object_repr": change.object_repr,
                    "related_object_type_id": change.related_object_type_id,
                    "related_object_id": change.related_object_id,
                    "message": change.message,
                    "prechange_data": self._normalized_json(change.prechange_data),
                    "postchange_data": self._normalized_json(change.postchange_data),
                }
            )
        change_diffs = []
        for diff in (
            ChangeDiff.objects.using(branch.connection_name)
            .filter(branch=branch)
            .order_by("object_type_id", "object_id")
        ):
            if diff.object_type_id == mac_type.pk:
                object_identity = logical_ids[diff.object_id]
            else:
                object_identity = (
                    f"{content_types[diff.object_type_id]}#{diff.object_id}"
                )
            change_diffs.append(
                {
                    "object_type_id": diff.object_type_id,
                    "object": object_identity,
                    "object_repr": diff.object_repr,
                    "action": diff.action,
                    "original": self._normalized_json(diff.original),
                    "modified": self._normalized_json(diff.modified),
                    "current": self._normalized_json(diff.current),
                    "conflicts": diff.conflicts,
                }
            )
        change_diffs.sort(key=lambda row: row["object"])
        issues = list(
            ForwardIngestionIssue.objects.filter(ingestion=ingestion)
            .order_by("model", "exception", "message")
            .values(
                "phase",
                "model",
                "message",
                "coalesce_fields",
                "defaults",
                "raw_data",
                "exception",
            )
        )
        return {
            "targets": targets,
            "tagged_items": sorted(tagged_items),
            "side_effect_rows": side_effect_rows,
            "statistics": self._statistics(runner.logger),
            "issues": issues,
            "object_changes": object_changes,
            "change_diffs": change_diffs,
        }

    def _stage_and_capture(
        self,
        *,
        sync,
        branch,
        ingestion,
        upsert_rows,
        delete_rows,
        request_id,
        known_ids,
    ):
        from core.signals import _signals_received

        _signals_received.pre_delete = set()
        runner = ForwardSyncRunner(
            sync=sync,
            ingestion=ingestion,
            client=None,
            logger_=Mock(),
        )
        self.request.id = request_id
        with activate_branch(branch), event_tracking(self.request):
            engine = select_apply_engine(sync=sync, model_string="dcim.macaddress")
            engine.apply_plan_item(
                runner,
                "dcim.macaddress",
                upsert_rows,
                delete_rows,
            )
            return engine.name, self._capture_branch(
                branch=branch,
                ingestion=ingestion,
                request_id=request_id,
                runner=runner,
                known_ids=known_ids,
            )

    def test_paired_branches_match_full_mixed_dispositions_and_evidence(self):
        second_interface = Interface.objects.create(
            device=self.device, name="Ethernet2", type="1000base-t"
        )
        third_interface = Interface.objects.create(
            device=self.device, name="Ethernet3", type="1000base-t"
        )
        interface_type = ContentType.objects.get_for_model(Interface)
        mac_type = ContentType.objects.get_for_model(MACAddress)

        def seed_mac(address, interface):
            return MACAddress.objects.create(
                mac_address=address,
                assigned_object_type=interface_type,
                assigned_object_id=interface.pk,
            )

        update_mac = seed_mac("00:11:22:33:44:02", self.interface)
        noop_mac = seed_mac("00:11:22:33:44:03", self.interface)
        identity_old = seed_mac("00:11:22:33:44:05", self.interface)
        delete_mac = seed_mac("00:11:22:33:44:06", self.interface)
        tagged_delete = seed_mac("00:11:22:33:44:07", self.interface)
        from extras.models import Tag
        from extras.models import Bookmark
        from extras.models import JournalEntry
        from extras.models import Subscription

        tag = Tag.objects.create(name="paired-tag", slug="paired-tag")
        tagged_delete.tags.add(tag)
        Bookmark.objects.create(
            object_type=mac_type,
            object_id=tagged_delete.pk,
            user=self.user,
        )
        JournalEntry.objects.create(
            assigned_object_type=mac_type,
            assigned_object_id=tagged_delete.pk,
            created_by=self.user,
            comments="paired journal",
        )
        Subscription.objects.create(
            object_type=mac_type,
            object_id=tagged_delete.pk,
            user=self.user,
        )
        ambiguous_a = seed_mac("00:11:22:33:44:AA", self.interface)
        ambiguous_b = seed_mac("00:11:22:33:44:AA", second_interface)
        primary_mac = seed_mac("00:11:22:33:44:DD", self.interface)
        conflict_mac = seed_mac("00:11:22:33:44:08", self.interface)
        self.interface.primary_mac_address = primary_mac
        self.interface.save(update_fields=["primary_mac_address"])

        known_ids = {
            str(mac.mac_address): mac.pk
            for mac in (
                update_mac,
                noop_mac,
                identity_old,
                delete_mac,
                tagged_delete,
                primary_mac,
                conflict_mac,
            )
        }
        known_ids[f"{ambiguous_a.mac_address}:a"] = ambiguous_a.pk
        known_ids[f"{ambiguous_b.mac_address}:b"] = ambiguous_b.pk
        upsert_rows = [
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "00:11:22:33:44:01",
            },
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "0011.2233.4402",
            },
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "00-11-22-33-44-03",
            },
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "00:11:22:33:44:04",
            },
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "00:11:22:33:44:08",
            },
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "00:11:22:33:44:AA",
            },
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "not-a-mac",
            },
            {
                "device": self.device.name,
                "interface": "",
                "mac": "00:11:22:33:44:EE",
            },
            {
                "device": "missing-device",
                "interface": "Ethernet1",
                "mac": "00:11:22:33:44:BB",
            },
            {
                "device": self.device.name,
                "interface": "Ethernet404",
                "mac": "00:11:22:33:44:CC",
            },
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "00:11:22:33:44:DD",
            },
        ]
        delete_rows = [
            {"mac": "0011.2233.4405"},
            {"mac": "00:11:22:33:44:06"},
            {"mac": "00-11-22-33-44-07"},
            {"mac": "00:11:22:33:44:FE"},
        ]

        current_sync = self._paired_sync(suffix="current", enable_copy_sql=False)
        copy_sync = self._paired_sync(suffix="copy", enable_copy_sql=True)
        current_branch = provision_branch(user=self.user, name="Paired current")
        copy_branch = provision_branch(user=self.user, name="Paired COPY SQL")
        conflict_mac.assigned_object_id = third_interface.pk
        conflict_mac.save(update_fields=["assigned_object_id"])
        current_ingestion = self._paired_ingestion(
            sync=current_sync, branch=current_branch, suffix="current"
        )
        copy_ingestion = self._paired_ingestion(
            sync=copy_sync, branch=copy_branch, suffix="copy"
        )
        current_engine, current = self._stage_and_capture(
            sync=current_sync,
            branch=current_branch,
            ingestion=current_ingestion,
            upsert_rows=upsert_rows,
            delete_rows=delete_rows,
            request_id=uuid.uuid4(),
            known_ids=known_ids,
        )
        copy_engine, copied = self._stage_and_capture(
            sync=copy_sync,
            branch=copy_branch,
            ingestion=copy_ingestion,
            upsert_rows=upsert_rows,
            delete_rows=delete_rows,
            request_id=uuid.uuid4(),
            known_ids=known_ids,
        )
        self.assertEqual(current_engine, "bulk_orm")
        self.assertEqual(copy_engine, "copy_sql")
        self.assertEqual(copied, current)

    def test_relation_bound_delete_keeps_native_evidence_after_fallback_upserts(self):
        interface_type = ContentType.objects.get_for_model(Interface)
        mac = MACAddress.objects.create(
            mac_address="00:11:22:33:44:70",
            assigned_object_type=interface_type,
            assigned_object_id=self.interface.pk,
        )
        from extras.models import Tag

        mac.tags.add(Tag.objects.create(name="relation-tag", slug="relation-tag"))
        branch = provision_branch(user=self.user, name="Relation fallback evidence")
        self.request.id = uuid.uuid4()
        runner = self._runner()
        with activate_branch(branch), event_tracking(self.request):
            engine = select_apply_engine(sync=self.sync, model_string="dcim.macaddress")
            engine.apply_plan_item(
                runner,
                "dcim.macaddress",
                [
                    {
                        "device": self.device.name,
                        "interface": self.interface.name,
                        "mac": "not-a-mac",
                    }
                ],
                [{"mac": "00:11:22:33:44:70"}],
            )
            change = ObjectChange.objects.using(branch.connection_name).get(
                request_id=self.request.id,
                changed_object_id=mac.pk,
                action="delete",
            )
            self.assertEqual(change.prechange_data["tags"], ["relation-tag"])

    def test_faults_rollback_then_match_current_engine_without_partial_evidence(self):
        second_interface = Interface.objects.create(
            device=self.device, name="Ethernet2", type="1000base-t"
        )
        interface_type = ContentType.objects.get_for_model(Interface)
        updated = MACAddress.objects.create(
            mac_address="00:11:22:33:45:01",
            assigned_object_type=interface_type,
            assigned_object_id=self.interface.pk,
        )
        deleted = MACAddress.objects.create(
            mac_address="00:11:22:33:45:02",
            assigned_object_type=interface_type,
            assigned_object_id=self.interface.pk,
        )
        known_ids = {
            str(updated.mac_address): updated.pk,
            str(deleted.mac_address): deleted.pk,
        }
        upsert_rows = [
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "00:11:22:33:45:03",
            },
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "00:11:22:33:45:01",
            },
        ]
        delete_rows = [{"mac": "00:11:22:33:45:02"}]
        current_sync = self._paired_sync(suffix="fault-current", enable_copy_sql=False)
        current_branch = provision_branch(user=self.user, name="Fault current")
        current_ingestion = self._paired_ingestion(
            sync=current_sync, branch=current_branch, suffix="fault-current"
        )
        _, expected = self._stage_and_capture(
            sync=current_sync,
            branch=current_branch,
            ingestion=current_ingestion,
            upsert_rows=upsert_rows,
            delete_rows=delete_rows,
            request_id=uuid.uuid4(),
            known_ids=known_ids,
        )

        for fault_stage in (
            "after_target_dml",
            "after_object_changes",
            "during_change_diff_update",
        ):
            with self.subTest(fault_stage=fault_stage):
                sync = self._paired_sync(
                    suffix=f"fault-{fault_stage}", enable_copy_sql=True
                )
                branch = provision_branch(
                    user=self.user, name=f"Fault COPY SQL {fault_stage}"
                )
                ingestion = self._paired_ingestion(
                    sync=sync, branch=branch, suffix=fault_stage
                )

                def inject(stage):
                    if stage == fault_stage:
                        raise RuntimeError(f"injected {stage}")

                with patch(
                    "forward_netbox.utilities.apply_engine_copy_sql._inject_copy_sql_fault",
                    side_effect=inject,
                ):
                    engine_name, actual = self._stage_and_capture(
                        sync=sync,
                        branch=branch,
                        ingestion=ingestion,
                        upsert_rows=upsert_rows,
                        delete_rows=delete_rows,
                        request_id=uuid.uuid4(),
                        known_ids=known_ids,
                    )
                self.assertEqual(engine_name, "copy_sql")
                self.assertEqual(actual, expected)

    def test_paired_branches_produce_the_same_final_merge_result(self):
        second_interface = Interface.objects.create(
            device=self.device, name="Ethernet2", type="1000base-t"
        )
        interface_type = ContentType.objects.get_for_model(Interface)
        updated = MACAddress.objects.create(
            mac_address="00:11:22:33:46:01",
            assigned_object_type=interface_type,
            assigned_object_id=self.interface.pk,
        )
        deleted = MACAddress.objects.create(
            mac_address="00:11:22:33:46:02",
            assigned_object_type=interface_type,
            assigned_object_id=self.interface.pk,
        )
        known_ids = {
            str(updated.mac_address): updated.pk,
            str(deleted.mac_address): deleted.pk,
        }
        upsert_rows = [
            {
                "device": self.device.name,
                "interface": second_interface.name,
                "mac": "00:11:22:33:46:01",
            },
            {
                "device": self.device.name,
                "interface": self.interface.name,
                "mac": "00:11:22:33:46:03",
            },
        ]
        delete_rows = [{"mac": "00:11:22:33:46:02"}]
        staged = []
        for suffix, enable_copy_sql in (("merge-current", False), ("merge-copy", True)):
            sync = self._paired_sync(
                suffix=suffix,
                enable_copy_sql=enable_copy_sql,
            )
            branch = provision_branch(user=self.user, name=f"Paired {suffix}")
            ingestion = self._paired_ingestion(
                sync=sync,
                branch=branch,
                suffix=suffix,
            )
            self._stage_and_capture(
                sync=sync,
                branch=branch,
                ingestion=ingestion,
                upsert_rows=upsert_rows,
                delete_rows=delete_rows,
                request_id=uuid.uuid4(),
                known_ids=known_ids,
            )
            staged.append((branch, ingestion))

        def merged_state():
            return sorted(
                (
                    str(mac.mac_address),
                    mac.assigned_object_type_id,
                    mac.assigned_object_id,
                    mac.description,
                    mac.comments,
                    mac.owner_id,
                    mac.custom_field_data,
                    sorted(mac.tags.values_list("name", flat=True)),
                )
                for mac in MACAddress.objects.filter(
                    mac_address__in=[
                        "00:11:22:33:46:01",
                        "00:11:22:33:46:02",
                        "00:11:22:33:46:03",
                    ]
                )
            )

        results = []
        for branch, ingestion in staged:
            with transaction.atomic():
                merge_branch(ingestion, user=self.user)
                results.append(merged_state())
                transaction.set_rollback(True)
            branch.refresh_from_db()
            ingestion.refresh_from_db()
        self.assertEqual(results[0], results[1])

    def test_update_noop_delete_write_complete_native_evidence(self):
        other_interface = Interface.objects.create(
            device=self.device,
            name="Ethernet2",
            type="1000base-t",
        )
        mac = MACAddress.objects.create(
            mac_address="00:11:22:33:44:66",
            assigned_object_type=self._runner()._content_type_for(Interface),
            assigned_object_id=self.interface.pk,
        )
        branch = provision_branch(user=self.user, name="COPY SQL update delete")
        row = {
            "device": self.device.name,
            "interface": other_interface.name,
            "mac": "0011.2233.4466",
        }
        self.request.id = uuid.uuid4()
        runner = self._runner()
        with activate_branch(branch), event_tracking(self.request):
            engine = select_apply_engine(
                sync=self.sync,
                model_string="dcim.macaddress",
            )
            with patch(
                "forward_netbox.utilities.apply_engine._bulk_orm_apply_simple_models",
                side_effect=AssertionError("COPY/SQL unexpectedly fell back"),
            ):
                engine.apply_plan_item(runner, "dcim.macaddress", [row], [])
                object_change_count = (
                    ObjectChange.objects.using(branch.connection_name)
                    .filter(
                        request_id=self.request.id,
                        changed_object_id=mac.pk,
                    )
                    .count()
                )
                engine.apply_plan_item(runner, "dcim.macaddress", [row], [])
                self.assertEqual(
                    ObjectChange.objects.using(branch.connection_name)
                    .filter(
                        request_id=self.request.id,
                        changed_object_id=mac.pk,
                    )
                    .count(),
                    object_change_count,
                    "no-op emitted ObjectChange evidence",
                )
                engine.apply_plan_item(runner, "dcim.macaddress", [], [row])

            changes = list(
                ObjectChange.objects.using(branch.connection_name)
                .filter(
                    request_id=self.request.id,
                    changed_object_id=mac.pk,
                )
                .order_by("pk")
            )
            self.assertEqual(
                [change.action for change in changes], ["update", "delete"]
            )
            self.assertEqual(
                changes[0].prechange_data["assigned_object_id"], self.interface.pk
            )
            self.assertEqual(
                changes[0].postchange_data["assigned_object_id"], other_interface.pk
            )
            self.assertEqual(
                changes[1].prechange_data["assigned_object_id"], other_interface.pk
            )
            self.assertIsNone(changes[1].postchange_data)
            self.assertFalse(MACAddress.objects.filter(pk=mac.pk).exists())
            diff = ChangeDiff.objects.using(branch.connection_name).get(
                branch=branch,
                object_id=mac.pk,
            )
            self.assertEqual(diff.action, "delete")
            self.assertEqual(diff.original["assigned_object_id"], self.interface.pk)
            self.assertIsNone(diff.modified)
            self.assertEqual(diff.current["assigned_object_id"], self.interface.pk)
            self.assertIsNone(diff.conflicts)
