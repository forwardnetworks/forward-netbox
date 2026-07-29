import logging
import uuid
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
from django.db import connections
from django.db.models.signals import post_save
from django.test import RequestFactory
from django.test import TransactionTestCase
from django.urls import reverse
from extras.models import Bookmark
from extras.models import CachedValue
from extras.models import JournalEntry
from extras.models import Notification
from extras.models import Subscription
from extras.models import Tag
from extras.models import TaggedItem
from netbox.context import current_request
from netbox.context_managers import event_tracking
from netbox_branching.merge_strategies import SquashMergeStrategy
from netbox_branching.models import AppliedChange
from netbox_branching.models import Branch
from netbox_branching.models import ChangeDiff
from netbox_branching.utilities import activate_branch

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.apply_engine import select_apply_engine
from forward_netbox.utilities.branching import build_branch_request
from forward_netbox.utilities.logging import SyncLogging
from forward_netbox.utilities.merge import merge_branch
from forward_netbox.utilities.merge_set_based import apply_set_based_mac_range
from forward_netbox.utilities.merge_set_based import set_based_merge_decision
from forward_netbox.utilities.sync import ForwardSyncRunner


def provision_branch(*, user, name):
    branch = Branch(name=name)
    branch.save(provision=False)
    branch.provision(user=user)
    branch.refresh_from_db()
    return branch


class SetBasedMergeMACTest(TransactionTestCase):
    reset_sequences = True

    @classmethod
    def _pre_setup(cls):
        current_request.set(None)
        super()._pre_setup()

    def _post_teardown(self):
        current_request.set(None)
        try:
            super()._post_teardown()
        finally:
            connections.close_all()
            current_request.set(None)

    def setUp(self):
        self.user = get_user_model().objects.create_user(username="set-merge-user")
        manufacturer = Manufacturer.objects.create(
            name="Set Merge Manufacturer", slug="set-merge-manufacturer"
        )
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer,
            model="Set Merge Device Type",
            slug="set-merge-device-type",
        )
        role = DeviceRole.objects.create(name="Set Merge Role", slug="set-merge-role")
        site = Site.objects.create(name="Set Merge Site", slug="set-merge-site")
        self.device = Device.objects.create(
            name="set-merge-device",
            device_type=device_type,
            role=role,
            site=site,
        )
        self.interfaces = [
            Interface.objects.create(
                device=self.device,
                name=f"Ethernet{index}",
                type="1000base-t",
            )
            for index in range(1, 5)
        ]
        self.interface_type = ContentType.objects.get_for_model(Interface)
        self.mac_type = ContentType.objects.get_for_model(MACAddress)
        self.logical_macs = {}
        self.request = RequestFactory().get(reverse("home"))
        self.request.user = self.user

    def _seed_mac(self, address, interface=None, logical_name=None):
        interface = interface or self.interfaces[0]
        mac = MACAddress.objects.create(
            mac_address=address,
            assigned_object_type=self.interface_type,
            assigned_object_id=interface.pk,
        )
        if logical_name:
            self.logical_macs[str(mac.mac_address).upper()] = logical_name
        return mac

    def _branch_ingestion(self, *, suffix, enabled):
        source = ForwardSource.objects.create(
            name=f"set-merge-source-{suffix}",
            type="saas",
            url="https://fwd.app",
            parameters={"network_id": f"synthetic-{suffix}"},
        )
        sync = ForwardSync.objects.create(
            name=f"set-merge-sync-{suffix}",
            source=source,
            user=self.user,
            auto_merge=False,
            parameters={
                "snapshot_id": "latestProcessed",
                "enable_bulk_orm": True,
                "enable_set_based_merge": enabled,
                "set_based_merge_kill_switches": [],
                "dcim.macaddress": True,
            },
        )
        branch = provision_branch(user=self.user, name=f"Set merge {suffix}")
        ingestion = ForwardIngestion.objects.create(
            sync=sync,
            snapshot_selector="latestProcessed",
            snapshot_id=f"synthetic-snapshot-{suffix}",
            branch=branch,
        )
        return branch, ingestion

    def _stage(self, branch, ingestion, fixture):
        request = build_branch_request(self.user)
        runner = ForwardSyncRunner(
            sync=ingestion.sync,
            ingestion=ingestion,
            client=None,
            logger_=SyncLogging(),
        )
        upserts = [
            {
                "device": self.device.name,
                "interface": self.interfaces[1].name,
                "mac": str(fixture[key].mac_address),
            }
            for key in (
                "update",
                "destination_noop",
                "conflict",
                "nonconflict",
                "tagged_update",
                "main_delete",
            )
        ]
        upserts.append(
            {
                "device": self.device.name,
                "interface": self.interfaces[0].name,
                "mac": fixture["create_address"],
            }
        )
        deletes = [
            {"mac": str(fixture[key].mac_address)}
            for key in ("delete", "relation_delete", "missing_delete")
        ]
        with activate_branch(branch), event_tracking(request):
            token = current_request.set(request)
            try:
                engine = select_apply_engine(
                    sync=ingestion.sync,
                    model_string="dcim.macaddress",
                )
                engine.apply_upserts(runner, "dcim.macaddress", upserts)
                engine.apply_deletes(runner, "dcim.macaddress", deletes)
            finally:
                current_request.reset(token)
        return MACAddress.objects.using(branch.connection_name).get(
            mac_address=fixture["create_address"]
        )

    def _normalized_text(self, value):
        text = str(value)
        for address, logical_name in self.logical_macs.items():
            text = text.replace(address, logical_name)
        return text

    def _normalized_json(self, value):
        if isinstance(value, list):
            return [self._normalized_json(item) for item in value]
        if not isinstance(value, dict):
            if isinstance(value, str):
                return self._normalized_text(value)
            return value
        return {
            key: (
                "<timestamp>"
                if key in {"created", "last_updated"} and item is not None
                else self._normalized_json(item)
            )
            for key, item in value.items()
        }

    def _logical_identity(self, object_id):
        try:
            return str(MACAddress.objects.get(pk=object_id).mac_address)
        except MACAddress.DoesNotExist:
            return f"deleted:{object_id}"

    def _side_tables(self, target_ids, identities):
        result = {}
        definitions = (
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
        )
        for label, model, type_field, object_field, extra_fields in definitions:
            result[label] = sorted(
                (
                    identities[row[object_field]],
                    *(row[field] for field in extra_fields),
                )
                for row in model.objects.filter(
                    **{
                        type_field: self.mac_type,
                        f"{object_field}__in": target_ids,
                    }
                ).values(object_field, *extra_fields)
            )
        result["tags"] = sorted(
            (identities[row["object_id"]], row["tag__name"])
            for row in TaggedItem.objects.filter(
                content_type=self.mac_type,
                object_id__in=target_ids,
            ).values("object_id", "tag__name")
        )
        result["notifications"] = sorted(
            (
                identities[row["object_id"]],
                row["user_id"],
                row["event_type"],
                self._normalized_text(row["object_repr"]),
            )
            for row in Notification.objects.filter(
                object_type=self.mac_type,
                object_id__in=target_ids,
            ).values("object_id", "user_id", "event_type", "object_repr")
        )
        result["search_cache"] = sorted(
            (
                identities[row["object_id"]],
                row["field"],
                row["type"],
                self._normalized_text(row["value"]),
                row["weight"],
            )
            for row in CachedValue.objects.filter(
                object_type=self.mac_type,
                object_id__in=target_ids,
            ).values("object_id", "field", "type", "value", "weight")
        )
        return result

    def _capture_merge(self, branch, ingestion, fixture, created):
        targets = [
            item for item in fixture.values() if isinstance(item, MACAddress)
        ] + [created]
        target_ids = sorted(item.pk for item in targets)
        identities = {
            item.pk: self._normalized_text(str(item.mac_address)) for item in targets
        }
        source_diff_before = self._capture_diffs(branch)
        competing = fixture["competing_branch"]
        merge_branch(ingestion, user=self.user)
        ingestion.refresh_from_db()
        branch.refresh_from_db()
        audits = []
        for change in ObjectChange.objects.filter(
            request_id=ingestion.change_request_id,
            changed_object_type=self.mac_type,
            changed_object_id__in=target_ids,
        ).order_by("action", "changed_object_id"):
            identity_payload = change.postchange_data or change.prechange_data or {}
            audits.append(
                {
                    "user_id": change.user_id,
                    "user_name": change.user_name,
                    "request_id": "<merge-request>",
                    "action": change.action,
                    "object": self._normalized_text(
                        identity_payload.get("mac_address")
                        or f"missing:{change.changed_object_id}"
                    ),
                    "object_repr": self._normalized_text(change.object_repr),
                    "related_object_type_id": change.related_object_type_id,
                    "related_object_id": change.related_object_id,
                    "message": change.message,
                    "prechange_data": self._normalized_json(change.prechange_data),
                    "postchange_data": self._normalized_json(change.postchange_data),
                }
            )
        applied = sorted(
            (
                item.change.action,
                (item.change.postchange_data or item.change.prechange_data or {}).get(
                    "mac_address"
                )
                or f"missing:{item.change.changed_object_id}",
                item.branch_id,
            )
            for item in AppliedChange.objects.filter(
                branch=branch,
                change__request_id=ingestion.change_request_id,
                change__changed_object_type=self.mac_type,
                change__changed_object_id__in=target_ids,
            ).select_related("change")
        )
        state = {
            "targets": sorted(
                (
                    self._normalized_text(str(mac.mac_address)),
                    mac.assigned_object_type_id,
                    mac.assigned_object_id,
                    mac.owner_id,
                    mac.description,
                    mac.comments,
                    mac.custom_field_data,
                    tuple(sorted(mac.tags.values_list("name", flat=True))),
                )
                for mac in MACAddress.objects.filter(pk__in=target_ids)
            ),
            "side_tables": self._side_tables(target_ids, identities),
            "audits": audits,
            "applied": [
                (action, self._normalized_text(object_id), "<source-branch>")
                for action, object_id, _branch_id in applied
            ],
            "source_diffs_before": source_diff_before,
            "source_diffs_after": self._capture_diffs(branch),
            "competing_diffs": self._capture_diffs(competing, object_ids=target_ids),
            "issues": list(
                ForwardIngestionIssue.objects.filter(ingestion=ingestion)
                .order_by("model", "exception", "message")
                .values(
                    "phase",
                    "model",
                    "message",
                    "exception",
                )
            ),
            "counts": (
                ingestion.applied_change_count,
                ingestion.failed_change_count,
                ingestion.created_change_count,
                ingestion.updated_change_count,
                ingestion.deleted_change_count,
            ),
            "branch_status": branch.status,
        }
        self.assertEqual(state["source_diffs_before"], state["source_diffs_after"])
        return state

    def _capture_diffs(self, branch, object_ids=None):
        queryset = ChangeDiff.objects.filter(
            branch=branch,
            object_type=self.mac_type,
        )
        if object_ids is not None:
            queryset = queryset.filter(object_id__in=object_ids)
        return sorted(
            [
                {
                    "object": self._diff_identity(diff),
                    "object_repr": self._normalized_text(diff.object_repr),
                    "action": diff.action,
                    "original": self._normalized_json(diff.original),
                    "modified": self._normalized_json(diff.modified),
                    "current": self._normalized_json(diff.current),
                    "conflicts": diff.conflicts,
                }
                for diff in queryset
            ],
            key=lambda item: item["object"],
        )

    def _diff_identity(self, diff):
        for payload in (diff.modified, diff.current, diff.original):
            if payload and payload.get("mac_address"):
                return self._normalized_text(payload["mac_address"])
        return f"missing:{diff.object_id}"

    def _paired_fixture(self, address_group):
        fixture = {}
        for index, name in enumerate(
            (
                "update",
                "destination_noop",
                "delete",
                "relation_delete",
                "conflict",
                "nonconflict",
                "tagged_update",
                "main_delete",
                "missing_delete",
            ),
            start=1,
        ):
            fixture[name] = self._seed_mac(
                f"02:00:00:{address_group}:00:{index:02X}",
                logical_name=name,
            )
        fixture["create_address"] = f"02:00:00:{address_group}:01:00"
        self.logical_macs[fixture["create_address"]] = "create"
        return fixture

    @staticmethod
    def _collapsed_branch_changes(branch):
        collapsed, _ = SquashMergeStrategy._collapse_changes(
            branch.get_unmerged_changes().order_by("time"),
            logging.getLogger("forward_netbox.tests.set_merge"),
        )
        return list(collapsed.values())

    def _direct_range(self, branch, ingestion):
        request = RequestFactory().get(reverse("home"))
        request.user = self.user
        request.id = uuid.uuid4()
        decision = set_based_merge_decision(sync=ingestion.sync, branch=branch)
        self.assertTrue(decision.enabled, decision)
        return apply_set_based_mac_range(
            branch=branch,
            collapsed_changes=self._collapsed_branch_changes(branch),
            request=request,
            decision=decision,
        )

    def test_selection_is_default_off_and_honors_model_kill_switch(self):
        branch, ingestion = self._branch_ingestion(suffix="selection", enabled=False)
        decision = set_based_merge_decision(sync=ingestion.sync, branch=branch)
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "disabled_by_default")

        ingestion.sync.parameters["enable_set_based_merge"] = True
        ingestion.sync.parameters["set_based_merge_kill_switches"] = ["dcim.macaddress"]
        decision = set_based_merge_decision(sync=ingestion.sync, branch=branch)
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "model_kill_switch")

        ingestion.sync.parameters["set_based_merge_kill_switches"] = []
        with patch(
            "forward_netbox.utilities.merge_set_based."
            "set_based_merge_runtime_version_tuple",
            return_value=("4.6.6", "1.1.1", ()),
        ):
            decision = set_based_merge_decision(
                sync=ingestion.sync,
                branch=branch,
            )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_netbox_version")

        decision = set_based_merge_decision(
            sync=ingestion.sync,
            branch=branch,
            model_string="dcim.interface",
        )
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "model_not_allowlisted")

        def unexpected_receiver(sender, instance, **kwargs):
            return None

        dispatch_uid = "forward-netbox-set-merge-unexpected-signal-test"
        post_save.connect(
            unexpected_receiver,
            sender=MACAddress,
            weak=False,
            dispatch_uid=dispatch_uid,
        )
        try:
            decision = set_based_merge_decision(
                sync=ingestion.sync,
                branch=branch,
            )
        finally:
            post_save.disconnect(sender=MACAddress, dispatch_uid=dispatch_uid)
        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unexpected_model_signal_receivers")

    def test_existing_create_primary_mac_and_missing_gfk_fail_closed(self):
        branch, ingestion = self._branch_ingestion(
            suffix="existing-create", enabled=True
        )
        self.request.id = uuid.uuid4()
        with activate_branch(branch), event_tracking(self.request):
            branch_mac = MACAddress.objects.create(
                mac_address="02:00:00:20:00:01",
                assigned_object_type=self.interface_type,
                assigned_object_id=self.interfaces[0].pk,
            )
        main_collision = MACAddress.objects.create(
            pk=branch_mac.pk,
            mac_address="02:00:00:20:00:02",
            assigned_object_type=self.interface_type,
            assigned_object_id=self.interfaces[0].pk,
        )
        result = self._direct_range(branch, ingestion)
        self.assertEqual(result.applied, ())
        self.assertEqual(len(result.fallback), 1)
        self.assertEqual(
            result.fallback_reason_counts,
            {"existing_create_requires_lineage": 1},
        )
        main_collision.refresh_from_db()
        self.assertEqual(str(main_collision.mac_address), "02:00:00:20:00:02")

        primary_target = self._seed_mac("02:00:00:20:00:03")
        primary_branch, primary_ingestion = self._branch_ingestion(
            suffix="primary", enabled=True
        )
        self.request.id = uuid.uuid4()
        with activate_branch(primary_branch), event_tracking(self.request):
            branch_target = MACAddress.objects.get(pk=primary_target.pk)
            branch_target.snapshot()
            branch_target.assigned_object_id = self.interfaces[1].pk
            branch_target.save()
        primary_interface = Interface.objects.get(pk=self.interfaces[0].pk)
        primary_interface.primary_mac_address = primary_target
        primary_interface.save()
        result = self._direct_range(primary_branch, primary_ingestion)
        self.assertEqual(result.applied, ())
        self.assertEqual(len(result.fallback), 1)
        self.assertEqual(
            result.fallback_reason_counts,
            {"primary_mac_semantics": 1},
            [
                (change.prechange_data, change.postchange_data)
                for change in result.fallback
            ],
        )
        primary_target.refresh_from_db()
        self.assertEqual(primary_target.assigned_object_id, self.interfaces[0].pk)
        source_diff = ChangeDiff.objects.get(
            branch=primary_branch,
            object_type=self.mac_type,
            object_id=primary_target.pk,
        )
        source_diff.modified = {
            **source_diff.modified,
            "assigned_object_id": self.interfaces[3].pk,
        }
        source_diff.save(update_fields=["modified", "last_updated"])
        result = self._direct_range(primary_branch, primary_ingestion)
        self.assertEqual(result.applied, ())
        self.assertEqual(
            result.fallback_reason_counts,
            {"source_changediff_mismatch": 1},
        )

        missing_target = self._seed_mac("02:00:00:20:00:04")
        missing_branch, missing_ingestion = self._branch_ingestion(
            suffix="missing-gfk", enabled=True
        )
        self.request.id = uuid.uuid4()
        with activate_branch(missing_branch), event_tracking(self.request):
            branch_target = MACAddress.objects.get(pk=missing_target.pk)
            branch_target.snapshot()
            branch_target.assigned_object_id = self.interfaces[2].pk
            branch_target.save()
        Interface.objects.get(pk=self.interfaces[2].pk).delete()
        result = self._direct_range(missing_branch, missing_ingestion)
        self.assertEqual(result.applied, ())
        self.assertEqual(len(result.fallback), 1)
        self.assertEqual(
            result.fallback_reason_counts,
            {"missing_gfk_dependency": 1},
        )
        missing_target.refresh_from_db()
        self.assertEqual(missing_target.assigned_object_id, self.interfaces[0].pk)

    def test_paired_merge_matches_targets_side_tables_audit_lineage_and_conflicts(self):
        current_fixture = self._paired_fixture("00")
        set_fixture = self._paired_fixture("10")
        relation_tag = Tag.objects.create(
            name="set-merge-relation", slug="set-merge-relation"
        )
        current_fixture["relation_delete"].tags.add(relation_tag)
        set_fixture["relation_delete"].tags.add(relation_tag)
        preserved_tag = Tag.objects.create(
            name="set-merge-preserved", slug="set-merge-preserved"
        )
        for fixture in (current_fixture, set_fixture):
            fixture["nonconflict"].tags.add(preserved_tag)
            fixture["tagged_update"].tags.add(preserved_tag)
            Bookmark.objects.create(
                object_type=self.mac_type,
                object_id=fixture["tagged_update"].pk,
                user=self.user,
            )
            JournalEntry.objects.create(
                assigned_object_type=self.mac_type,
                assigned_object_id=fixture["tagged_update"].pk,
                created_by=self.user,
                comments="preserve concurrent operator evidence",
            )
            Subscription.objects.create(
                object_type=self.mac_type,
                object_id=fixture["nonconflict"].pk,
                user=self.user,
            )

        current_branch, current_ingestion = self._branch_ingestion(
            suffix="paired-current", enabled=False
        )
        set_branch, set_ingestion = self._branch_ingestion(
            suffix="paired-set", enabled=True
        )
        competing = provision_branch(user=self.user, name="Set merge competing")
        current_fixture["competing_branch"] = competing
        set_fixture["competing_branch"] = competing

        # Main edits happen after provisioning but before branch writes. The
        # staged ChangeDiffs therefore contain real no-op, nonconflict, and
        # same-field conflict evidence.
        self.request.id = uuid.uuid4()
        with event_tracking(self.request):
            for fixture in (current_fixture, set_fixture):
                noop = MACAddress.objects.get(pk=fixture["destination_noop"].pk)
                noop.assigned_object_id = self.interfaces[1].pk
                noop.save()
                conflict = MACAddress.objects.get(pk=fixture["conflict"].pk)
                conflict.assigned_object_id = self.interfaces[2].pk
                conflict.save()
                nonconflict = MACAddress.objects.get(pk=fixture["nonconflict"].pk)
                nonconflict.description = "operator description survives"
                nonconflict.save()

        self.request.id = uuid.uuid4()
        with activate_branch(competing), event_tracking(self.request):
            for fixture in (current_fixture, set_fixture):
                competing_mac = MACAddress.objects.get(pk=fixture["update"].pk)
                competing_mac.snapshot()
                competing_mac.description = "competing branch description"
                competing_mac.save()
                competing_delete = MACAddress.objects.get(pk=fixture["delete"].pk)
                competing_delete.snapshot()
                competing_delete.description = "delete conflict description"
                competing_delete.assigned_object_id = self.interfaces[2].pk
                competing_delete.save()

        current_created = self._stage(
            current_branch, current_ingestion, current_fixture
        )
        set_created = self._stage(set_branch, set_ingestion, set_fixture)
        expected_source_diff_count = 10
        self.assertEqual(
            len(self._capture_diffs(current_branch)), expected_source_diff_count
        )
        self.assertEqual(
            len(self._capture_diffs(set_branch)), expected_source_diff_count
        )
        for branch, fixture in (
            (current_branch, current_fixture),
            (set_branch, set_fixture),
        ):
            conflict_current = MACAddress.objects.get(pk=fixture["conflict"].pk)
            conflict_current_data = conflict_current.serialize_object(
                exclude=["created", "last_updated"]
            )
            diff = ChangeDiff.objects.get(
                branch=branch,
                object_id=fixture["conflict"].pk,
            )
            diff.current = conflict_current_data
            diff.save()
        conflict_diffs = list(
            ChangeDiff.objects.filter(
                branch=set_branch,
                object_id=set_fixture["conflict"].pk,
            ).values("original", "modified", "current", "conflicts")
        )
        self.assertTrue(
            any(diff["conflicts"] for diff in conflict_diffs),
            conflict_diffs,
        )
        self.assertIn("assigned_object_id", conflict_diffs[0]["conflicts"])
        self.request.id = uuid.uuid4()
        with event_tracking(self.request):
            for fixture in (current_fixture, set_fixture):
                MACAddress.objects.get(pk=fixture["main_delete"].pk).delete()
                MACAddress.objects.get(pk=fixture["missing_delete"].pk).delete()
        Notification.objects.filter(
            object_type=self.mac_type,
            object_id__in=(
                current_fixture["nonconflict"].pk,
                set_fixture["nonconflict"].pk,
            ),
        ).delete()

        current_state = self._capture_merge(
            current_branch,
            current_ingestion,
            current_fixture,
            current_created,
        )
        self.assertEqual(
            len(self._capture_diffs(set_branch)), expected_source_diff_count
        )
        fast_results = []

        def observe_fast_range(**kwargs):
            result = apply_set_based_mac_range(**kwargs)
            fast_results.append(result)
            return result

        with patch(
            "forward_netbox.utilities.merge_set_based." "apply_set_based_mac_range",
            side_effect=observe_fast_range,
        ):
            set_state = self._capture_merge(
                set_branch,
                set_ingestion,
                set_fixture,
                set_created,
            )
        self.assertEqual(len(fast_results), 1)
        self.assertEqual(
            fast_results[0].operation_counts,
            {"I": 1, "U": 3, "N": 2, "D": 1},
        )
        self.assertEqual(
            fast_results[0].fallback_reason_counts,
            {"delete_side_effects": 1, "notification_side_effects": 1},
        )
        competing_delete = next(
            item for item in set_state["competing_diffs"] if item["object"] == "delete"
        )
        self.assertEqual(
            competing_delete["conflicts"],
            None,
        )
        self.assertIsNotNone(competing_delete["current"])
        for evidence_name in current_state:
            with self.subTest(evidence=evidence_name):
                self.assertEqual(set_state[evidence_name], current_state[evidence_name])

    def test_fault_injection_leaves_target_and_all_evidence_uncommitted(self):
        target = self._seed_mac("02:00:00:00:02:01")
        branch, ingestion = self._branch_ingestion(suffix="fault", enabled=True)
        # Stage only one update for this direct range test.
        self.request.id = uuid.uuid4()
        with activate_branch(branch), event_tracking(self.request):
            branch_target = MACAddress.objects.get(pk=target.pk)
            branch_target.snapshot()
            branch_target.assigned_object_id = self.interfaces[1].pk
            branch_target.save()
        collapsed, _ = SquashMergeStrategy._collapse_changes(
            branch.get_unmerged_changes().order_by("time"),
            logging.getLogger("forward_netbox.tests.set_merge"),
        )
        changes = list(collapsed.values())
        decision = set_based_merge_decision(sync=ingestion.sync, branch=branch)
        self.assertTrue(decision.enabled, decision)
        request = RequestFactory().get(reverse("home"))
        request.user = self.user
        request.id = uuid.uuid4()

        def observable_state():
            target.refresh_from_db()
            return {
                "assignment": target.assigned_object_id,
                "audits": list(
                    ObjectChange.objects.filter(request_id=request.id).values()
                ),
                "applied": list(AppliedChange.objects.filter(branch=branch).values()),
                "diffs": list(
                    ChangeDiff.objects.filter(
                        object_type=self.mac_type,
                        object_id=target.pk,
                    )
                    .order_by("pk")
                    .values()
                ),
                "search_cache": list(
                    CachedValue.objects.filter(
                        object_type=self.mac_type,
                        object_id=target.pk,
                    )
                    .order_by("pk")
                    .values()
                ),
            }

        before = observable_state()
        for stage in (
            "after_target_dml",
            "after_audit_lineage",
            "during_change_diff_update",
        ):
            with self.subTest(stage=stage):
                observed_stages = []

                def inject(actual_stage):
                    observed_stages.append(actual_stage)
                    if actual_stage == stage:
                        raise RuntimeError(f"injected {stage}")

                with patch(
                    "forward_netbox.utilities.merge_set_based."
                    "_inject_set_based_merge_fault",
                    side_effect=inject,
                ):
                    result = apply_set_based_mac_range(
                        branch=branch,
                        collapsed_changes=changes,
                        request=request,
                        decision=decision,
                    )
                self.assertFalse(result.applied)
                self.assertEqual(tuple(changes), result.fallback)
                self.assertIn(stage, observed_stages)
                self.assertEqual(observable_state(), before)
