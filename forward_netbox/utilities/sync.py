import hashlib
import logging
from ipaddress import ip_address
from ipaddress import ip_interface

from core.signals import clear_events
from django.apps import apps
from django.core.exceptions import ObjectDoesNotExist
from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.db import transaction
from extras.events import flush_events
from netbox.context import events_queue

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError
from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError
from .module_readiness import module_bay_import_row
from .query_registry import get_query_specs
from .sync_cable import apply_dcim_cable
from .sync_cable import delete_dcim_cable
from .sync_cable import lookup_cable_between
from .sync_contracts import canonical_cable_endpoint_identity
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import validate_row_shape_for_model
from .sync_core_models import apply_dcim_devicerole
from .sync_core_models import apply_dcim_devicetype
from .sync_core_models import apply_dcim_manufacturer
from .sync_core_models import apply_dcim_platform
from .sync_core_models import apply_dcim_site
from .sync_core_models import delete_dcim_devicerole
from .sync_core_models import delete_dcim_devicetype
from .sync_core_models import delete_dcim_manufacturer
from .sync_core_models import delete_dcim_platform
from .sync_core_models import delete_dcim_site
from .sync_device import apply_dcim_device
from .sync_device import apply_dcim_virtualchassis
from .sync_device import delete_dcim_device
from .sync_device import delete_dcim_virtualchassis
from .sync_inventory_module import apply_dcim_inventoryitem
from .sync_inventory_module import apply_dcim_module
from .sync_inventory_module import delete_dcim_inventoryitem
from .sync_inventory_module import delete_dcim_module
from .sync_ipam import apply_ipam_ipaddress
from .sync_ipam import apply_ipam_prefix
from .sync_ipam import apply_ipam_vlan
from .sync_ipam import apply_ipam_vrf
from .sync_ipam import delete_ipam_ipaddress
from .sync_ipam import delete_ipam_prefix
from .sync_ipam import delete_ipam_vlan
from .sync_ipam import delete_ipam_vrf
from .sync_routing import apply_netbox_peering_manager_peeringsession
from .sync_routing import apply_netbox_routing_bgpaddressfamily
from .sync_routing import apply_netbox_routing_bgppeer
from .sync_routing import apply_netbox_routing_bgppeeraddressfamily
from .sync_routing import apply_netbox_routing_ospfarea
from .sync_routing import apply_netbox_routing_ospfinstance
from .sync_routing import apply_netbox_routing_ospfinterface
from .sync_routing import delete_netbox_peering_manager_peeringsession
from .sync_routing import delete_netbox_routing_bgpaddressfamily
from .sync_routing import delete_netbox_routing_bgppeer
from .sync_routing import delete_netbox_routing_bgppeeraddressfamily
from .sync_routing import delete_netbox_routing_ospfarea
from .sync_routing import delete_netbox_routing_ospfinstance
from .sync_routing import delete_netbox_routing_ospfinterface

logger = logging.getLogger("forward_netbox.sync")


class EventsClearer:
    def __init__(self, threshold=100):
        self.threshold = threshold
        self.counter = 0

    def increment(self):
        self.counter += 1
        if self.counter >= self.threshold:
            self.clear()

    def clear(self):
        queued_events = events_queue.get() or {}
        if events := list(queued_events.values()):
            transaction.on_commit(lambda: flush_events(events))
        clear_events.send(sender=None)
        self.counter = 0


class ForwardSyncRunner:
    CONFLICT_WARNING_DETAIL_LIMIT = 20
    MODULE_NATIVE_INVENTORY_PART_TYPES = {
        "FABRIC MODULE",
        "LINE CARD",
        "ROUTING ENGINE",
        "SUPERVISOR",
    }
    FORWARD_BGP_ADDRESS_FAMILY_ALIASES = {
        "ipv4-any": "ipv4-unicast",
        "ipv6-any": "ipv6-unicast",
        "l2vpn-evpn": "l2vpn-evpn",
        "l2vpn-vpls": "l2vpn-vpls",
        "l3vpn-ipv4-any": "vpnv4-unicast",
        "l3vpn-ipv4-unicast": "vpnv4-unicast",
        "l3vpn-ipv6-any": "vpnv6-unicast",
        "l3vpn-ipv6-unicast": "vpnv6-unicast",
        "l3vpn-ipv6-multicast": "vpnv6-multicast",
        "link-state": "link-state",
        "nsap-unicast": "nsap",
    }
    MODEL_CONFLICT_POLICIES = {
        "dcim.site": "reuse_on_unique_conflict",
        "dcim.manufacturer": "reuse_on_unique_conflict",
        "dcim.devicerole": "reuse_on_unique_conflict",
        "dcim.platform": "reuse_on_unique_conflict",
        "dcim.devicetype": "reuse_on_unique_conflict",
        "dcim.inventoryitemrole": "reuse_on_unique_conflict",
        "dcim.cable": "skip_warn_aggregate",
    }

    def __init__(self, sync, ingestion, client, logger_):
        self.sync = sync
        self.ingestion = ingestion
        self.client = client
        self.logger = logger_
        self._content_types = {}
        self._model_coalesce_fields: dict[str, list[list[str]]] = {}
        self._recorded_issue_ids: set[tuple] = set()
        self._failed_dependencies: dict[str, set[tuple]] = {}
        self._aggregated_conflict_warning_counts: dict[tuple[str, str], int] = {}
        self._aggregated_conflict_warning_suppressed: dict[tuple[str, str], int] = {}
        self._aggregated_skip_warning_counts: dict[tuple[str, str], int] = {}
        self._aggregated_skip_warning_suppressed: dict[tuple[str, str], int] = {}
        self.events_clearer = EventsClearer()

    def _record_aggregated_conflict_warning(
        self, *, model_string, reason, warning_message
    ):
        key = (model_string, reason)
        count = self._aggregated_conflict_warning_counts.get(key, 0)
        if count < self.CONFLICT_WARNING_DETAIL_LIMIT:
            self.logger.log_warning(
                warning_message,
                obj=self.sync,
            )
        else:
            self._aggregated_conflict_warning_suppressed[key] = (
                self._aggregated_conflict_warning_suppressed.get(key, 0) + 1
            )
        self._aggregated_conflict_warning_counts[key] = count + 1

    def _emit_aggregated_conflict_warning_summaries(self, model_string):
        for (warning_model, reason), suppressed_count in sorted(
            self._aggregated_conflict_warning_suppressed.items()
        ):
            if warning_model != model_string or suppressed_count <= 0:
                continue
            self.logger.log_warning(
                f"Suppressed {suppressed_count} additional {model_string} conflict warnings "
                f"for `{reason}` after the first {self.CONFLICT_WARNING_DETAIL_LIMIT}.",
                obj=self.sync,
            )

    def _record_aggregated_skip_warning(self, *, model_string, reason, warning_message):
        key = (model_string, reason)
        count = self._aggregated_skip_warning_counts.get(key, 0)
        if count < self.CONFLICT_WARNING_DETAIL_LIMIT:
            self.logger.log_warning(
                warning_message,
                obj=self.sync,
            )
        else:
            self._aggregated_skip_warning_suppressed[key] = (
                self._aggregated_skip_warning_suppressed.get(key, 0) + 1
            )
        self._aggregated_skip_warning_counts[key] = count + 1

    def _emit_aggregated_skip_warning_summaries(self, model_string):
        for (warning_model, reason), suppressed_count in sorted(
            self._aggregated_skip_warning_suppressed.items()
        ):
            if warning_model != model_string or suppressed_count <= 0:
                continue
            self.logger.log_warning(
                f"Suppressed {suppressed_count} additional {model_string} skip warnings "
                f"for `{reason}` after the first {self.CONFLICT_WARNING_DETAIL_LIMIT}.",
                obj=self.sync,
            )

    def _conflict_policy(self, model_string):
        return self.MODEL_CONFLICT_POLICIES.get(model_string, "strict")

    def _is_module_native_inventory_row(self, row):
        if row.get("module_component") is True:
            return True
        return row.get("part_type") in self.MODULE_NATIVE_INVENTORY_PART_TYPES

    def _ipaddress_assignment_skip_reason(self, address):
        try:
            interface = ip_interface(str(address))
        except ValueError:
            return None

        network = interface.network
        ip_address = interface.ip
        if network.version == 4 and network.prefixlen < 31:
            if ip_address == network.network_address:
                return "network-id"
            if ip_address == network.broadcast_address:
                return "broadcast-address"
        if network.version == 6 and network.prefixlen < 127:
            if ip_address == network.network_address:
                return "network-id"
        return None

    def _first_complete_coalesce_set(self, row, coalesce_sets):
        for field_set in coalesce_sets:
            if all(
                field in row and row[field] not in ("", None) for field in field_set
            ):
                return tuple(field_set)
        return None

    def _coalesce_identity(self, model_string, row, coalesce_sets):
        if model_string == "dcim.cable":
            canonical_identity = canonical_cable_endpoint_identity(row)
            if canonical_identity is not None:
                return ("canonical_cable_endpoints", canonical_identity)
        field_set = self._first_complete_coalesce_set(row, coalesce_sets)
        if field_set is None:
            return None
        return (
            field_set,
            tuple((field, row.get(field)) for field in field_set),
        )

    def _split_diff_rows(self, model_string, diff_rows):
        coalesce_sets = self._model_coalesce_fields.get(model_string, [])
        upsert_rows = []
        delete_rows = []

        for diff_row in diff_rows:
            change_type = diff_row.get("type")
            before = diff_row.get("before")
            after = diff_row.get("after")

            if change_type == "ADDED":
                if not isinstance(after, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `after` data for ADDED."
                    )
                upsert_rows.append(after)
                continue

            if change_type == "DELETED":
                if not isinstance(before, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `before` data for DELETED."
                    )
                delete_rows.append(before)
                continue

            if change_type == "MODIFIED":
                if not isinstance(before, dict) or not isinstance(after, dict):
                    raise ForwardQueryError(
                        f"Forward diff row for {model_string} was missing `before`/`after` data for MODIFIED."
                    )
                upsert_rows.append(after)
                if self._coalesce_identity(
                    model_string, before, coalesce_sets
                ) != self._coalesce_identity(model_string, after, coalesce_sets):
                    delete_rows.append(before)
                continue

            raise ForwardQueryError(
                f"Forward diff row for {model_string} had unsupported type `{change_type}`."
            )

        return upsert_rows, delete_rows

    def run(self):
        self.logger.log_info("Starting Forward ingestion sync stage.", obj=self.sync)
        network_id = self.sync.get_network_id()
        snapshot_selector = self.sync.get_snapshot_id()
        snapshot_id = self.sync.resolve_snapshot_id(self.client)
        query_parameters = self.sync.get_query_parameters()
        maps = self.sync.get_maps()

        if not network_id:
            raise ForwardQueryError(
                "Forward sync requires a network ID on the sync or its source."
            )
        if not snapshot_id:
            raise ForwardQueryError(
                "Forward sync requires a snapshot ID for NQE execution."
            )

        snapshot_info = {}
        if snapshot_selector == snapshot_id:
            for snapshot in self.client.get_snapshots(network_id):
                if snapshot["id"] == snapshot_id:
                    snapshot_info = {
                        "id": snapshot["id"],
                        "state": snapshot.get("state") or "",
                        "createdAt": snapshot.get("created_at") or "",
                        "processedAt": snapshot.get("processed_at") or "",
                    }
                    break
        else:
            snapshot_info = self.client.get_latest_processed_snapshot(network_id)

        snapshot_metrics = {}
        try:
            snapshot_metrics = self.client.get_snapshot_metrics(snapshot_id)
        except Exception as exc:
            self.logger.log_warning(
                f"Unable to fetch Forward snapshot metrics for `{snapshot_id}`: {exc}",
                obj=self.sync,
            )

        self.ingestion.snapshot_selector = snapshot_selector
        self.ingestion.snapshot_id = snapshot_id
        self.ingestion.snapshot_info = snapshot_info or {}
        self.ingestion.snapshot_metrics = snapshot_metrics or {}
        self.ingestion.save(
            update_fields=[
                "snapshot_selector",
                "snapshot_id",
                "snapshot_info",
                "snapshot_metrics",
            ]
        )

        self.logger.log_info(
            f"Using snapshot `{snapshot_id}` for network `{network_id}`.",
            obj=self.sync,
        )
        baseline_ingestion = self.sync.latest_baseline_ingestion(
            exclude_ingestion_id=self.ingestion.pk
        )
        if baseline_ingestion is None:
            self.logger.log_info(
                "No eligible diff baseline exists yet; this run will establish the first baseline if it completes without issues.",
                obj=self.sync,
            )
        else:
            self.logger.log_info(
                f"Latest eligible diff baseline is ingestion `{baseline_ingestion.pk}` on snapshot `{baseline_ingestion.snapshot_id}`.",
                obj=self.sync,
            )

        pending_deletes: dict[str, list[dict]] = {}
        used_full = False
        used_diff = False

        for model_string in self.sync.get_model_strings():
            self.logger.log_info(
                f"Starting model ingestion for {model_string}.", obj=self.sync
            )
            try:
                specs = get_query_specs(model_string, maps=maps)
                if specs:
                    self._model_coalesce_fields[model_string] = [
                        list(field_set) for field_set in specs[0].coalesce_fields
                    ] or default_coalesce_fields_for_model(model_string)
                else:
                    self._model_coalesce_fields[model_string] = (
                        default_coalesce_fields_for_model(model_string)
                    )
                self.logger.init_statistics(model_string, 0)
                model_delete_rows = pending_deletes.setdefault(model_string, [])
                model_baseline = self.sync.incremental_diff_baseline(
                    specs=specs,
                    current_snapshot_id=snapshot_id,
                    exclude_ingestion_id=self.ingestion.pk,
                )
                for spec in specs:
                    rows = []
                    delete_rows = []
                    if model_baseline is not None and spec.query_id:
                        try:
                            self.logger.log_info(
                                f"Running Forward NQE diff `{spec.execution_value}` for {model_string} "
                                f"between snapshots `{model_baseline.snapshot_id}` and `{snapshot_id}`.",
                                obj=self.sync,
                            )
                            diff_rows = self.client.run_nqe_diff(
                                query_id=spec.query_id,
                                commit_id=spec.commit_id,
                                before_snapshot_id=model_baseline.snapshot_id,
                                after_snapshot_id=snapshot_id,
                                fetch_all=True,
                            )
                            rows, delete_rows = self._split_diff_rows(
                                model_string, diff_rows
                            )
                            used_diff = True
                            self.logger.log_info(
                                f"Fetched {len(diff_rows)} diff rows for {model_string} from query_id `{spec.execution_value}`.",
                                obj=self.sync,
                            )
                        except (ForwardClientError, ForwardConnectivityError) as exc:
                            self.logger.log_warning(
                                f"Forward NQE diff failed for {model_string} using `{spec.execution_value}`; falling back to full query execution: {exc}",
                                obj=self.sync,
                            )
                            model_baseline = None

                    if model_baseline is None or not spec.query_id:
                        self.logger.log_info(
                            f"Running Forward {spec.execution_mode} `{spec.execution_value}` for {model_string}.",
                            obj=self.sync,
                        )
                        rows = self.client.run_nqe_query(
                            query=spec.query,
                            query_id=spec.query_id,
                            commit_id=spec.commit_id,
                            network_id=network_id,
                            snapshot_id=snapshot_id,
                            parameters=spec.merged_parameters(query_parameters),
                            fetch_all=True,
                        )
                        used_full = True
                        self.logger.log_info(
                            f"Fetched {len(rows)} rows for {model_string} from {spec.execution_mode} `{spec.execution_value}`.",
                            obj=self.sync,
                        )

                    for row in rows:
                        validate_row_shape_for_model(
                            model_string,
                            row,
                            self._model_coalesce_fields[model_string],
                        )
                    for row in delete_rows:
                        validate_row_shape_for_model(
                            model_string,
                            row,
                            self._model_coalesce_fields[model_string],
                        )
                    self.logger.add_statistics_total(
                        model_string, len(rows) + len(delete_rows)
                    )
                    self._apply_model_rows(model_string, rows)
                    model_delete_rows.extend(delete_rows)
                stats = self.logger.log_data.get("statistics", {}).get(model_string, {})
                self.logger.log_info(
                    f"Completed {model_string}: applied={stats.get('applied', 0)} failed={stats.get('failed', 0)} skipped={stats.get('skipped', 0)} total={stats.get('total', 0)}.",
                    obj=self.sync,
                )
            except ForwardQueryError as exc:
                self._record_issue(
                    model_string,
                    str(exc),
                    {},
                    exception=exc,
                )
                self.logger.log_warning(
                    f"Aborted {model_string} due to validation failure: {exc}",
                    obj=self.sync,
                )
                continue
            except ForwardSyncDataError as exc:
                self.logger.log_warning(
                    f"Aborted {model_string} after row failure: {exc}",
                    obj=self.sync,
                )
                continue

        for model_string in reversed(self.sync.get_model_strings()):
            delete_rows = pending_deletes.get(model_string, [])
            if not delete_rows:
                continue
            try:
                self._delete_model_rows(model_string, delete_rows)
            except ForwardSyncDataError as exc:
                self.logger.log_warning(
                    f"Aborted delete phase for {model_string} after row failure: {exc}",
                    obj=self.sync,
                )
                continue

        if used_diff and used_full:
            self.ingestion.sync_mode = "hybrid"
        elif used_diff:
            self.ingestion.sync_mode = "diff"
        else:
            self.ingestion.sync_mode = "full"
        self.ingestion.save(update_fields=["sync_mode"])
        self.logger.log_info("Finished Forward ingestion sync stage.", obj=self.sync)

    def _record_issue(
        self,
        model_string,
        message,
        row,
        *,
        exception=None,
        context=None,
        defaults=None,
    ):
        from ..models import ForwardIngestionIssue

        if exception is not None and getattr(exception, "issue_id", None):
            issue = ForwardIngestionIssue.objects.filter(
                pk=exception.issue_id,
                ingestion=self.ingestion,
            ).first()
            if issue:
                return issue

        exception_name = (
            exception.__class__.__name__
            if exception is not None
            else "ForwardSyncDataError"
        )
        context_data = dict(context or {})
        defaults_data = dict(defaults or {})
        issue_key = (
            self.ingestion.pk if self.ingestion else None,
            ForwardIngestionPhaseChoices.SYNC,
            model_string,
            exception_name,
            str(message),
            str(sorted(context_data.items())),
            str(sorted(defaults_data.items())),
        )
        if issue_key in self._recorded_issue_ids:
            existing = ForwardIngestionIssue.objects.filter(
                ingestion=self.ingestion,
                phase=ForwardIngestionPhaseChoices.SYNC,
                model=model_string,
                message=message,
                exception=exception_name,
                coalesce_fields=context_data,
                defaults=defaults_data,
            ).first()
            if existing:
                if exception is not None and hasattr(exception, "issue_id"):
                    exception.issue_id = existing.pk
                return existing
            return None
        issue = ForwardIngestionIssue.objects.create(
            ingestion=self.ingestion,
            phase=ForwardIngestionPhaseChoices.SYNC,
            model=model_string,
            message=message,
            coalesce_fields=context_data,
            defaults=defaults_data,
            raw_data=row or {},
            exception=exception_name,
        )
        self._recorded_issue_ids.add(issue_key)
        if exception is not None and hasattr(exception, "issue_id"):
            exception.issue_id = issue.pk
        self.logger.log_failure(f"{model_string}: {message}", obj=self.ingestion)
        return issue

    def _dependency_key(self, model_string, row):
        if model_string == "dcim.device":
            return (row.get("name"),)
        if model_string == "dcim.interface":
            return (row.get("device"), row.get("name"))
        if model_string == "dcim.virtualchassis":
            return (row.get("device"), row.get("vc_name") or row.get("name"))
        return None

    def _mark_dependency_failed(self, model_string, row):
        key = self._dependency_key(model_string, row)
        if key and all(item not in (None, "") for item in key):
            self._failed_dependencies.setdefault(model_string, set()).add(key)

    def _dependency_failed(self, model_string, key):
        return key in self._failed_dependencies.get(model_string, set())

    def _update_existing_or_create(
        self,
        model,
        *,
        lookup,
        defaults,
        fallback_lookups=None,
        conflict_policy="strict",
    ):
        return self._coalesce_update_or_create(
            model,
            coalesce_lookups=[lookup, *(fallback_lookups or [])],
            create_values={**lookup, **defaults},
            update_values=defaults,
            conflict_policy=conflict_policy,
        )

    def _coalesce_update_or_create(
        self,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
        conflict_policy="strict",
    ):
        lookups = [lookup for lookup in (coalesce_lookups or []) if lookup]
        if not lookups:
            raise ValueError("At least one coalesce lookup must be provided.")

        update_values = create_values if update_values is None else update_values

        obj = None
        for lookup in lookups:
            obj = self._get_unique_or_raise(model, lookup)
            if obj is not None:
                break

        if obj is None:
            try:
                obj = model(**create_values)
                obj.full_clean()
                obj.save()
                return obj, True
            except IntegrityError:
                if conflict_policy != "reuse_on_unique_conflict":
                    raise
                for retry_lookup in lookups:
                    obj = self._get_unique_or_raise(model, retry_lookup)
                    if obj is not None:
                        break
                if obj is None:
                    raise

        update_fields = []
        for field, value in update_values.items():
            if getattr(obj, field) != value:
                setattr(obj, field, value)
                update_fields.append(field)
        if update_fields:
            obj.full_clean()
            obj.save(update_fields=update_fields)
        return obj, False

    def _get_unique_or_raise(self, model, lookup):
        queryset = model.objects.filter(**lookup).order_by("pk")
        obj = queryset.first()
        if obj is None:
            return None
        if queryset.exclude(pk=obj.pk).exists():
            raise ForwardSearchError(
                f"Ambiguous coalesce lookup for `{model._meta.label_lower}` with {lookup}.",
                model_string=model._meta.label_lower,
                context=lookup,
            )
        return obj

    def _coalesce_lookup(self, row, *fields):
        return {
            field: row[field]
            for field in fields
            if field in row and row[field] not in ("", None)
        }

    def _coalesce_upsert(
        self,
        model_string,
        model,
        *,
        coalesce_lookups,
        create_values,
        update_values=None,
    ):
        return self._coalesce_update_or_create(
            model,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
            conflict_policy=self._conflict_policy(model_string),
        )

    def _coalesce_sets_for(self, model_string, default_sets):
        configured = self._model_coalesce_fields.get(model_string)
        if configured:
            return configured
        return [list(field_set) for field_set in default_sets]

    def _upsert_row(
        self,
        model_string,
        model,
        *,
        row,
        coalesce_sets,
        create_values,
        update_values=None,
    ):
        lookups = [
            self._coalesce_lookup(row, *coalesce_set) for coalesce_set in coalesce_sets
        ]
        return self._coalesce_upsert(
            model_string,
            model,
            coalesce_lookups=lookups,
            create_values=create_values,
            update_values=update_values,
        )

    def _upsert_row_from_defaults(
        self,
        model_string,
        model,
        *,
        row,
        coalesce_sets,
        defaults,
    ):
        return self._upsert_row(
            model_string,
            model,
            row=row,
            coalesce_sets=coalesce_sets,
            create_values=defaults,
            update_values=defaults,
        )

    def _upsert_values_from_defaults(
        self,
        model_string,
        model,
        *,
        values,
        coalesce_sets,
    ):
        lookups = [
            self._coalesce_lookup(values, *coalesce_set)
            for coalesce_set in coalesce_sets
        ]
        return self._coalesce_upsert(
            model_string,
            model,
            coalesce_lookups=lookups,
            create_values=values,
            update_values=values,
        )

    def _apply_model_rows(self, model_string, rows):
        if model_string == "dcim.interface":
            rows = sorted(rows, key=lambda row: bool(row.get("lag")))
        handler_name = f"_apply_{model_string.replace('.', '_')}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            self.logger.log_warning(
                f"No adapter is defined yet for {model_string}; skipping {len(rows)} rows.",
                obj=self.sync,
            )
            return
        self.logger.log_info(
            f"Applying {len(rows)} rows for {model_string}.",
            obj=self.sync,
        )
        for row in rows:
            try:
                with transaction.atomic():
                    result = handler(row)
                self.events_clearer.increment()
                if result is False:
                    self.logger.increment_statistics(model_string, outcome="skipped")
                else:
                    self.logger.increment_statistics(model_string, outcome="applied")
            except ForwardDependencySkipError as exc:
                logger.exception("Failed applying %s row", model_string)
                self.logger.increment_statistics(model_string, outcome="skipped")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                    context=exc.context,
                    defaults=exc.defaults,
                )
            except (ForwardSearchError, ForwardQueryError) as exc:
                logger.exception("Failed applying %s row", model_string)
                self._mark_dependency_failed(model_string, row)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                    context=getattr(exc, "context", {}),
                    defaults=getattr(exc, "defaults", {}),
                )
            except (ValidationError, IntegrityError) as exc:
                logger.exception("Failed applying %s row", model_string)
                self._mark_dependency_failed(model_string, row)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                )
            except Exception as exc:
                logger.exception("Failed applying %s row", model_string)
                self._mark_dependency_failed(model_string, row)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                )
        self.logger.log_info(
            f"Finished applying rows for {model_string}.",
            obj=self.sync,
        )
        self._emit_aggregated_conflict_warning_summaries(model_string)
        self._emit_aggregated_skip_warning_summaries(model_string)
        self.events_clearer.clear()

    def _delete_model_rows(self, model_string, rows):
        handler_name = f"_delete_{model_string.replace('.', '_')}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            self.logger.log_warning(
                f"No delete adapter is defined yet for {model_string}; skipping {len(rows)} rows.",
                obj=self.sync,
            )
            return
        self.logger.log_info(
            f"Deleting {len(rows)} rows for {model_string}.",
            obj=self.sync,
        )
        for row in rows:
            try:
                with transaction.atomic():
                    deleted = handler(row)
                self.events_clearer.increment()
                if deleted:
                    self.logger.increment_statistics(model_string, outcome="applied")
                else:
                    self.logger.increment_statistics(model_string, outcome="skipped")
            except (ForwardSearchError, ForwardQueryError) as exc:
                logger.exception("Failed deleting %s row", model_string)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                    context=getattr(exc, "context", {}),
                    defaults=getattr(exc, "defaults", {}),
                )
            except (ValidationError, IntegrityError) as exc:
                logger.exception("Failed deleting %s row", model_string)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                )
            except Exception as exc:
                logger.exception("Failed deleting %s row", model_string)
                self.logger.increment_statistics(model_string, outcome="failed")
                self._record_issue(
                    model_string,
                    str(exc),
                    row,
                    exception=exc,
                )
        self.logger.log_info(
            f"Finished deleting rows for {model_string}.",
            obj=self.sync,
        )
        self.events_clearer.clear()

    def _ensure_site(self, row):
        from dcim.models import Site

        site, _ = self._upsert_row_from_defaults(
            "dcim.site",
            Site,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.site",
                [("slug",), ("name",)],
            ),
            defaults={"name": row["name"], "slug": row["slug"]},
        )
        return site

    def _ensure_manufacturer(self, row):
        from dcim.models import Manufacturer

        manufacturer, _ = self._upsert_row_from_defaults(
            "dcim.manufacturer",
            Manufacturer,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.manufacturer",
                [("slug",), ("name",)],
            ),
            defaults={"name": row["name"], "slug": row["slug"]},
        )
        return manufacturer

    def _ensure_role(self, row):
        from dcim.models import DeviceRole

        role, _ = self._upsert_row_from_defaults(
            "dcim.devicerole",
            DeviceRole,
            row=row,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.devicerole",
                [("slug",), ("name",)],
            ),
            defaults={
                "name": row["name"],
                "slug": row["slug"],
                "color": row["color"],
            },
        )
        return role

    def _ensure_platform(self, row):
        from dcim.models import Platform

        manufacturer = None
        if row.get("manufacturer"):
            manufacturer = self._ensure_manufacturer(
                {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
            )
        platform_values = {
            "name": row["name"],
            "slug": row["slug"],
            "manufacturer": manufacturer,
        }
        platform, _ = self._upsert_values_from_defaults(
            "dcim.platform",
            Platform,
            values=platform_values,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.platform",
                [("slug",), ("name",)],
            ),
        )
        return platform

    def _ensure_device_type(self, row):
        from dcim.models import DeviceType

        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        model = row["model"]
        slug = row["slug"]
        configured_sets = self._coalesce_sets_for(
            "dcim.devicetype",
            [("manufacturer_slug", "slug"), ("manufacturer_slug", "model")],
        )
        coalesce_lookups = []
        for field_set in configured_sets:
            lookup = {}
            for field_name in field_set:
                if field_name in {"manufacturer", "manufacturer_slug"}:
                    lookup["manufacturer"] = manufacturer
                elif field_name in {"slug", "model"}:
                    lookup[field_name] = row[field_name]
            if lookup:
                coalesce_lookups.append(lookup)
        if not coalesce_lookups:
            raise ForwardQueryError(
                "No usable coalesce lookups were configured for dcim.devicetype."
            )
        device_type_by_model = (
            DeviceType.objects.filter(**coalesce_lookups[1]).order_by("pk").first()
        )
        device_type_by_slug = (
            DeviceType.objects.filter(**coalesce_lookups[0]).order_by("pk").first()
        )

        if (
            device_type_by_model is not None
            and device_type_by_slug is not None
            and device_type_by_model.pk != device_type_by_slug.pk
        ):
            raise ForwardQueryError(
                "Conflicting NetBox device types already exist for "
                f"manufacturer `{manufacturer.name}`: model `{model}` and slug `{slug}` "
                "resolve to different rows."
            )

        create_values = {
            "manufacturer": manufacturer,
            "model": model,
            "slug": slug,
        }
        update_values = {
            "model": model,
            "slug": slug,
        }
        if "part_number" in row:
            part_number = row.get("part_number", "")
            create_values["part_number"] = part_number
            update_values["part_number"] = part_number

        device_type, _ = self._coalesce_upsert(
            "dcim.devicetype",
            DeviceType,
            coalesce_lookups=coalesce_lookups,
            create_values=create_values,
            update_values=update_values,
        )
        return device_type

    def _ensure_vrf(self, row):
        from ipam.models import VRF

        rd = row.get("rd") or None
        values = {
            "name": row["name"],
            "rd": rd,
            "description": row.get("description", ""),
            "enforce_unique": row.get("enforce_unique", False),
        }
        coalesce_sets = [("name",)]
        if rd:
            coalesce_sets.insert(0, ("rd",))

        vrf, _ = self._upsert_values_from_defaults(
            "ipam.vrf",
            VRF,
            values=values,
            coalesce_sets=self._coalesce_sets_for("ipam.vrf", coalesce_sets),
        )
        return vrf

    def _optional_model(self, app_label, model_name, model_string):
        try:
            return apps.get_model(app_label, model_name)
        except LookupError as exc:
            raise ForwardQueryError(
                f"`{model_string}` sync requires the optional `{app_label}` NetBox "
                "plugin to be installed and migrated."
            ) from exc

    def _model_field_values(self, model, values):
        model_fields = {
            field.name
            for field in model._meta.fields
            if not getattr(field, "auto_created", False)
        }
        return {key: value for key, value in values.items() if key in model_fields}

    def _ensure_forward_observed_rir(self):
        from ipam.models import RIR

        values = {"name": "Forward Observed", "slug": "forward-observed"}
        if any(field.name == "is_private" for field in RIR._meta.fields):
            values["is_private"] = True
        rir, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeer",
            RIR,
            values=values,
            coalesce_sets=[("slug",), ("name",)],
        )
        return rir

    def _ensure_asn(self, asn_value):
        from ipam.models import ASN

        try:
            asn_number = int(asn_value)
        except (TypeError, ValueError) as exc:
            raise ForwardQueryError(f"Invalid BGP ASN value `{asn_value}`.") from exc
        existing = self._get_unique_or_raise(ASN, {"asn": asn_number})
        if existing is not None:
            return existing
        rir = self._ensure_forward_observed_rir()
        asn = ASN(asn=asn_number, rir=rir)
        asn.full_clean()
        asn.save()
        return asn

    def _bgp_vrf(self, row):
        return self._routing_vrf(row)

    def _routing_vrf(self, row):
        if not row.get("vrf"):
            return None
        return self._ensure_vrf(
            {
                "name": row["vrf"],
                "rd": None,
                "description": "",
                "enforce_unique": False,
            }
        )

    def _lookup_device_for_routing(self, row, model_string, object_label):
        from dcim.models import Device

        try:
            return Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping {object_label} because dependency `dcim.device` failed for {key}.",
                    model_string=model_string,
                    context={"device": row["device"]},
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for {object_label}.",
                model_string=model_string,
                context={"device": row["device"]},
                data=row,
            ) from exc

    def _host_address(self, address):
        parsed = ip_address(str(address))
        prefix_length = 32 if parsed.version == 4 else 128
        return f"{parsed}/{prefix_length}"

    def _lookup_ipaddress_by_host(self, *, address, vrf):
        from ipam.models import IPAddress

        host = str(ip_address(str(address)))
        lookup = {"address__net_host": host}
        if vrf is None:
            lookup["vrf__isnull"] = True
        else:
            lookup["vrf"] = vrf
        return self._get_unique_or_raise(IPAddress, lookup)

    def _ensure_bgp_peer_ip(self, row, vrf):
        from ipam.models import IPAddress

        neighbor_address = row["neighbor_address"]
        existing = self._lookup_ipaddress_by_host(address=neighbor_address, vrf=vrf)
        if existing is not None:
            return existing
        ip_obj = IPAddress(
            address=self._host_address(neighbor_address),
            vrf=vrf,
            status="active",
        )
        ip_obj.full_clean()
        ip_obj.save()
        return ip_obj

    def _ensure_bgp_router(self, row, device, local_asn):
        BGPRouter = self._optional_model(
            "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
        )
        values = self._model_field_values(
            BGPRouter,
            {
                "name": f"{device.name} AS{local_asn.asn}"[:100],
                "assigned_object_type": self._content_type_for(device.__class__),
                "assigned_object_id": device.pk,
                "asn": local_asn,
            },
        )
        router, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeer",
            BGPRouter,
            values=values,
            coalesce_sets=[("assigned_object_type", "assigned_object_id", "asn")],
        )
        return router

    def _ensure_bgp_scope(self, row, router, vrf):
        BGPScope = self._optional_model(
            "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
        )
        scope, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeer",
            BGPScope,
            values={"router": router, "vrf": vrf},
            coalesce_sets=[("router", "vrf")],
        )
        return scope

    def _bgp_peer_name(self, row):
        name = row.get("name") or f"AS{row['peer_asn']} {row['neighbor_address']}"
        return str(name)[:100]

    def _bgp_peer_comments(self, row):
        lines = ["Observed by Forward from structured BGP neighbor state."]
        for label, key in (
            ("Router ID", "router_id"),
            ("Peer type", "peer_type"),
            ("Peer device", "peer_device"),
            ("Peer VRF", "peer_vrf"),
            ("Peer router ID", "peer_router_id"),
            ("Session state", "session_state"),
            ("Advertised prefixes", "advertised_prefixes"),
            ("Received prefixes", "received_prefixes"),
        ):
            value = row.get(key)
            if value not in ("", None):
                lines.append(f"{label}: {value}")
        return "\n".join(lines)

    def _rib_presence_label(self, value):
        if value in ("", None):
            return None
        if isinstance(value, str):
            return "present" if value.strip().lower() == "true" else "absent"
        return "present" if bool(value) else "absent"

    def _bgp_address_family_comments(self, row):
        lines = ["Observed by Forward from BGP RIB AFI/SAFI state."]
        if row.get("afi_safi") not in ("", None):
            lines.append(f"Forward AFI/SAFI: {row.get('afi_safi')}")
        return "\n".join(lines)

    def _bgp_peer_address_family_comments(self, row):
        lines = [self._bgp_address_family_comments(row)]
        for label, key in (
            ("Adj-RIB-In post-policy", "has_adj_rib_in"),
            ("Adj-RIB-Out post-policy", "has_adj_rib_out"),
        ):
            state = self._rib_presence_label(row.get(key))
            if state:
                lines.append(f"{label}: {state}")
        return "\n".join(lines)

    def _bgp_peer_values(self, row):
        device = self._lookup_device_for_routing(
            row, "netbox_routing.bgppeer", "BGP peer"
        )

        vrf = self._bgp_vrf(row)
        local_asn = self._ensure_asn(row["local_asn"])
        remote_asn = self._ensure_asn(row["peer_asn"])
        peer_ip = self._ensure_bgp_peer_ip(row, vrf)
        router = self._ensure_bgp_router(row, device, local_asn)
        scope = self._ensure_bgp_scope(row, router, vrf)
        status = row.get("status") or ("active" if row.get("enabled") else "offline")
        if status not in {"active", "planned", "offline", "failed"}:
            status = "active" if row.get("enabled") else "offline"
        return {
            "scope": scope,
            "peer": peer_ip,
            "name": self._bgp_peer_name(row),
            "remote_as": remote_asn,
            "local_as": local_asn,
            "enabled": bool(row.get("enabled")),
            "status": status,
            "description": str(row.get("description") or "")[:200],
            "comments": self._bgp_peer_comments(row),
        }

    def _ensure_netbox_routing_bgppeer(self, row):
        BGPPeer = self._optional_model(
            "netbox_routing", "BGPPeer", "netbox_routing.bgppeer"
        )
        values = self._model_field_values(BGPPeer, self._bgp_peer_values(row))
        peer, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeer",
            BGPPeer,
            values=values,
            coalesce_sets=[("scope", "peer"), ("scope", "peer", "name")],
        )
        return peer

    def _normalize_bgp_address_family(self, afi_safi):
        value = str(afi_safi or "").strip()
        if "." in value:
            value = value.rsplit(".", 1)[-1]
        value = value.lower().replace("_", "-")
        if not value:
            raise ForwardQueryError(
                "BGP address-family row did not include `afi_safi`."
            )
        return self.FORWARD_BGP_ADDRESS_FAMILY_ALIASES.get(value, value)

    def _ensure_bgp_scope_for_row(self, row, model_string):
        device = self._lookup_device_for_routing(row, model_string, "BGP scope")
        vrf = self._bgp_vrf(row)
        local_asn = self._ensure_asn(row["local_asn"])
        router = self._ensure_bgp_router(row, device, local_asn)
        return self._ensure_bgp_scope(row, router, vrf)

    def _ensure_bgp_address_family(self, row):
        BGPAddressFamily = self._optional_model(
            "netbox_routing",
            "BGPAddressFamily",
            "netbox_routing.bgpaddressfamily",
        )
        scope = self._ensure_bgp_scope_for_row(row, "netbox_routing.bgpaddressfamily")
        address_family = self._normalize_bgp_address_family(row.get("afi_safi"))
        choices = {
            str(choice[0])
            for choice in BGPAddressFamily._meta.get_field("address_family").choices
        }
        if choices and address_family not in choices:
            raise ForwardQueryError(
                f"Unsupported BGP address family `{row.get('afi_safi')}`.",
                model_string="netbox_routing.bgpaddressfamily",
                context={"afi_safi": row.get("afi_safi")},
                data=row,
            )
        values = self._model_field_values(
            BGPAddressFamily,
            {
                "scope": scope,
                "address_family": address_family,
                "description": "Observed by Forward from BGP RIB AFI/SAFI state.",
                "comments": self._bgp_address_family_comments(row),
            },
        )
        address_family_obj, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgpaddressfamily",
            BGPAddressFamily,
            values=values,
            coalesce_sets=[("scope", "address_family")],
        )
        return address_family_obj

    def _resolve_bgp_address_family_for_delete(self, row):
        BGPAddressFamily = self._optional_model(
            "netbox_routing",
            "BGPAddressFamily",
            "netbox_routing.bgpaddressfamily",
        )
        scope = self._resolve_bgp_scope_for_delete(row)
        if scope is None:
            return None
        return self._get_unique_or_raise(
            BGPAddressFamily,
            {
                "scope": scope,
                "address_family": self._normalize_bgp_address_family(
                    row.get("afi_safi")
                ),
            },
        )

    def _ensure_bgp_peer_address_family(self, row):
        BGPPeerAddressFamily = self._optional_model(
            "netbox_routing",
            "BGPPeerAddressFamily",
            "netbox_routing.bgppeeraddressfamily",
        )
        bgp_peer = self._ensure_netbox_routing_bgppeer(row)
        address_family = self._ensure_bgp_address_family(row)
        values = self._model_field_values(
            BGPPeerAddressFamily,
            {
                "assigned_object_type": self._content_type_for(bgp_peer.__class__),
                "assigned_object_id": bgp_peer.pk,
                "address_family": address_family,
                "enabled": bool(row.get("enabled")),
                "description": "Observed by Forward from BGP RIB AFI/SAFI state.",
                "comments": self._bgp_peer_address_family_comments(row),
            },
        )
        peer_af, _ = self._upsert_values_from_defaults(
            "netbox_routing.bgppeeraddressfamily",
            BGPPeerAddressFamily,
            values=values,
            coalesce_sets=[
                ("assigned_object_type", "assigned_object_id", "address_family")
            ],
        )
        return peer_af

    def _ospf_area_type(self, value):
        area_type = str(value or "").strip()
        if "." in area_type:
            area_type = area_type.rsplit(".", 1)[-1]
        area_type = area_type.lower().replace("_", "-")
        return {
            "backbone": "backbone",
            "stub": "stub",
            "nssa": "nssa",
            "standard": "standard",
        }.get(area_type, "standard")

    def _ospf_process_values(self, row):
        raw_process_id = str(row.get("process_id") or "0").strip() or "0"
        try:
            process_id = int(raw_process_id)
        except ValueError:
            digest_input = "|".join(
                str(value or "")
                for value in (
                    row.get("device"),
                    row.get("vrf"),
                    raw_process_id,
                    row.get("domain"),
                )
            )
            digest = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()
            process_id = 1_000_000 + (int(digest[:8], 16) % 1_000_000_000)
        return process_id, raw_process_id

    def _ospf_instance_comments(self, row, process_label):
        lines = ["Observed by Forward from structured OSPF state."]
        for label, value in (
            ("Forward process ID", process_label),
            ("Forward domain", row.get("domain")),
        ):
            if value not in ("", None):
                lines.append(f"{label}: {value}")
        return "\n".join(lines)

    def _ospf_interface_comments(self, row):
        lines = ["Observed by Forward from structured OSPF neighbor state."]
        for label, key in (
            ("Cost", "cost"),
            ("Role", "role"),
            ("Remote device", "remote_device"),
            ("Remote interface", "remote_interface"),
            ("Remote interface IP", "remote_interface_ip"),
            ("Remote router ID", "remote_router_id"),
        ):
            value = row.get(key)
            if value not in ("", None):
                lines.append(f"{label}: {value}")
        return "\n".join(lines)

    def _ensure_ospf_instance(self, row):
        OSPFInstance = self._optional_model(
            "netbox_routing", "OSPFInstance", "netbox_routing.ospfinstance"
        )
        device = self._lookup_device_for_routing(
            row, "netbox_routing.ospfinstance", "OSPF instance"
        )
        vrf = self._routing_vrf(row)
        process_id, process_label = self._ospf_process_values(row)
        router_id = str(row.get("router_id") or "").strip()
        if not router_id:
            raise ForwardQueryError(
                "OSPF instance row did not include `router_id`.",
                model_string="netbox_routing.ospfinstance",
                context={"device": row.get("device"), "process_id": process_label},
                data=row,
            )
        values = self._model_field_values(
            OSPFInstance,
            {
                "name": (row.get("name") or f"{device.name} OSPF {process_label}")[
                    :100
                ],
                "router_id": router_id,
                "process_id": process_id,
                "device": device,
                "vrf": vrf,
                "comments": row.get("comments")
                or self._ospf_instance_comments(row, process_label),
            },
        )
        instance, _ = self._upsert_values_from_defaults(
            "netbox_routing.ospfinstance",
            OSPFInstance,
            values=values,
            coalesce_sets=[("device", "vrf", "process_id")],
        )
        return instance

    def _ensure_ospf_area(self, row):
        OSPFArea = self._optional_model(
            "netbox_routing", "OSPFArea", "netbox_routing.ospfarea"
        )
        values = self._model_field_values(
            OSPFArea,
            {
                "area_id": str(row.get("area_id")),
                "area_type": self._ospf_area_type(row.get("area_type")),
                "description": "Observed by Forward from structured OSPF state.",
            },
        )
        area, _ = self._upsert_values_from_defaults(
            "netbox_routing.ospfarea",
            OSPFArea,
            values=values,
            coalesce_sets=[("area_id",)],
        )
        return area

    def _ensure_ospf_interface(self, row):
        OSPFInterface = self._optional_model(
            "netbox_routing", "OSPFInterface", "netbox_routing.ospfinterface"
        )
        device = self._lookup_device_for_routing(
            row, "netbox_routing.ospfinterface", "OSPF interface"
        )
        interface = self._lookup_interface(device, row.get("local_interface"))
        if interface is None:
            raise ForwardSearchError(
                f"Unable to find interface `{row.get('local_interface')}` on `{device.name}` for OSPF interface.",
                model_string="netbox_routing.ospfinterface",
                context={
                    "device": device.name,
                    "local_interface": row.get("local_interface"),
                },
                data=row,
            )
        instance = self._ensure_ospf_instance(row)
        area = self._ensure_ospf_area(row)
        values = self._model_field_values(
            OSPFInterface,
            {
                "instance": instance,
                "area": area,
                "interface": interface,
                "priority": None,
                "comments": self._ospf_interface_comments(row),
            },
        )
        ospf_interface, _ = self._upsert_values_from_defaults(
            "netbox_routing.ospfinterface",
            OSPFInterface,
            values=values,
            coalesce_sets=[("interface",)],
        )
        return ospf_interface

    def _ensure_peering_relationship(self, row):
        relationship_slug = row.get("relationship_slug") or ""
        relationship_name = row.get("relationship") or ""
        if not relationship_slug or not relationship_name:
            peer_type = str(row.get("peer_type") or "").upper()
            if "EXTERNAL" in peer_type:
                relationship_name = "External BGP"
                relationship_slug = "external-bgp"
            elif "INTERNAL" in peer_type:
                relationship_name = "Internal BGP"
                relationship_slug = "internal-bgp"
        if not relationship_slug or not relationship_name:
            return None
        Relationship = self._optional_model(
            "netbox_peering_manager",
            "Relationship",
            "netbox_peering_manager.peeringsession",
        )
        relationship, _ = self._upsert_values_from_defaults(
            "netbox_peering_manager.peeringsession",
            Relationship,
            values={
                "name": relationship_name,
                "slug": relationship_slug,
            },
            coalesce_sets=[("slug",), ("name",)],
        )
        return relationship

    def _ensure_vlan(self, *, vid, name, status, site=None):
        from ipam.models import VLAN

        values = {
            "site": site,
            "vid": vid,
            "name": name,
            "status": status,
        }
        vlan, _ = self._upsert_values_from_defaults(
            "ipam.vlan",
            VLAN,
            values=values,
            coalesce_sets=self._coalesce_sets_for(
                "ipam.vlan",
                [("site", "vid")],
            ),
        )
        return vlan

    def _ensure_inventory_item_role(self, row):
        from dcim.models import InventoryItemRole

        role_name = row.get("role")
        if not role_name:
            return None
        role, _ = self._upsert_row_from_defaults(
            "dcim.inventoryitemrole",
            InventoryItemRole,
            row={"name": str(role_name), "slug": row["role_slug"]},
            coalesce_sets=[("slug",), ("name",)],
            defaults={
                "name": str(role_name),
                "slug": row["role_slug"],
                "color": row["role_color"],
            },
        )
        return role

    def _ensure_module_type(self, row):
        from dcim.models.modules import ModuleType

        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        values = {
            "manufacturer": manufacturer,
            "model": row["model"],
            "part_number": row.get("part_number") or "",
            "description": row.get("description") or "",
            "comments": row.get("comments") or "",
        }
        module_type, _ = self._upsert_values_from_defaults(
            "dcim.module",
            ModuleType,
            values=values,
            coalesce_sets=[("manufacturer", "model")],
        )
        return module_type

    def _lookup_module_bay(self, device, module_bay_name):
        return device.modulebays.filter(name=module_bay_name).order_by("pk").first()

    def _ensure_module_bay(self, device, row):
        module_bay = self._lookup_module_bay(device, row["module_bay"])
        if module_bay is not None:
            return module_bay
        import_row = module_bay_import_row(row)
        values = {
            "name": import_row["name"],
            "label": import_row["label"],
            "position": import_row["position"],
            "description": import_row["description"],
        }
        if any(
            field.name == "enabled" for field in device.modulebays.model._meta.fields
        ):
            values["enabled"] = True
        return device.modulebays.create(**values)

    def _content_type_for(self, model):
        from django.contrib.contenttypes.models import ContentType

        key = model._meta.label_lower
        if key not in self._content_types:
            self._content_types[key] = ContentType.objects.get_for_model(model)
        return self._content_types[key]

    def _lookup_interface(self, device, interface_name):
        from dcim.models import Interface

        return Interface.objects.filter(device=device, name=interface_name).first()

    def _delete_by_coalesce(self, model, lookups):
        filtered_lookups = [lookup for lookup in lookups if lookup]
        if not filtered_lookups:
            return False
        obj = None
        for lookup in filtered_lookups:
            obj = self._get_unique_or_raise(model, lookup)
            if obj is not None:
                break
        if obj is None:
            return False
        obj.delete()
        return True

    def _delete_dcim_site(self, row):
        return delete_dcim_site(self, row)

    def _delete_dcim_manufacturer(self, row):
        return delete_dcim_manufacturer(self, row)

    def _delete_dcim_devicerole(self, row):
        return delete_dcim_devicerole(self, row)

    def _delete_dcim_platform(self, row):
        return delete_dcim_platform(self, row)

    def _delete_dcim_devicetype(self, row):
        return delete_dcim_devicetype(self, row)

    def _delete_dcim_device(self, row):
        return delete_dcim_device(self, row)

    def _delete_dcim_virtualchassis(self, row):
        return delete_dcim_virtualchassis(self, row)

    def _delete_extras_taggeditem(self, row):
        from dcim.models import Device
        from extras.models import Tag

        device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
        tag = Tag.objects.filter(slug=row.get("tag_slug")).order_by("pk").first()
        if device is None or tag is None:
            return False
        if tag not in device.tags.all():
            return False
        device.tags.remove(tag)
        return True

    def _delete_dcim_interface(self, row):
        from dcim.models import Device
        from dcim.models import Interface

        device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
        if device is None or not row.get("name"):
            return False
        return self._delete_by_coalesce(
            Interface,
            [{"device": device, "name": row["name"]}],
        )

    def _delete_dcim_cable(self, row):
        return delete_dcim_cable(self, row)

    def _delete_dcim_macaddress(self, row):
        from dcim.models import MACAddress

        mac_address = row.get("mac_address") or row.get("mac")
        if not mac_address:
            return False
        return self._delete_by_coalesce(
            MACAddress,
            [{"mac_address": mac_address}],
        )

    def _delete_ipam_vlan(self, row):
        return delete_ipam_vlan(self, row)

    def _delete_ipam_vrf(self, row):
        return delete_ipam_vrf(self, row)

    def _delete_ipam_prefix(self, row):
        return delete_ipam_prefix(self, row)

    def _delete_ipam_ipaddress(self, row):
        return delete_ipam_ipaddress(self, row)

    def _delete_dcim_inventoryitem(self, row):
        return delete_dcim_inventoryitem(self, row)

    def _delete_dcim_module(self, row):
        return delete_dcim_module(self, row)

    def _resolve_bgp_peer_for_delete(self, row):
        from dcim.models import Device
        from ipam.models import ASN
        from ipam.models import VRF

        BGPRouter = self._optional_model(
            "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
        )
        BGPScope = self._optional_model(
            "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
        )
        BGPPeer = self._optional_model(
            "netbox_routing", "BGPPeer", "netbox_routing.bgppeer"
        )

        device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
        if device is None:
            return None
        local_asn = ASN.objects.filter(asn=row.get("local_asn")).order_by("pk").first()
        if local_asn is None:
            return None
        vrf = None
        if row.get("vrf"):
            vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
            if vrf is None:
                return None
        router = BGPRouter.objects.filter(
            assigned_object_type=self._content_type_for(Device),
            assigned_object_id=device.pk,
            asn=local_asn,
        ).first()
        if router is None:
            return None
        scope = BGPScope.objects.filter(router=router, vrf=vrf).first()
        if scope is None:
            return None
        peer_ip = self._lookup_ipaddress_by_host(
            address=row.get("neighbor_address"), vrf=vrf
        )
        if peer_ip is None:
            return None
        return self._get_unique_or_raise(
            BGPPeer,
            {
                "scope": scope,
                "peer": peer_ip,
            },
        )

    def _resolve_bgp_scope_for_delete(self, row):
        from dcim.models import Device
        from ipam.models import ASN
        from ipam.models import VRF

        BGPRouter = self._optional_model(
            "netbox_routing", "BGPRouter", "netbox_routing.bgppeer"
        )
        BGPScope = self._optional_model(
            "netbox_routing", "BGPScope", "netbox_routing.bgppeer"
        )
        device = Device.objects.filter(name=row.get("device")).order_by("pk").first()
        if device is None:
            return None
        local_asn = ASN.objects.filter(asn=row.get("local_asn")).order_by("pk").first()
        if local_asn is None:
            return None
        vrf = None
        if row.get("vrf"):
            vrf = VRF.objects.filter(name=row["vrf"]).order_by("pk").first()
            if vrf is None:
                return None
        router = BGPRouter.objects.filter(
            assigned_object_type=self._content_type_for(Device),
            assigned_object_id=device.pk,
            asn=local_asn,
        ).first()
        if router is None:
            return None
        return BGPScope.objects.filter(router=router, vrf=vrf).first()

    def _delete_netbox_peering_manager_peeringsession(self, row):
        return delete_netbox_peering_manager_peeringsession(self, row)

    def _delete_netbox_routing_bgppeer(self, row):
        return delete_netbox_routing_bgppeer(self, row)

    def _delete_netbox_routing_bgpaddressfamily(self, row):
        return delete_netbox_routing_bgpaddressfamily(self, row)

    def _delete_netbox_routing_bgppeeraddressfamily(self, row):
        return delete_netbox_routing_bgppeeraddressfamily(self, row)

    def _delete_netbox_routing_ospfinstance(self, row):
        return delete_netbox_routing_ospfinstance(self, row)

    def _delete_netbox_routing_ospfarea(self, row):
        return delete_netbox_routing_ospfarea(self, row)

    def _delete_netbox_routing_ospfinterface(self, row):
        return delete_netbox_routing_ospfinterface(self, row)

    def _apply_dcim_site(self, row):
        return apply_dcim_site(self, row)

    def _apply_dcim_manufacturer(self, row):
        return apply_dcim_manufacturer(self, row)

    def _apply_dcim_platform(self, row):
        return apply_dcim_platform(self, row)

    def _apply_dcim_devicerole(self, row):
        return apply_dcim_devicerole(self, row)

    def _apply_dcim_devicetype(self, row):
        return apply_dcim_devicetype(self, row)

    def _apply_dcim_virtualchassis(self, row):
        return apply_dcim_virtualchassis(self, row)

    def _apply_dcim_device(self, row):
        return apply_dcim_device(self, row)

    def _apply_dcim_interface(self, row):
        from dcim.models import Device
        from dcim.models import Interface

        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping interface `{row.get('name')}` because dependency `dcim.device` failed for {key}.",
                    model_string="dcim.interface",
                    context={"device": row["device"], "name": row.get("name")},
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for interface `{row.get('name')}`.",
                model_string="dcim.interface",
                context={"device": row["device"], "name": row.get("name")},
                data=row,
            ) from exc
        defaults = {
            "device": device,
            "name": row["name"],
            "type": row["type"],
            "enabled": row["enabled"],
            "mtu": row.get("mtu") or None,
            "description": row.get("description") or "",
            "speed": row.get("speed") or None,
        }
        if row.get("lag"):
            if row["lag"] == row["name"]:
                raise ForwardQueryError(
                    f"Interface `{row['name']}` on device `{device.name}` cannot be its own LAG parent.",
                    model_string="dcim.interface",
                    context={
                        "device": device.name,
                        "name": row["name"],
                        "lag": row["lag"],
                    },
                    data=row,
                )
            lag, _ = self._upsert_values_from_defaults(
                "dcim.interface",
                Interface,
                values={
                    "device": device,
                    "name": row["lag"],
                    "type": "lag",
                    "enabled": True,
                    "mtu": None,
                    "description": "",
                    "speed": None,
                },
                coalesce_sets=self._coalesce_sets_for(
                    "dcim.interface",
                    [("device", "name")],
                ),
            )
            defaults["lag"] = lag
        self._upsert_values_from_defaults(
            "dcim.interface",
            Interface,
            values=defaults,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.interface",
                [("device", "name")],
            ),
        )

    def _apply_extras_taggeditem(self, row):
        from dcim.models import Device
        from extras.models import Tag

        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping feature tag `{row.get('tag')}` because dependency `dcim.device` failed for {key}.",
                    model_string="extras.taggeditem",
                    context={"device": row["device"], "tag": row.get("tag")},
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for feature tag `{row.get('tag')}`.",
                model_string="extras.taggeditem",
                context={"device": row["device"], "tag": row.get("tag")},
                data=row,
            ) from exc

        tag, _ = self._upsert_values_from_defaults(
            "extras.taggeditem",
            Tag,
            values={
                "name": row["tag"],
                "slug": row["tag_slug"],
                "color": row["tag_color"],
            },
            coalesce_sets=[("slug",)],
        )
        device.tags.add(tag)

    def _lookup_cable_between(self, interface, remote_interface):
        return lookup_cable_between(self, interface, remote_interface)

    def _apply_dcim_cable(self, row):
        return apply_dcim_cable(self, row)

    def _apply_dcim_macaddress(self, row):
        from dcim.models import Device
        from dcim.models import Interface
        from dcim.models import MACAddress

        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping MAC assignment because dependency `dcim.device` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={
                        "device": row["device"],
                        "interface": row.get("interface"),
                    },
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for MAC assignment.",
                model_string="dcim.macaddress",
                context={"device": row["device"], "interface": row.get("interface")},
                data=row,
            ) from exc
        interface = self._lookup_interface(device, row["interface"])
        if interface is None:
            key = (device.name, row["interface"])
            if self._dependency_failed("dcim.interface", key):
                raise ForwardDependencySkipError(
                    f"Skipping MAC assignment because dependency `dcim.interface` failed for {key}.",
                    model_string="dcim.macaddress",
                    context={"device": device.name, "interface": row["interface"]},
                    data=row,
                )
            raise ForwardSearchError(
                f"Unable to find interface {row['interface']} on device {device.name} for MAC assignment.",
                model_string="dcim.macaddress",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        self._upsert_values_from_defaults(
            "dcim.macaddress",
            MACAddress,
            values={
                "mac_address": row["mac"],
                "assigned_object_type": self._content_type_for(Interface),
                "assigned_object_id": interface.pk,
            },
            coalesce_sets=self._coalesce_sets_for(
                "dcim.macaddress",
                [("mac_address",)],
            ),
        )

    def _apply_ipam_vlan(self, row):
        return apply_ipam_vlan(self, row)

    def _apply_ipam_vrf(self, row):
        return apply_ipam_vrf(self, row)

    def _apply_ipam_prefix(self, row):
        return apply_ipam_prefix(self, row)

    def _apply_ipam_ipaddress(self, row):
        return apply_ipam_ipaddress(self, row)

    def _apply_dcim_inventoryitem(self, row):
        return apply_dcim_inventoryitem(self, row)

    def _apply_dcim_module(self, row):
        return apply_dcim_module(self, row)

    def _apply_netbox_routing_bgppeer(self, row):
        return apply_netbox_routing_bgppeer(self, row)

    def _apply_netbox_routing_bgpaddressfamily(self, row):
        return apply_netbox_routing_bgpaddressfamily(self, row)

    def _apply_netbox_routing_bgppeeraddressfamily(self, row):
        return apply_netbox_routing_bgppeeraddressfamily(self, row)

    def _apply_netbox_routing_ospfinstance(self, row):
        return apply_netbox_routing_ospfinstance(self, row)

    def _apply_netbox_routing_ospfarea(self, row):
        return apply_netbox_routing_ospfarea(self, row)

    def _apply_netbox_routing_ospfinterface(self, row):
        return apply_netbox_routing_ospfinterface(self, row)

    def _apply_netbox_peering_manager_peeringsession(self, row):
        return apply_netbox_peering_manager_peeringsession(self, row)
