import logging

from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardDependencySkipError
from ..exceptions import ForwardQueryError
from ..exceptions import ForwardSearchError
from ..exceptions import ForwardSyncDataError
from .query_registry import get_query_specs
from .sync_contracts import default_coalesce_fields_for_model
from .sync_contracts import validate_row_shape_for_model

logger = logging.getLogger("forward_netbox.sync")


class ForwardSyncRunner:
    MODEL_CONFLICT_POLICIES = {
        "dcim.site": "reuse_on_unique_conflict",
        "dcim.manufacturer": "reuse_on_unique_conflict",
        "dcim.devicerole": "reuse_on_unique_conflict",
        "dcim.platform": "reuse_on_unique_conflict",
        "dcim.devicetype": "reuse_on_unique_conflict",
        "dcim.inventoryitemrole": "reuse_on_unique_conflict",
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

    def _conflict_policy(self, model_string):
        return self.MODEL_CONFLICT_POLICIES.get(model_string, "strict")

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
                query_results = []
                total_rows = 0
                for spec in specs:
                    self.logger.log_info(
                        f"Running Forward {spec.execution_mode} `{spec.execution_value}` for {model_string}.",
                        obj=self.sync,
                    )
                    rows = self.client.run_nqe_query(
                        query=spec.query,
                        query_id=spec.query_id,
                        commit_id=spec.commit_id,
                        snapshot_id=snapshot_id,
                        parameters=spec.merged_parameters(query_parameters),
                        fetch_all=True,
                    )
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
                    query_results.append(rows)
                    total_rows += len(rows)
                self.logger.init_statistics(model_string, total_rows)
                for rows in query_results:
                    self._apply_model_rows(model_string, rows)
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
            field: row[field] for field in fields if field in row and row[field] != ""
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
                handler(row)
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
                raise ForwardSyncDataError(
                    str(exc),
                    model_string=model_string,
                    context=exc.context,
                    defaults=exc.defaults,
                    data=row,
                ) from exc
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
                raise ForwardSyncDataError(
                    str(exc),
                    model_string=model_string,
                    context=getattr(exc, "context", {}),
                    defaults=getattr(exc, "defaults", {}),
                    data=row,
                ) from exc
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
                raise ForwardSyncDataError(
                    str(exc),
                    model_string=model_string,
                    data=row,
                ) from exc
        self.logger.log_info(
            f"Finished applying rows for {model_string}.",
            obj=self.sync,
        )

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

    def _content_type_for(self, model):
        from django.contrib.contenttypes.models import ContentType

        key = model._meta.label_lower
        if key not in self._content_types:
            self._content_types[key] = ContentType.objects.get_for_model(model)
        return self._content_types[key]

    def _lookup_interface(self, device, interface_name):
        from dcim.models import Interface

        return Interface.objects.filter(device=device, name=interface_name).first()

    def _apply_dcim_site(self, row):
        self._ensure_site(row)

    def _apply_dcim_manufacturer(self, row):
        self._ensure_manufacturer(row)

    def _apply_dcim_platform(self, row):
        self._ensure_platform(row)

    def _apply_dcim_devicerole(self, row):
        self._ensure_role(row)

    def _apply_dcim_devicetype(self, row):
        self._ensure_device_type(row)

    def _apply_dcim_virtualchassis(self, row):
        from dcim.models import Device
        from dcim.models import VirtualChassis

        vc_name = row.get("vc_name") or row.get("name")
        vc_values = {
            "name": vc_name,
            "domain": row.get("vc_domain", row.get("domain", "")),
        }
        vc, _ = self._upsert_values_from_defaults(
            "dcim.virtualchassis",
            VirtualChassis,
            values=vc_values,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.virtualchassis",
                [("name",)],
            ),
        )
        if row.get("device"):
            try:
                device = Device.objects.get(name=row["device"])
            except ObjectDoesNotExist as exc:
                key = (row["device"],)
                if self._dependency_failed("dcim.device", key):
                    raise ForwardDependencySkipError(
                        f"Skipping virtual chassis assignment because dependency `dcim.device` failed for {key}.",
                        model_string="dcim.virtualchassis",
                        context={"device": row["device"]},
                        data=row,
                    ) from exc
                raise ForwardSearchError(
                    f"Unable to find device `{row['device']}` for virtual chassis assignment.",
                    model_string="dcim.virtualchassis",
                    context={"device": row["device"]},
                    data=row,
                ) from exc
            defaults = {"virtual_chassis": vc}
            if row.get("vc_position"):
                defaults["vc_position"] = row["vc_position"]
            Device.objects.filter(pk=device.pk).update(**defaults)
        return vc

    def _apply_dcim_device(self, row):
        from dcim.models import Device

        site = self._ensure_site({"name": row["site"], "slug": row["site_slug"]})
        role = self._ensure_role(
            {"name": row["role"], "slug": row["role_slug"], "color": row["role_color"]}
        )
        device_type = self._ensure_device_type(
            {
                "manufacturer": row["manufacturer"],
                "manufacturer_slug": row["manufacturer_slug"],
                "slug": row["device_type_slug"],
                "model": row["device_type"],
                **({"part_number": row["part_number"]} if "part_number" in row else {}),
            }
        )
        platform = None
        if row.get("platform"):
            platform = self._ensure_platform(
                {
                    "name": row["platform"],
                    "manufacturer": row["manufacturer"],
                    "manufacturer_slug": row["manufacturer_slug"],
                    "slug": row["platform_slug"],
                }
            )

        defaults = {
            "name": row["name"],
            "site": site,
            "role": role,
            "device_type": device_type,
            "platform": platform,
            "serial": row.get("serial", ""),
            "status": row.get("status") or "active",
        }
        if row.get("virtual_chassis"):
            defaults["virtual_chassis"] = self._apply_dcim_virtualchassis(
                {"name": row["virtual_chassis"]}
            )
            if row.get("vc_position"):
                defaults["vc_position"] = row["vc_position"]

        self._upsert_values_from_defaults(
            "dcim.device",
            Device,
            values=defaults,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.device",
                [("name",)],
            ),
        )

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
        self._upsert_values_from_defaults(
            "dcim.interface",
            Interface,
            values=defaults,
            coalesce_sets=self._coalesce_sets_for(
                "dcim.interface",
                [("device", "name")],
            ),
        )

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
        site = (
            self._ensure_site({"name": row["site"], "slug": row["site_slug"]})
            if row.get("site")
            else None
        )
        self._ensure_vlan(
            vid=int(row["vid"]),
            name=row["name"],
            status=row["status"],
            site=site,
        )

    def _apply_ipam_vrf(self, row):
        self._ensure_vrf(row)

    def _apply_ipam_prefix(self, row):
        from ipam.models import Prefix

        vrf = (
            self._ensure_vrf(
                {
                    "name": row["vrf"],
                    "rd": None,
                    "description": "",
                    "enforce_unique": False,
                }
            )
            if row.get("vrf")
            else None
        )
        self._upsert_values_from_defaults(
            "ipam.prefix",
            Prefix,
            values={
                "prefix": row["prefix"],
                "vrf": vrf,
                "status": row["status"],
            },
            coalesce_sets=self._coalesce_sets_for(
                "ipam.prefix",
                [("prefix", "vrf")],
            ),
        )

    def _apply_ipam_ipaddress(self, row):
        from dcim.models import Device
        from dcim.models import Interface
        from ipam.models import IPAddress

        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping IP assignment because dependency `dcim.device` failed for {key}.",
                    model_string="ipam.ipaddress",
                    context={
                        "device": row["device"],
                        "interface": row.get("interface"),
                    },
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for IP assignment.",
                model_string="ipam.ipaddress",
                context={"device": row["device"], "interface": row.get("interface")},
                data=row,
            ) from exc
        interface = self._lookup_interface(device, row["interface"])
        if interface is None:
            key = (device.name, row["interface"])
            if self._dependency_failed("dcim.interface", key):
                raise ForwardDependencySkipError(
                    f"Skipping IP assignment because dependency `dcim.interface` failed for {key}.",
                    model_string="ipam.ipaddress",
                    context={"device": device.name, "interface": row["interface"]},
                    data=row,
                )
            raise ForwardSearchError(
                f"Unable to find interface {row['interface']} on device {device.name} for IP assignment.",
                model_string="ipam.ipaddress",
                context={"device": device.name, "interface": row["interface"]},
                data=row,
            )
        vrf = (
            self._ensure_vrf(
                {
                    "name": row["vrf"],
                    "rd": None,
                    "description": "",
                    "enforce_unique": False,
                }
            )
            if row.get("vrf")
            else None
        )
        self._upsert_values_from_defaults(
            "ipam.ipaddress",
            IPAddress,
            values={
                "address": row["address"],
                "vrf": vrf,
                "status": row["status"],
                "assigned_object_type": self._content_type_for(Interface),
                "assigned_object_id": interface.pk,
            },
            coalesce_sets=self._coalesce_sets_for(
                "ipam.ipaddress",
                [("address", "vrf")],
            ),
        )

    def _apply_dcim_inventoryitem(self, row):
        from dcim.models import Device
        from dcim.models import InventoryItem

        try:
            device = Device.objects.get(name=row["device"])
        except ObjectDoesNotExist as exc:
            key = (row["device"],)
            if self._dependency_failed("dcim.device", key):
                raise ForwardDependencySkipError(
                    f"Skipping inventory item because dependency `dcim.device` failed for {key}.",
                    model_string="dcim.inventoryitem",
                    context={"device": row["device"], "name": row.get("name")},
                    data=row,
                ) from exc
            raise ForwardSearchError(
                f"Unable to find device `{row['device']}` for inventory item `{row.get('name')}`.",
                model_string="dcim.inventoryitem",
                context={"device": row["device"], "name": row.get("name")},
                data=row,
            ) from exc
        manufacturer = None
        if row.get("manufacturer"):
            manufacturer = self._ensure_manufacturer(
                {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
            )
        role = self._ensure_inventory_item_role(row)
        self._upsert_values_from_defaults(
            "dcim.inventoryitem",
            InventoryItem,
            values={
                "device": device,
                "name": row["name"],
                "part_id": row.get("part_id") or "",
                "serial": row.get("serial") or "",
                "status": row["status"],
                "role": role,
                "manufacturer": manufacturer,
                "discovered": row["discovered"],
                "description": row.get("description") or "",
            },
            coalesce_sets=self._coalesce_sets_for(
                "dcim.inventoryitem",
                [("device", "name", "part_id", "serial")],
            ),
        )
