import logging

from ..choices import ForwardIngestionPhaseChoices
from ..exceptions import ForwardQueryError
from .query_registry import get_query_specs

logger = logging.getLogger("forward_netbox.sync")


class ForwardSyncRunner:
    def __init__(self, sync, ingestion, client, logger_):
        self.sync = sync
        self.ingestion = ingestion
        self.client = client
        self.logger = logger_
        self._content_types = {}

    def run(self):
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
            specs = get_query_specs(model_string, maps=maps)
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
                query_results.append(rows)
                total_rows += len(rows)
            self.logger.init_statistics(model_string, total_rows)
            for rows in query_results:
                self._apply_model_rows(model_string, rows)

    def _record_issue(self, model_string, message, row):
        from ..models import ForwardIngestionIssue

        ForwardIngestionIssue.objects.create(
            ingestion=self.ingestion,
            phase=ForwardIngestionPhaseChoices.SYNC,
            model=model_string,
            message=message,
            raw_data=row or {},
            exception="ForwardAdapterError",
        )
        self.logger.log_failure(f"{model_string}: {message}", obj=self.ingestion)

    def _update_existing_or_create(self, model, *, lookup, defaults):
        obj = model.objects.filter(**lookup).order_by("pk").first()
        if obj is None:
            return model.objects.create(**lookup, **defaults), True
        for field, value in defaults.items():
            setattr(obj, field, value)
        obj.save(update_fields=list(defaults))
        return obj, False

    def _apply_model_rows(self, model_string, rows):
        handler_name = f"_apply_{model_string.replace('.', '_')}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            self.logger.log_warning(
                f"No adapter is defined yet for {model_string}; skipping {len(rows)} rows.",
                obj=self.sync,
            )
            return
        for row in rows:
            try:
                handler(row)
                self.logger.increment_statistics(model_string)
            except Exception as exc:
                logger.exception("Failed applying %s row", model_string)
                self._record_issue(model_string, str(exc), row)

    def _ensure_site(self, row):
        from dcim.models import Site

        name = row["name"]
        site, _ = self._update_existing_or_create(
            Site,
            lookup={"name": name},
            defaults={"slug": row["slug"]},
        )
        return site

    def _ensure_manufacturer(self, row):
        from dcim.models import Manufacturer

        name = row["name"]
        slug = row["slug"]
        manufacturer = Manufacturer.objects.filter(name=name).order_by("pk").first()
        if manufacturer is None:
            manufacturer = Manufacturer.objects.create(name=name, slug=slug)
            return manufacturer
        if manufacturer.name == name and manufacturer.slug == slug:
            return manufacturer
        if manufacturer.name == name and manufacturer.slug != slug:
            manufacturer.slug = slug
            manufacturer.save(update_fields=["slug"])
        return manufacturer

    def _ensure_role(self, row):
        from dcim.models import DeviceRole

        name = row["name"]
        role, _ = self._update_existing_or_create(
            DeviceRole,
            lookup={"name": name},
            defaults={"slug": row["slug"], "color": row["color"]},
        )
        return role

    def _ensure_platform(self, row):
        from dcim.models import Platform

        manufacturer = None
        if row.get("manufacturer"):
            manufacturer = self._ensure_manufacturer(
                {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
            )
        name = row["name"]
        platform, _ = self._update_existing_or_create(
            Platform,
            lookup={"name": name},
            defaults={
                "slug": row["slug"],
                "manufacturer": manufacturer,
            },
        )
        return platform

    def _ensure_device_type(self, row):
        from dcim.models import DeviceType

        manufacturer = self._ensure_manufacturer(
            {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
        )
        model = row["model"]
        slug = row["slug"]
        device_type_by_model = (
            DeviceType.objects.filter(manufacturer=manufacturer, model=model)
            .order_by("pk")
            .first()
        )
        device_type_by_slug = (
            DeviceType.objects.filter(manufacturer=manufacturer, slug=slug)
            .order_by("pk")
            .first()
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

        device_type = device_type_by_model or device_type_by_slug
        if device_type is None:
            create_kwargs = {
                "manufacturer": manufacturer,
                "model": model,
                "slug": slug,
            }
            if "part_number" in row:
                create_kwargs["part_number"] = row.get("part_number", "")
            return DeviceType.objects.create(**create_kwargs)

        update_fields = []
        if device_type.model != model:
            device_type.model = model
            update_fields.append("model")
        if device_type.slug != slug:
            device_type.slug = slug
            update_fields.append("slug")
        if "part_number" in row:
            part_number = row.get("part_number", "")
            if device_type.part_number != part_number:
                device_type.part_number = part_number
                update_fields.append("part_number")
        if update_fields:
            device_type.save(update_fields=update_fields)
        return device_type

    def _ensure_vrf(self, row):
        from ipam.models import VRF

        name = row["name"]
        vrf, _ = self._update_existing_or_create(
            VRF,
            lookup={"name": name},
            defaults={
                "rd": row.get("rd") or None,
                "description": row.get("description", ""),
                "enforce_unique": row.get("enforce_unique", False),
            },
        )
        return vrf

    def _ensure_vlan(self, *, vid, name, status, site=None):
        from ipam.models import VLAN

        vlan, _ = self._update_existing_or_create(
            VLAN,
            lookup={"site": site, "vid": vid},
            defaults={
                "name": name,
                "status": status,
            },
        )
        return vlan

    def _ensure_inventory_item_role(self, row):
        from dcim.models import InventoryItemRole

        role_name = row.get("role")
        if not role_name:
            return None
        role, _ = self._update_existing_or_create(
            InventoryItemRole,
            lookup={"name": str(role_name)},
            defaults={"slug": row["role_slug"], "color": row["role_color"]},
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
        vc, _ = self._update_existing_or_create(
            VirtualChassis,
            lookup={"name": vc_name},
            defaults={"domain": row.get("vc_domain", row.get("domain", ""))},
        )
        if row.get("device"):
            device = Device.objects.get(name=row["device"])
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

        self._update_existing_or_create(
            Device, lookup={"name": row["name"]}, defaults=defaults
        )

    def _apply_dcim_interface(self, row):
        from dcim.models import Device
        from dcim.models import Interface

        device = Device.objects.get(name=row["device"])
        defaults = {
            "device": device,
            "type": row["type"],
            "enabled": row["enabled"],
            "mtu": row.get("mtu") or None,
            "description": row.get("description") or "",
            "speed": row.get("speed") or None,
        }
        self._update_existing_or_create(
            Interface,
            lookup={"device": device, "name": row["name"]},
            defaults=defaults,
        )

    def _apply_dcim_macaddress(self, row):
        from dcim.models import Device
        from dcim.models import Interface
        from dcim.models import MACAddress

        device = Device.objects.get(name=row["device"])
        interface = self._lookup_interface(device, row["interface"])
        if interface is None:
            raise ForwardQueryError(
                f"Unable to find interface {row['interface']} on device {device.name} for MAC assignment."
            )
        self._update_existing_or_create(
            MACAddress,
            lookup={"mac_address": row["mac"]},
            defaults={
                "assigned_object_type": self._content_type_for(Interface),
                "assigned_object_id": interface.pk,
            },
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
        self._update_existing_or_create(
            Prefix,
            lookup={"prefix": row["prefix"], "vrf": vrf},
            defaults={"status": row["status"]},
        )

    def _apply_ipam_ipaddress(self, row):
        from dcim.models import Device
        from dcim.models import Interface
        from ipam.models import IPAddress

        device = Device.objects.get(name=row["device"])
        interface = self._lookup_interface(device, row["interface"])
        if interface is None:
            raise ForwardQueryError(
                f"Unable to find interface {row['interface']} on device {device.name} for IP assignment."
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
        self._update_existing_or_create(
            IPAddress,
            lookup={"address": row["address"], "vrf": vrf},
            defaults={
                "status": row["status"],
                "assigned_object_type": self._content_type_for(Interface),
                "assigned_object_id": interface.pk,
            },
        )

    def _apply_dcim_inventoryitem(self, row):
        from dcim.models import Device
        from dcim.models import InventoryItem

        device = Device.objects.get(name=row["device"])
        manufacturer = None
        if row.get("manufacturer"):
            manufacturer = self._ensure_manufacturer(
                {"name": row["manufacturer"], "slug": row["manufacturer_slug"]}
            )
        role = self._ensure_inventory_item_role(row)
        self._update_existing_or_create(
            InventoryItem,
            lookup={
                "device": device,
                "name": row["name"],
                "part_id": row.get("part_id") or "",
                "serial": row.get("serial") or "",
            },
            defaults={
                "status": row["status"],
                "role": role,
                "manufacturer": manufacturer,
                "discovered": row["discovered"],
                "description": row.get("description") or "",
            },
        )
