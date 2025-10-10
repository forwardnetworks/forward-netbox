import json
import logging
from importlib import metadata
from typing import Callable
from typing import TYPE_CHECKING
from typing import TypeVar

from core.choices import DataSourceStatusChoices
from core.exceptions import SyncError
from dcim.models import Device
from dcim.models import Interface
from dcim.models import MACAddress
from django.conf import settings
from django.core.exceptions import MultipleObjectsReturned
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Model
from django.utils.text import slugify
from django_tables2 import Column
from jinja2.sandbox import SandboxedEnvironment
from netbox.config import get_config
from netutils.utils import jinja2_convenience_function

try:
    from forward import FWDClient  # type: ignore
except ModuleNotFoundError:
    FWDClient = None

from ..choices import ForwardSourceTypeChoices
from ..exceptions import create_or_get_sync_issue
from ..exceptions import IPAddressDuplicateError
from ..exceptions import SearchError
from ..exceptions import SyncDataError
from .nbutils import device_serial_max_length
from .nbutils import order_devices
from .nbutils import order_members

if TYPE_CHECKING:
    from ..models import ForwardIngestion
    from ipam.models import IPAddress

logger = logging.getLogger("forward_netbox.utilities.fwdutils")

ModelTypeVar = TypeVar("ModelTypeVar", bound=Model)


def slugify_text(value):
    return slugify(value)


def serial(value):
    sn_length = len(value.get("sn"))
    serial_number = value.get("sn") if sn_length < device_serial_max_length else ""
    if not serial_number:
        serial_number = value.get("id")
    return serial_number


FWD_JINJA_FILTERS = {"slugify": slugify_text, "serial": serial}


def render_jinja2(template_code, context):
    """
    Render a Jinja2 template with the provided context. Return the rendered content.
    """
    environment = SandboxedEnvironment()
    environment.filters.update(get_config().JINJA2_FILTERS)
    environment.filters.update(FWD_JINJA_FILTERS)
    environment.filters.update(jinja2_convenience_function())
    return environment.from_string(source=template_code).render(**context)


class Forward(object):
    def __init__(self, parameters=None) -> None:
        if FWDClient is None:
            raise ImportError(
                "Forward SDK is not installed. Install the `forward` package to enable Forward integrations."
            )
        if parameters:
            self.fwd = FWDClient(**parameters, unloaded=True)
        else:
            self.fwd = FWDClient(
                **settings.PLUGINS_CONFIG["forward_netbox"], unloaded=True
            )
        self.fwd._client.headers[
            "user-agent"
        ] += f'; forward-netbox/{metadata.version("forward-netbox")}'  # noqa: E702

    def get_snapshots(self) -> dict:
        formatted_snapshots = {}
        if self.fwd:
            for snapshot_ref, snapshot in self.fwd.snapshots.items():
                if snapshot.status != "done" and snapshot.finish_status != "done":
                    continue
                if snapshot_ref in ["$prev", "$lastLocked"]:
                    continue
                if snapshot.name:
                    description = (
                        snapshot.name
                        + " - "
                        + snapshot.end.strftime("%d-%b-%y %H:%M:%S")
                    )
                else:
                    description = snapshot.end.strftime("%d-%b-%y %H:%M:%S")

                formatted_snapshots[snapshot_ref] = (description, snapshot.snapshot_id)
        return formatted_snapshots

    def get_sites(self, snapshot=None) -> list:
        if snapshot:
            raw_sites = self.fwd.inventory.sites.all(snapshot_id=snapshot)
        else:
            raw_sites = self.fwd.inventory.sites.all()
        sites = []
        for item in raw_sites:
            sites.append(item["siteName"])
        return sites

    def get_table_data(self, table, device):
        filter = {"sn": ["eq", device.serial]}
        split = table.split(".")

        if len(split) == 2:
            if split[1] == "serial_ports":
                table = getattr(self.fwd.technology, split[1])
            else:
                tech = getattr(self.fwd.technology, split[0])
                table = getattr(tech, split[1])
        else:
            table = getattr(self.fwd.inventory, split[0])

        columns = self.fwd.get_columns(table.endpoint)

        columns.pop(0)

        columns = [(k, Column()) for k in columns]
        data = table.all(
            filters=filter,
        )
        return data, columns


class ForwardSyncRunner(object):
    def __init__(
        self,
        sync,
        client: Forward = None,
        ingestion=None,
        settings: dict = None,
    ) -> None:
        self.client = client
        self.settings = settings
        self.ingestion = ingestion
        self.sync = sync
        self.transform_maps = sync.get_transform_maps(sync.parameters.get("groups"))
        if hasattr(self.sync, "logger"):
            self.logger = self.sync.logger

        if self.sync.snapshot_data.status != "loaded":
            raise SyncError("Snapshot not loaded in Forward.")

    @staticmethod
    def handle_errors(func: Callable):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                # Log the error to logger outside of job - console/file
                logger.error(err, exc_info=True)
                if hasattr(err, "issue_id") and err.issue_id:
                    # The error is already logged to user, no need to log it again
                    return None
                # Logging section for logs inside job - facing user
                self = args[0]
                if isinstance(err, SearchError):
                    if self.settings.get(err.model):
                        self.logger.log_failure(
                            f"Aborting syncing `{err.model}` instance due to above error, please check your transform maps and/or existing data.",
                            obj=self.sync,
                        )
                    else:
                        self.logger.log_failure(
                            f"Syncing `{err.model}` is disabled in settings, but hit above error trying to find the correct item. Please check your transform maps and/or existing data.",
                            obj=self.sync,
                        )
                if isinstance(err, IPAddressDuplicateError):
                    self.logger.log_warning(
                        f"IP Address `{err.data.get('address')}` already exists in `{err.model}` with coalesce fields: `{err.coalesce_fields}`. Please check your transform maps and/or existing data.",
                        obj=self.sync,
                    )
                else:
                    self.logger.log_failure(
                        f"Syncing failed with: `{err}`. See above error for more details.",
                        obj=self.sync,
                    )
                # Make sure the whole sync is failed when we encounter error
                self.sync.status = DataSourceStatusChoices.FAILED
                return None

        return wrapper

    def get_db_connection_name(self) -> str:
        connection_name = None
        if self.ingestion:
            connection_name = self.ingestion.branch.connection_name
        return connection_name

    def get_model_or_update(self, app, model, data):
        transform_map = self.transform_maps.get(
            target_model__app_label=app, target_model__model=model
        )

        if not transform_map:
            raise SystemError(f"No transform map available for {app}: {model}")

        model_settings = self.settings.get(model, False)
        try:
            context = transform_map.get_context(data)
        except Exception as err:
            message = f"Error getting context for `{model}`."
            if isinstance(err, ObjectDoesNotExist):
                message += (
                    " Could not find related object using template in transform maps."
                )
            elif isinstance(err, MultipleObjectsReturned):
                message += " Multiple objects returned using on template in transform maps, the template is not strict enough."
            _, issue = create_or_get_sync_issue(
                exception=err,
                ingestion=self.ingestion,
                message=message,
                model=model,
                data=data,
            )
            raise SearchError(
                message=message, data=data, model=model, issue_id=issue.id
            ) from err

        queryset = transform_map.target_model.model_class().objects

        object = None
        try:
            connection_name = self.get_db_connection_name()
            if model_settings:
                logger.info(f"Creating {model}")
                object = transform_map.update_or_create_instance(
                    context=context,
                    tags=self.sync.tags.all(),
                    connection_name=connection_name,
                )
            else:
                logger.info(f"Getting {model}")
                context.pop("defaults", None)
                object = queryset.using(connection_name).get(**context)
        except queryset.model.DoesNotExist as err:
            message = f"Instance of `{model}` not found."
            _, issue = create_or_get_sync_issue(
                exception=err,
                ingestion=self.ingestion,
                message=message,
                model=model,
                context=context,
                data=data,
            )
            raise SearchError(
                message=message,
                model=model,
                context=context,
                data=data,
                issue_id=issue.id,
            ) from err
        except queryset.model.MultipleObjectsReturned as err:
            message = f"Multiple instances of `{model}` found."
            _, issue = create_or_get_sync_issue(
                exception=err,
                ingestion=self.ingestion,
                message=message,
                model=model,
                context=context,
                data=data,
            )
            raise SearchError(
                message=message,
                model=model,
                context=context,
                data=data,
                issue_id=issue.id,
            ) from err
        except Exception as err:
            _, issue = create_or_get_sync_issue(
                exception=err,
                ingestion=self.ingestion,
                model=model,
                context=context,
                data=data,
            )
            raise SyncDataError(
                model=model, context=context, data=data, issue_id=issue.id
            ) from err

        return object

    def collect_data(self):
        try:
            self.logger.log_info(
                "Collecting information from Forward",
                obj=self.sync.snapshot_data.source,
            )
            data = {}
            if self.sync.snapshot_data.source.type == ForwardSourceTypeChoices.REMOTE:
                self.logger.log_info(
                    "Remote collector checking for snapshot data.", obj=self.sync
                )
                if not self.sync.snapshot_data.fwd_data.count() > 0:
                    raise SyncError(
                        "No snapshot data available. This is a remote sync. Push data to NetBox first."
                    )
                data["site"] = list(
                    self.sync.snapshot_data.fwd_data.filter(type="site").values_list(
                        "data", flat=True
                    )
                )
                data["device"] = list(
                    self.sync.snapshot_data.fwd_data.filter(type="device").values_list(
                        "data", flat=True
                    )
                )
                data["virtualchassis"] = list(
                    self.sync.snapshot_data.fwd_data.filter(
                        type="virtualchassis"
                    ).values_list("data", flat=True)
                )
                data["interface"] = list(
                    self.sync.snapshot_data.fwd_data.filter(
                        type="interface"
                    ).values_list("data", flat=True)
                )
                data["inventoryitem"] = list(
                    self.sync.snapshot_data.fwd_data.filter(
                        type="inventoryitem"
                    ).values_list("data", flat=True)
                )
                data["vlan"] = list(
                    self.sync.snapshot_data.fwd_data.filter(type="vlan").values_list(
                        "data", flat=True
                    )
                )
                data["vrf"] = list(
                    self.sync.snapshot_data.fwd_data.filter(type="vrf").values_list(
                        "data", flat=True
                    )
                )
                data["prefix"] = list(
                    self.sync.snapshot_data.fwd_data.filter(type="prefix").values_list(
                        "data", flat=True
                    )
                )
                data["ipaddress"] = list(
                    self.sync.snapshot_data.fwd_data.filter(
                        type="ipaddress"
                    ).values_list("data", flat=True)
                )
            else:
                self.logger.log_info(
                    "Local collector being used for snapshot data.", obj=self.sync
                )
                excluded_vendors = ["aws", "azure"]

                query_filter = {
                    "and": [{"vendor": ["neq", vendor]} for vendor in excluded_vendors]
                }

                if ingestion_sites := self.settings.get("sites"):
                    site_filter = {
                        "or": [{"siteName": ["eq", site]} for site in ingestion_sites]
                    }
                    query_filter["and"].append(site_filter)

                    self.logger.log_info(
                        f"Creating site filter `{json.dumps(site_filter)}`",
                        obj=self.sync,
                    )
                else:
                    site_filter = {}

                data["site"] = self.client.inventory.sites.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )

                data["device"] = self.client.inventory.devices.all(
                    snapshot_id=self.settings["snapshot_id"], filters=query_filter
                )

                data[
                    "virtualchassis"
                ] = self.client.technology.platforms.stacks_members.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )

                data["interface"] = self.client.inventory.interfaces.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )

                inventory_item_filter = {
                    "and": [
                        {"sn": ["empty", False]},
                        {"name": ["empty", False]},
                    ]
                }
                if site_filter:
                    inventory_item_filter["and"].append(site_filter)

                data["inventoryitem"] = self.client.inventory.pn.all(
                    snapshot_id=self.settings["snapshot_id"],
                    filters=inventory_item_filter,
                )

                data["vlan"] = self.client.technology.vlans.site_summary.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )

                data["vrf"] = self.client.technology.routing.vrf_detail.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )

                if site_filter:
                    networks_filter = {
                        "and": [site_filter, {"and": [{"net": ["empty", False]}]}]
                    }
                else:
                    networks_filter = {"and": [{"net": ["empty", False]}]}
                self.logger.log_info(f"Creating network filter: `{networks_filter}`")
                data["prefix"] = self.client.technology.managed_networks.networks.all(
                    snapshot_id=self.settings["snapshot_id"], filters=networks_filter
                )

                data[
                    "ipaddress"
                ] = self.client.technology.addressing.managed_ip_ipv4.all(
                    snapshot_id=self.settings["snapshot_id"], filters=site_filter
                )
        except Exception as e:
            self.logger.log_failure(
                f"Error collecting data from Forward: {e}", obj=self.sync
            )
            raise SyncError(f"Error collecting data from Forward: {e}")

        self.logger.log_info(
            f"{len(data['site'])} sites collected", obj=self.sync.snapshot_data.source
        )
        self.logger.log_info(
            f"{len(data['device'])} devices collected",
            obj=self.sync.snapshot_data.source,
        )
        self.logger.log_info(
            f"{len(data['virtualchassis'])} stack members collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data['interface'])} interfaces collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data.get('inventoryitem', []))} part numbers collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data.get('vlan', []))} VLANs collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data.get('vrf', []))} VRFs collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data.get('prefix', []))} networks collected",
            obj=self.sync.snapshot_data.source,
        )

        self.logger.log_info(
            f"{len(data.get('ipaddress', []))} management IP's collected",
            obj=self.sync.snapshot_data.source,
        )
        self.logger.log_info("Ordering devices", obj=self.sync)

        members = order_members(data.get("virtualchassis", []))
        devices = order_devices(data.get("device", []), members)

        self.logger.log_info("Ordering Part Numbers", obj=self.sync)

        interface_dict = {}
        for interface in data["interface"]:
            if int_sn := interface.get("sn"):
                if interface_dict.get(int_sn):
                    interface_dict[int_sn].append(interface)
                else:
                    interface_dict[int_sn] = [interface]

        interface_key = "nameOriginal"
        try:
            int_transform_map = self.transform_maps.get(
                target_model__app_label="dcim", target_model__model="interface"
            )
            int_name_field_map = int_transform_map.field_maps.get(target_field="name")
            interface_key = int_name_field_map.source_field
        except Exception as e:
            self.logger.log_failure(
                f"Error collecting information about transform map for interface name: {e}",
                obj=self.sync,
            )
            raise SyncError(f"Error collecting source column name for interface: {e}")

        managed_ips = {}
        for ip in data["ipaddress"]:
            # Find corresponding interface list by serial number (sn)
            device_interfaces = interface_dict.get(ip["sn"], [])

            # Use filter to find the interface with the matching intName
            filtered_interface = list(
                filter(lambda d: d["intName"] == ip["intName"], device_interfaces)
            )

            if filtered_interface:
                ip["nameOriginal"] = filtered_interface[0]["nameOriginal"]
                if ip[interface_key]:
                    int_name = ip[interface_key]
                else:
                    int_name = ip["intName"]
                if ip["sn"] not in managed_ips:
                    managed_ips[ip["sn"]] = {int_name: [ip]}
                elif int_name not in managed_ips.get(ip["sn"]):
                    managed_ips[ip["sn"]][int_name] = [ip]
                else:
                    managed_ips[ip["sn"]][int_name].append(ip)

        for vlan in data["vlan"][:]:
            # Remove VLANs with ID 0, minimum VLAN ID in NetBox is 1
            if vlan.get("vlanId") == 0:
                data["vlan"].remove(vlan)

        for item in data["inventoryitem"][:]:
            # Remove items with empty serial number
            if item.get("sn") in [None, "None"]:
                data["inventoryitem"].remove(item)

        for model, item_count in [
            ("site", len(data.get("site", []))),
            ("device", len(devices)),
            ("interface", len(data.get("interface", []))),
            ("inventoryitem", len(data.get("inventoryitem", []))),
            ("vlan", len(data.get("vlan", []))),
            ("vrf", len(data.get("vrf", []))),
            ("prefix", len(data.get("prefix", []))),
            # TODO: Since we sync only those assigned to interfaces, we are skipping some IPs
            # TODO: This is fixable by syncing IPs separately from interface and only assign them on interfaces
            ("ipaddress", len(data.get("ipaddress", []))),
        ]:
            if self.settings.get(model):
                self.logger.init_statistics(model, item_count)

        return (
            data["site"],
            devices,
            interface_dict,
            data["inventoryitem"],
            data["vrf"],
            data["vlan"],
            data["prefix"],
            managed_ips,
        )

    @handle_errors
    def sync_model(
        self,
        app_label: str,
        model: str,
        data: dict | None,
        stats: bool = True,
        sync: bool = False,
    ) -> ModelTypeVar | None:
        """Sync a single item to NetBox."""
        # The `sync` param is a workaround since we need to get some models (Device...) even when not syncing them.
        if not sync:
            return None

        if not data:
            return None

        instance = self.get_model_or_update(app_label, model, data)

        # Only log when we successfully synced the item and asked for it
        if stats:
            self.logger.increment_statistics(model=model)

        return instance

    def sync_item(
        self,
        item,
        app_label: str,
        model: str,
        cf: bool = False,
        ingestion: "ForwardIngestion" = None,
    ) -> ModelTypeVar | None:
        """Sync a single item to NetBox."""
        synced_object = self.sync_model(
            app_label=app_label,
            model=model,
            data=item,
            sync=self.settings.get(model),
        )
        if synced_object is None:
            return None

        if cf:
            synced_object.snapshot()
            synced_object.custom_field_data[
                "forward_source"
            ] = self.sync.snapshot_data.source.pk
            if ingestion:
                synced_object.custom_field_data["forward_ingestion"] = ingestion.pk
            synced_object.save()

        return synced_object

    def sync_items(
        self,
        items,
        app_label: str,
        model: str,
        cf: bool = False,
        ingestion: "ForwardIngestion" = None,
    ) -> None:
        """Sync list of items to NetBox."""
        if not self.settings.get(model):
            self.logger.log_info(
                f"Did not ask to sync {model}s, skipping.", obj=self.sync
            )
            return

        for item in items:
            self.sync_item(item, app_label, model, cf, ingestion)

    @handle_errors
    def sync_devices(
        self,
        ingestion,
        devices,
        interface_dict,
        managed_ips,
    ):
        for model, name in [
            ("manufacturer", "manufacturers"),
            ("devicetype", "device types"),
            ("platform", "platforms"),
            ("devicerole", "device roles"),
            ("virtualchassis", "virtual chassis"),
            ("device", "devices"),
            ("inventoryitem", "device inventory items"),
        ]:
            if not self.settings.get(model):
                self.logger.log_info(
                    f"Did not ask to sync {name}, skipping", obj=self.sync
                )

        devices_total = len(devices)

        for device in devices:
            self.sync_model(
                "dcim", "manufacturer", device, sync=self.settings.get("manufacturer")
            )
            self.sync_model(
                "dcim", "devicetype", device, sync=self.settings.get("devicetype")
            )
            self.sync_model(
                "dcim", "platform", device, sync=self.settings.get("platform")
            )
            self.sync_model(
                "dcim", "devicerole", device, sync=self.settings.get("devicerole")
            )

            virtual_chassis = device.get("virtual_chassis", {})
            self.sync_model(
                "dcim",
                "virtualchassis",
                virtual_chassis,
                stats=False,
                sync=self.settings.get("virtualchassis"),
            )

            # We need to get a Device instance even when not syncing it but syncing Interfaces, IPs or MACs
            device_object: Device | None = self.sync_model(
                "dcim",
                "device",
                device,
                stats=False,
                sync=self.settings.get("device")
                or self.settings.get("interface")
                or self.settings.get("ipaddress")
                or self.settings.get("macaddress"),
            )

            if device_object and self.settings.get("device"):
                device_object.snapshot()
                if self.sync.update_custom_fields:
                    device_object.custom_field_data[
                        "forward_source"
                    ] = self.sync.snapshot_data.source.pk
                    if ingestion:
                        device_object.custom_field_data[
                            "forward_ingestion"
                        ] = ingestion.pk
                device_object.save()

                self.logger.increment_statistics(model="device")
                logger.info(
                    f"Device {self.logger.log_data.get('statistics', {}).get('device', {}).get('current')} out of {devices_total}"
                )

                # The Device exists now, so we can update the master of the VC.
                # The logic is handled in transform maps.
                self.sync_model(
                    "dcim",
                    "virtualchassis",
                    virtual_chassis,
                    stats=False,
                    sync=self.settings.get("virtualchassis"),
                )

            device_interfaces = interface_dict.get(device.get("sn"), [])
            for device_interface in device_interfaces:
                self.sync_interface(
                    device_interface, managed_ips, device_object, device
                )

    @handle_errors
    def sync_ipaddress(
        self,
        managed_ip: dict | None,
        device_object: Device | None,
        primary_ip: str | None,
        login_ip: str | None,
    ):
        ip_address_obj: "IPAddress | None" = self.sync_model(
            "ipam",
            "ipaddress",
            managed_ip,
            sync=self.settings.get("ipaddress"),
        )
        if ip_address_obj is None:
            return None

        connection_name = self.get_db_connection_name()

        try:
            # Removing another IP is done in .signals.clear_other_primary_ip
            # But do it here too, so the change is shown in StagedChange diff
            other_device = Device.objects.using(connection_name).get(
                primary_ip4=ip_address_obj
            )
            if other_device and device_object != other_device:
                other_device.snapshot()
                other_device.primary_ip4 = None
                other_device.save(using=connection_name)
        except ObjectDoesNotExist:
            pass

        if login_ip == primary_ip:
            try:
                device_object.snapshot()
                device_object.primary_ip4 = ip_address_obj
                device_object.save(using=connection_name)
            except (ValueError, AttributeError) as err:
                self.logger.log_failure(
                    f"Error assigning primary IP to device: {err}", obj=self.sync
                )
                return None
        return ip_address_obj

    @handle_errors
    def sync_macaddress(
        self, data: dict | None, interface_object: Interface
    ) -> MACAddress | None:
        # Need to create MAC Address object before we can assign it to Interface
        # TODO: Figure out how to do this using transform maps
        macaddress_data = {
            "mac": data,
            "id": getattr(interface_object, "pk", None),
        }
        macaddress_object: MACAddress | None = self.sync_model(
            "dcim", "macaddress", macaddress_data, sync=self.settings.get("macaddress")
        )
        if macaddress_object is None:
            return None
        try:
            interface_object.snapshot()
            interface_object.primary_mac_address = macaddress_object
            interface_object.save(using=self.get_db_connection_name())
        except ValueError as err:
            self.logger.log_failure(
                f"Error assigning MAC Address to interface: {err}", obj=self.sync
            )
            return None
        return macaddress_object

    @handle_errors
    def sync_interface(
        self,
        device_interface: dict,
        managed_ips: dict,
        device_object: Device | None,
        device: dict,
    ):
        device_interface["loginIp"] = device.get("loginIp")
        # We need to get an Interface instance even when not syncing it but syncing IPs or MACs
        interface_object: Interface | None = self.sync_model(
            "dcim",
            "interface",
            device_interface,
            sync=self.settings.get("interface")
            or self.settings.get("ipaddress")
            or self.settings.get("macaddress"),
        )

        for ipaddress in managed_ips.get(
            getattr(device_object, "serial", None), {}
        ).get(getattr(interface_object, "name", None), []):
            self.sync_ipaddress(
                ipaddress,
                device_object,
                device_interface.get("primaryIp"),
                device.get("loginIp"),
            )

        self.sync_macaddress(device_interface.get("mac"), interface_object)

        return interface_object

    def collect_and_sync(self, ingestion=None) -> None:
        self.logger.log_info("Starting data sync.", obj=self.sync)
        (
            sites,
            devices,
            interface_dict,
            inventory_items,
            vrfs,
            vlans,
            networks,
            managed_ips,
        ) = self.collect_data()

        self.sync_items(
            app_label="dcim",
            model="site",
            items=sites,
            cf=self.sync.update_custom_fields,
            ingestion=ingestion,
        )
        self.sync_devices(
            ingestion,
            devices,
            interface_dict,
            managed_ips,
        )
        self.sync_items(app_label="dcim", model="inventoryitem", items=inventory_items)
        self.sync_items(app_label="ipam", model="vlan", items=vlans)
        self.sync_items(app_label="ipam", model="vrf", items=vrfs)
        self.sync_items(app_label="ipam", model="prefix", items=networks)
