import json
import logging
from importlib import metadata
from typing import Callable
from typing import TYPE_CHECKING
from typing import TypeVar

from core.choices import DataSourceStatusChoices
from core.exceptions import SyncError
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Interface
from dcim.models import Location
from dcim.models import Manufacturer
from dcim.models import Site
from django.conf import settings
from django.db import transaction
from django.db.models import Model
from django.utils.text import slugify
from django_tables2 import Column
import httpx
from httpx import HTTPStatusError

from ..exceptions import ForwardAPIError

from ..choices import ForwardSourceTypeChoices
from ..exceptions import SearchError
from ..models import apply_tags

if TYPE_CHECKING:
    from ..models import ForwardIngestion
    from ipam.models import IPAddress

logger = logging.getLogger("forward_netbox.utilities.fwdutils")

ModelTypeVar = TypeVar("ModelTypeVar", bound=Model)


class ForwardRESTClient:
    """Thin wrapper around Forward's REST API."""

    DEFAULT_TIMEOUT = 60

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        *,
        verify: bool = True,
        timeout: int | None = None,
        session: httpx.Client | None = None,
        network_id: str | None = None,
    ):
        if not base_url:
            raise ForwardAPIError("Forward Networks base URL is not configured.")

        self.base_url = base_url.rstrip("/")
        self.network_id = network_id
        headers = {
            "Accept": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        headers["User-Agent"] = f"forward-netbox/{metadata.version('forward-netbox')}"
        self._client = session or httpx.Client(
            base_url=self.base_url,
            headers=headers,
            timeout=timeout or self.DEFAULT_TIMEOUT,
            verify=verify,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, path: str, *, params=None, json_body=None):
        try:
            response = self._client.request(
                method,
                path,
                params=params,
                json=json_body,
            )
            response.raise_for_status()
        except HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response else None
            raise ForwardAPIError(
                f"Forward Networks API returned {status_code}: {exc.response.text if exc.response else exc}",
                status_code=status_code,
            ) from exc
        except httpx.HTTPError as exc:
            raise ForwardAPIError(f"Forward Networks API request failed: {exc}") from exc
        if response.content:
            try:
                return response.json()
            except ValueError:
                return response.text
        return None

    @staticmethod
    def _extract_collection(payload):
        if payload is None:
            return []
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "results", "items"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return [payload]

    def _collection(self, path: str, *, snapshot_id=None, filters=None, method="GET"):
        params = {}
        if snapshot_id:
            params["snapshot"] = snapshot_id
        if self.network_id:
            params.setdefault("network", self.network_id)
        json_body = None
        if filters:
            json_body = {"filters": filters}
        try:
            payload = self._request(method, path, params=params, json_body=json_body)
            return self._extract_collection(payload)
        except ForwardAPIError as exc:
            if exc.status_code == 405 and method == "POST":
                # Retry with GET if POST is not supported
                fallback_params = dict(params)
                if filters:
                    fallback_params["filters"] = json.dumps(filters)
                fallback_params.setdefault("network", self.network_id)
                payload = self._request("GET", path, params=fallback_params)
                return self._extract_collection(payload)
            raise

    def list_snapshots(self):
        return self._collection("/api/v1/snapshots")

    def get_snapshot(self, snapshot_id: str):
        payload = self._request("GET", f"/api/v1/snapshots/{snapshot_id}")
        if isinstance(payload, dict):
            return payload.get("data", payload)
        return payload

    def inventory(self, resource: str, snapshot_id: str | None = None, filters=None):
        return self._collection(
            f"/api/v1/inventory/{resource}", snapshot_id=snapshot_id, filters=filters
        )

    def technology(self, resource: str, snapshot_id: str | None = None, filters=None):
        return self._collection(
            f"/api/v1/technology/{resource}", snapshot_id=snapshot_id, filters=filters
        )

    def table(self, table: str, *, filters=None, snapshot_id=None):
        path = f"/api/v1/{table.replace('.', '/')}"
        return self._collection(path, snapshot_id=snapshot_id, filters=filters, method="POST")

    def run_nqe_query(
        self,
        query_id: str,
        *,
        offset: int = 0,
        limit: int = 1000,
        query_options: dict | None = None,
    ) -> tuple[list[dict], int]:
        """Execute an NQE query and return a tuple of (results, total)."""

        params = {}
        if self.network_id:
            params["networkId"] = self.network_id

        payload = {"queryId": query_id}
        options = dict(query_options or {})
        options.setdefault("offset", offset)
        options.setdefault("limit", limit)
        payload["queryOptions"] = options

        response = self._request("POST", "/api/nqe", params=params, json_body=payload)
        if not isinstance(response, dict):
            raise ForwardAPIError("Unexpected response payload when executing NQE query.")

        data = response.get("data") or response.get("results") or response.get("items") or []
        if not isinstance(data, list):
            raise ForwardAPIError("Unexpected NQE response format: query results are not a list.")

        total = response.get("totalNumItems", len(data))
        return data, int(total)

    def get_site_topology(self, site: str, snapshot_id: str, settings: dict | None = None):
        payload = {
            "site": site,
            "snapshot": snapshot_id,
            "settings": settings or {},
        }
        if self.network_id:
            payload["network"] = self.network_id
        response = self._request("POST", "/api/v1/diagram/site", json_body=payload)
        if not isinstance(response, dict):
            raise ForwardAPIError("Unexpected response payload when requesting topology diagram.")
        return response
class Forward(object):
    """High-level helper used by forms/views for Forward REST interactions."""

    def __init__(self, parameters=None) -> None:
        params = parameters or settings.PLUGINS_CONFIG.get("forward_netbox", {})
        base_url = params.get("base_url") or params.get("url")
        token = params.get("auth")
        verify = params.get("verify", True)
        timeout = params.get("timeout")
        self.snapshot_id = params.get("snapshot_id")
        self.network_id = params.get("network_id")
        self.api = ForwardRESTClient(
            base_url=base_url,
            token=token,
            verify=verify,
            timeout=timeout,
            network_id=self.network_id,
        )

    def close(self):
        self.api.close()

    def get_snapshots(self) -> dict:
        formatted_snapshots: dict[str, tuple[str, str]] = {}
        for snapshot in self.api.list_snapshots():
            status = snapshot.get("status")
            finish_status = snapshot.get("finish_status", status)
            if status not in ("done", "loaded") and finish_status not in ("done", "loaded"):
                continue
            ref = snapshot.get("ref") or snapshot.get("snapshot_ref") or snapshot.get("snapshot_id")
            if ref in ("$prev", "$lastLocked"):
                continue
            snapshot_id = snapshot.get("snapshot_id") or ref
            name = snapshot.get("name") or snapshot_id
            end_time = snapshot.get("end") or snapshot.get("finished_at")
            if end_time:
                description = f"{name} - {end_time}"
            else:
                description = name
            formatted_snapshots[ref] = (description, snapshot_id)
        return formatted_snapshots

    def get_sites(self, snapshot=None) -> list[str]:
        snapshot_id = snapshot or self.snapshot_id or "$last"
        raw_sites = self.api.inventory("sites", snapshot_id=snapshot_id)
        return [item.get("siteName") or item.get("name") for item in raw_sites]

    def get_table_data(self, table: str, device: Device):
        filters = {"sn": ["eq", device.serial]}
        snapshot_id = self.snapshot_id or "$last"
        data = self.api.table(table, filters=filters, snapshot_id=snapshot_id)
        columns: list[tuple[str, Column]] = []
        if data:
            for column_name in data[0].keys():
                columns.append((column_name, Column()))
        return data, columns


class ForwardSyncRunner(object):
    """Orchestrates a Forward ingestion using Forward's NQE queries."""

    NQE_SEQUENCE = [
        "dcim.manufacturer",
        "dcim.devicerole",
        "dcim.devicetype",
        "dcim.location",
        "dcim.device",
        "dcim.interface",
    ]
    NQE_PAGE_SIZE = 1000

    def __init__(
        self,
        sync,
        client: ForwardRESTClient | None = None,
        ingestion=None,
        settings: dict | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or {}
        self.ingestion = ingestion
        self.sync = sync
        self.nqe_map = sync.get_nqe_map()
        if hasattr(self.sync, "logger"):
            self.logger = self.sync.logger
        else:
            self.logger = logger

        if self.sync.snapshot_data.status != "loaded":
            raise SyncError("Snapshot not loaded in Forward.")

        self.manufacturer_cache: dict[str, Manufacturer] = {}
        self.role_cache: dict[str, DeviceRole] = {}
        self.device_type_cache: dict[str, DeviceType] = {}
        self.device_cache: dict[str, Device] = {}
        self.location_cache: dict[tuple[int | None, str], Location] = {}
        self.site_cache: dict[str, Site] = {}

        self._handlers = {
            "dcim.manufacturer": self._sync_manufacturer,
            "dcim.devicerole": self._sync_devicerole,
            "dcim.devicetype": self._sync_devicetype,
            "dcim.location": self._sync_location,
            "dcim.device": self._sync_device,
            "dcim.interface": self._sync_interface,
        }

    @staticmethod
    def handle_errors(func: Callable):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as err:  # pragma: no cover - best effort logging
                logger.error(err, exc_info=True)
                if isinstance(err, SearchError):
                    self.logger.log_failure(
                        f"Aborting syncing `{err.model}`: {err}", obj=self.sync
                    )
                else:
                    self.logger.log_failure(
                        f"Syncing failed with: `{err}`. See logs for more details.",
                        obj=self.sync,
                    )
                self.sync.status = DataSourceStatusChoices.FAILED
                return None

        return wrapper

    def get_db_connection_name(self) -> str | None:
        if self.ingestion:
            return self.ingestion.branch.connection_name
        return None

    def _ensure_client(self) -> ForwardRESTClient:
        if not self.client:
            raise SyncError("Forward REST client is not initialized.")
        return self.client

    def _collect_nqe_records(self, model_key: str, query_id: str) -> list[dict]:
        client = self._ensure_client()
        results: list[dict] = []
        offset = 0
        while True:
            batch, total = client.run_nqe_query(
                query_id, offset=offset, limit=self.NQE_PAGE_SIZE
            )
            if not batch:
                break
            results.extend(batch)
            offset += len(batch)
            if offset >= total:
                break
        return results

    def _upsert(self, model_class, lookup: dict, defaults: dict):
        connection_name = self.get_db_connection_name()
        queryset = model_class.objects.using(connection_name)
        with transaction.atomic(using=connection_name):
            try:
                obj = queryset.get(**lookup)
                obj.snapshot()
                changed = False
                for attr, value in defaults.items():
                    if getattr(obj, attr) != value:
                        setattr(obj, attr, value)
                        changed = True
                if changed:
                    obj.full_clean()
                    obj.save(using=connection_name)
            except model_class.DoesNotExist:
                obj = model_class(**lookup, **defaults)
                obj.full_clean()
                obj.save(using=connection_name)
            apply_tags(obj, self.sync.tags.all(), connection_name)
        return obj

    def _to_bool(self, value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "on"}
        if value is None:
            return False
        return bool(value)

    def _resolve_site(self, value, *, for_model: str, data: dict) -> Site:
        if not value:
            raise SearchError("Site reference is required.", model=for_model, data=data)
        key = str(value).lower()
        if key in self.site_cache:
            return self.site_cache[key]
        connection_name = self.get_db_connection_name()
        queryset = Site.objects.using(connection_name)
        site = queryset.filter(slug__iexact=value).first()
        if not site:
            site = queryset.filter(name__iexact=value).first()
        if not site:
            raise SearchError("Site `{}` not found in NetBox.".format(value), model=for_model, data=data)
        self.site_cache[key] = site
        return site

    def _resolve_manufacturer(self, value, *, for_model: str, data: dict) -> Manufacturer:
        if isinstance(value, dict):
            slug = value.get("slug") or slugify(value.get("name", ""))
            name = value.get("name")
        else:
            slug = str(value) if value else None
            name = str(value) if value else None
        identifier = (slug or name or "").lower()
        if not identifier:
            raise SearchError("Manufacturer reference is required.", model=for_model, data=data)
        if identifier in self.manufacturer_cache:
            return self.manufacturer_cache[identifier]
        connection_name = self.get_db_connection_name()
        queryset = Manufacturer.objects.using(connection_name)
        manufacturer = None
        if slug:
            manufacturer = queryset.filter(slug__iexact=slug).first()
        if not manufacturer and name:
            manufacturer = queryset.filter(name__iexact=name).first()
        if not manufacturer:
            raise SearchError(
                "Manufacturer `{}` not found in NetBox.".format(slug or name),
                model=for_model,
                data=data,
            )
        self.manufacturer_cache[identifier] = manufacturer
        return manufacturer

    def _resolve_role(self, value, *, for_model: str, data: dict) -> DeviceRole:
        if isinstance(value, dict):
            slug = value.get("slug") or slugify(value.get("name", ""))
            name = value.get("name")
        else:
            slug = str(value) if value else None
            name = str(value) if value else None
        identifier = (slug or name or "").lower()
        if not identifier:
            raise SearchError("Device role reference is required.", model=for_model, data=data)
        if identifier in self.role_cache:
            return self.role_cache[identifier]
        connection_name = self.get_db_connection_name()
        queryset = DeviceRole.objects.using(connection_name)
        role = None
        if slug:
            role = queryset.filter(slug__iexact=slug).first()
        if not role and name:
            role = queryset.filter(name__iexact=name).first()
        if not role:
            raise SearchError(
                "Device role `{}` not found in NetBox.".format(slug or name),
                model=for_model,
                data=data,
            )
        self.role_cache[identifier] = role
        return role

    def _resolve_device_type(self, value, *, for_model: str, data: dict) -> DeviceType:
        if isinstance(value, dict):
            slug = value.get("slug") or slugify(value.get("model") or value.get("name", ""))
        else:
            slug = str(value) if value else None
        if not slug:
            raise SearchError("Device type reference is required.", model=for_model, data=data)
        identifier = slug.lower()
        if identifier in self.device_type_cache:
            return self.device_type_cache[identifier]
        connection_name = self.get_db_connection_name()
        queryset = DeviceType.objects.using(connection_name)
        device_type = queryset.filter(slug__iexact=slug).first()
        if not device_type:
            raise SearchError(
                "Device type `{}` not found in NetBox.".format(slug),
                model=for_model,
                data=data,
            )
        self.device_type_cache[identifier] = device_type
        return device_type

    def _resolve_device(self, name: str, *, for_model: str, data: dict) -> Device:
        if not name:
            raise SearchError("Device reference is required.", model=for_model, data=data)
        identifier = name.lower()
        if identifier in self.device_cache:
            return self.device_cache[identifier]
        connection_name = self.get_db_connection_name()
        queryset = Device.objects.using(connection_name)
        device = queryset.filter(name__iexact=name).first()
        if not device:
            raise SearchError("Device `{}` not found in NetBox.".format(name), model=for_model, data=data)
        self.device_cache[identifier] = device
        return device

    def _resolve_location(self, value, *, site: Site, for_model: str, data: dict) -> Location | None:
        if value in (None, ""):
            return None
        if isinstance(value, dict):
            slug = value.get("slug") or slugify(value.get("name", ""))
            name = value.get("name")
        else:
            slug = str(value) if value else None
            name = str(value) if value else None
        identifier = (slug or name or "").lower()
        cache_key = (site.pk if site else None, identifier)
        if cache_key in self.location_cache:
            return self.location_cache[cache_key]
        connection_name = self.get_db_connection_name()
        queryset = Location.objects.using(connection_name).filter(site=site)
        location = None
        if slug:
            location = queryset.filter(slug__iexact=slug).first()
        if not location and name:
            location = queryset.filter(name__iexact=name).first()
        if not location:
            raise SearchError(
                "Location `{}` not found in site `{}`.".format(slug or name, site),
                model=for_model,
                data=data,
            )
        self.location_cache[cache_key] = location
        return location

    @handle_errors
    def _sync_manufacturer(self, item: dict):
        name = item.get("name")
        if not name:
            raise SearchError("Manufacturer name is required.", model="dcim.manufacturer", data=item)
        slug = item.get("slug") or slugify(name)
        defaults = {"name": name, "slug": slug}
        for field in ("description", "comments"):
            if item.get(field) is not None:
                defaults[field] = item[field]
        manufacturer = self._upsert(Manufacturer, {"slug": slug}, defaults)
        self.manufacturer_cache[slug.lower()] = manufacturer
        return manufacturer

    @handle_errors
    def _sync_devicerole(self, item: dict):
        name = item.get("name")
        if not name:
            raise SearchError("Device role name is required.", model="dcim.devicerole", data=item)
        slug = item.get("slug") or slugify(name)
        defaults = {"name": name, "slug": slug}
        if item.get("color") is not None:
            defaults["color"] = item["color"]
        if item.get("description") is not None:
            defaults["description"] = item["description"]
        role = self._upsert(DeviceRole, {"slug": slug}, defaults)
        self.role_cache[slug.lower()] = role
        return role

    @handle_errors
    def _sync_devicetype(self, item: dict):
        model_name = item.get("model") or item.get("name")
        if not model_name:
            raise SearchError("Device type model is required.", model="dcim.devicetype", data=item)
        slug = item.get("slug") or slugify(model_name)
        manufacturer_value = item.get("manufacturer")
        manufacturer = self._resolve_manufacturer(
            manufacturer_value, for_model="dcim.devicetype", data=item
        )
        lookup = {"slug": slug, "manufacturer": manufacturer}
        defaults = {"model": model_name, "slug": slug, "manufacturer": manufacturer}
        for field in ("part_number", "u_height", "is_full_depth", "subdevice_role", "comments"):
            if item.get(field) is not None:
                defaults[field] = item[field]
        device_type = self._upsert(DeviceType, lookup, defaults)
        self.device_type_cache[slug.lower()] = device_type
        return device_type

    @handle_errors
    def _sync_location(self, item: dict):
        name = item.get("name")
        if not name:
            raise SearchError("Location name is required.", model="dcim.location", data=item)
        site_value = item.get("site")
        site = self._resolve_site(site_value, for_model="dcim.location", data=item)
        slug = item.get("slug") or slugify(name)
        parent_value = item.get("parent")
        parent = None
        if parent_value:
            parent = self._resolve_location(parent_value, site=site, for_model="dcim.location", data=item)
        lookup = {"slug": slug, "site": site}
        defaults = {"name": name, "slug": slug, "site": site}
        if parent:
            defaults["parent"] = parent
        if item.get("status") is not None:
            defaults["status"] = item["status"]
        if item.get("description") is not None:
            defaults["description"] = item["description"]
        location = self._upsert(Location, lookup, defaults)
        cache_key = (site.pk, slug.lower())
        self.location_cache[cache_key] = location
        return location

    @handle_errors
    def _sync_device(self, item: dict):
        name = item.get("name")
        if not name:
            raise SearchError("Device name is required.", model="dcim.device", data=item)
        device_type_value = item.get("device_type")
        device_type = self._resolve_device_type(device_type_value, for_model="dcim.device", data=item)
        role_value = item.get("role")
        role = self._resolve_role(role_value, for_model="dcim.device", data=item)
        site_value = item.get("site")
        site = None
        if site_value:
            site = self._resolve_site(site_value, for_model="dcim.device", data=item)
        location_value = item.get("location")
        location = None
        if location_value:
            if not site and isinstance(location_value, dict) and location_value.get("site"):
                site = self._resolve_site(location_value.get("site"), for_model="dcim.device", data=item)
            if site:
                location = self._resolve_location(location_value, site=site, for_model="dcim.device", data=item)
        if not site and location:
            site = location.site
        defaults = {"device_type": device_type, "role": role}
        if site:
            defaults["site"] = site
        if location:
            defaults["location"] = location
        for field in ("status", "serial", "asset_tag", "comments"):
            if item.get(field) is not None:
                defaults[field] = item[field]
        device = self._upsert(Device, {"name": name}, defaults)
        self.device_cache[name.lower()] = device
        return device

    @handle_errors
    def _sync_interface(self, item: dict):
        name = item.get("name")
        if not name:
            raise SearchError("Interface name is required.", model="dcim.interface", data=item)
        device_value = item.get("device") or item.get("device_name")
        device = self._resolve_device(device_value, for_model="dcim.interface", data=item)
        lookup = {"device": device, "name": name}
        defaults = {}
        if item.get("type") is not None:
            defaults["type"] = item["type"]
        if item.get("enabled") is not None:
            defaults["enabled"] = self._to_bool(item["enabled"])
        if item.get("mtu") is not None:
            defaults["mtu"] = item["mtu"]
        if item.get("mac_address") is not None:
            defaults["mac_address"] = item["mac_address"]
        if item.get("description") is not None:
            defaults["description"] = item["description"]
        if item.get("speed") is not None:
            try:
                defaults["speed"] = int(item["speed"])
            except (TypeError, ValueError):
                self.logger.log_warning(
                    f"Invalid speed value `{item['speed']}` for interface `{name}` on `{device.name}`.",
                    obj=self.sync,
                )
        interface = self._upsert(Interface, lookup, defaults)
        return interface

    def collect_and_sync(self, ingestion=None) -> None:
        self.logger.log_info("Starting data sync.", obj=self.sync)
        for model_key in self.NQE_SEQUENCE:
            short_model = model_key.split(".", 1)[1]
            if not self.settings.get(short_model):
                self.logger.log_info(
                    f"Skipping `{model_key}` - disabled for this sync.", obj=self.sync
                )
                continue
            query_meta = self.nqe_map.get(model_key, {})
            if not query_meta.get("enabled", True):
                self.logger.log_info(
                    f"Skipping `{model_key}` - disabled in NQE map.", obj=self.sync
                )
                continue
            query_id = query_meta.get("query_id")
            if not query_id:
                self.logger.log_warning(
                    f"No NQE query ID configured for `{model_key}`; skipping.", obj=self.sync
                )
                continue
            records = self._collect_nqe_records(model_key, query_id)
            self.logger.log_info(
                f"Collected {len(records)} records for `{model_key}`.", obj=self.sync
            )
            self.logger.init_statistics(short_model, len(records))
            handler = self._handlers.get(model_key)
            if not handler:
                self.logger.log_warning(
                    f"No handler defined for `{model_key}`; skipping.", obj=self.sync
                )
                continue
            for record in records:
                if handler(record):
                    self.logger.increment_statistics(short_model)
        self.logger.log_info("Data sync completed.", obj=self.sync)
