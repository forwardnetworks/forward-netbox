import logging
import requests
import uuid
from datetime import datetime
from importlib import metadata

from django.conf import settings
from django.db import connections, DEFAULT_DB_ALIAS
from django.db.utils import ProgrammingError
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth import get_user_model
from django.db import models

from netbox_branching.models import Branch
from netbox_branching.choices import BranchStatusChoices
from netbox_branching.utilities import (
    activate_branch,
    update_object,
    record_applied_change,
)
from forward_netbox.models import ForwardNQEMap
from core.choices import DataSourceStatusChoices
from utilities.datetime import local_now

from core.models.change_logging import ObjectChange

logger = logging.getLogger("forward_netbox.utilities.fwd_utils")

MODEL_DEPENDENCIES = [
    "dcim.manufacturer",
    "dcim.devicetype",
    "dcim.device",
]

def sort_models_by_dependencies(models):
    order = {model: i for i, model in enumerate(MODEL_DEPENDENCIES)}
    return sorted(models, key=lambda ct: order.get(f"{ct.app_label}.{ct.model}", len(MODEL_DEPENDENCIES)))

class Forward:
    def __init__(self, parameters=None) -> None:
        plugin_cfg = settings.PLUGINS_CONFIG.get("forward_netbox", {})
        parameters = parameters or {}
        cfg = {**plugin_cfg, **parameters}

        self.base_url = cfg.get("base_url", "").rstrip("/")
        self.auth = cfg.get("auth")
        self.verify = cfg.get("verify", True)
        self.auto_merge = cfg.get("auto_merge", False)
        self.allow_deletes = cfg.get("allow_deletes", False)

        if not self.base_url or not self.auth:
            raise ValueError("Missing required Forward configuration parameters: 'base_url' or 'auth'")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Basic {self.auth}",
            "User-Agent": f"forward-netbox/{metadata.version('forward-netbox')}",
            "Content-Type": "application/json",
        })
        self.session.verify = self.verify
        self._cached_version = None

    def get_networks(self) -> list[dict]:
        try:
            r = self.session.get(f"{self.base_url}/api/networks")
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch networks: {e}")
            return []

    def get_snapshots(self, network_id: str) -> dict:
        formatted = {}
        version = self.get_version().get("version", "")
        try:
            r = self.session.get(f"{self.base_url}/api/networks/{network_id}/snapshots")
            r.raise_for_status()
            for snap in r.json().get("snapshots", []):
                sid = str(snap["id"])
                metrics = {}
                try:
                    m = self.session.get(f"{self.base_url}/api/snapshots/{sid}/metrics")
                    m.raise_for_status()
                    metrics = m.json()
                except requests.RequestException:
                    logger.warning(f"Metrics unavailable for snapshot {sid}")
                formatted[sid] = {
                    "id": sid,
                    "snapshot_id": sid,
                    "note": snap.get("note"),
                    "start": snap.get("start"),
                    "metrics": metrics,
                    "version": version,
                    "status": "processed" if snap.get("state") == "PROCESSED" else "unprocessed",
                }
        except requests.RequestException as e:
            logger.error(f"Failed to fetch snapshots: {e}")
        return formatted

    def resolve_snapshot_id(self, network_id: str, snapshot_id: str) -> str:
        if snapshot_id != "$latestProcessed":
            return snapshot_id
        r = self.session.get(f"{self.base_url}/api/networks/{network_id}/snapshots/latestProcessed")
        r.raise_for_status()
        return str(r.json()["id"])

    def get_version(self) -> dict:
        if self._cached_version is not None:
            return self._cached_version
        try:
            r = self.session.get(f"{self.base_url}/api/version")
            r.raise_for_status()
            self._cached_version = r.json()
        except requests.RequestException as e:
            logger.error(f"Failed to retrieve Forward version: {e}")
            self._cached_version = {}
        return self._cached_version

    def run_nqe_query(self, network_id: str, query_id: str, snapshot_id: str = None) -> list[dict]:
        if snapshot_id == "$latestProcessed":
            snapshot_id = self.resolve_snapshot_id(network_id, snapshot_id)

        offset = 0
        results = []
        while True:
            body = {"queryId": query_id, "queryOptions": {"offset": offset}}
            params = {"networkId": network_id, "snapshotId": snapshot_id}
            try:
                r = self.session.post(f"{self.base_url}/api/nqe", json=body, params=params)
                r.raise_for_status()
                payload = r.json()
                batch = payload.get("items", [])
                results.extend(batch)
                if offset + len(batch) >= payload.get("totalNumItems", len(batch)):
                    break
                offset += len(batch)
            except requests.RequestException as e:
                logger.error(f"NQE query {query_id} failed: {e}")
                break
        return results

    def run_ingestion(self, sync, job=None):
        sync.logger.log_info("🧪 Minimal ingestion start", obj=sync)

        sid = sync.snapshot_data.snapshot_id
        if sid == "$latestProcessed":
            sid = self.resolve_snapshot_id(sync.snapshot_data.source.network_id, sid)
            # Re-fetch the full snapshot object and assign it to sync.snapshot_data
            snapshots = self.get_snapshots(sync.snapshot_data.source.network_id)
            resolved_snapshot = snapshots.get(sid)
            if not resolved_snapshot:
                raise ValueError(f"Resolved snapshot ID {sid} not found in snapshot list.")
            sync.snapshot_data.snapshot_id = sid
            sync.snapshot_data.data = resolved_snapshot
        epoch = sync.snapshot_data.data.get("metrics", {}).get("creationDateMillis")
        if not epoch:
            raise ValueError("Snapshot creationDateMillis missing")
        ts = datetime.utcfromtimestamp(epoch / 1000.0).strftime("%Y%m%d-%H%M")
        branch_name = f"{sync.snapshot_data.source.name.lower()}-{ts}-{sid}"

        try:
            branch = Branch.objects.get(name=branch_name)
            created = False
            sync.logger.log_info(f"📦 Reusing existing branch {branch.name}", obj=sync)
        except Branch.DoesNotExist:
            branch = Branch(name=branch_name)
            branch.save(provision=False)
            created = True
            sync.logger.log_info(f"🪵 Created branch {branch.name} → {branch.schema_name}", obj=sync)

        if created:
            user = getattr(sync, "user", None) or getattr(job, "user", None)
            if not user:
                user = get_user_model().objects.filter(is_superuser=True).first()
            try:
                branch.provision(user=user)
            except ProgrammingError as e:
                if "already exists" in str(e):
                    sync.logger.log_info("⚠️ Schema already exists, skipping", obj=sync)
                else:
                    raise
            branch.status = BranchStatusChoices.READY
            branch.save(provision=False, update_fields=["status"])
            sync.logger.log_success("✅ Branch provisioned & marked READY", obj=sync)

        if branch.schema_id not in connections.databases:
            cfg = connections.databases[DEFAULT_DB_ALIAS].copy()
            opts = cfg.get("OPTIONS", {}).copy()
            opts["options"] = f"-c search_path={branch.schema_name},public"
            cfg["OPTIONS"] = opts
            connections.databases[branch.schema_id] = cfg
            sync.logger.log_info(f"🔗 Registered DB alias: {branch.schema_id}", obj=sync)

        sync.logger.log_info(f"🧩 Sync parameters: {sync.parameters}", obj=sync)
        selected_keys = [k for k, v in (sync.parameters or {}).items() if v is True]

        app_model_pairs = []
        for key in selected_keys:
            parts = key.split(".", 1)
            if len(parts) == 2:
                app_model_pairs.append((parts[0], parts[1]))
            else:
                sync.logger.log_warning(f"⚠️ Invalid model key '{key}', skipping", obj=sync)

        app_labels = [app for app, _ in app_model_pairs]
        model_names = [model for _, model in app_model_pairs]

        selected_models = ContentType.objects.filter(app_label__in=app_labels, model__in=model_names)
        ordered_models = sort_models_by_dependencies(selected_models)
        sync.logger.log_info(
            f"🔎 {len(ordered_models)} model(s) selected for sync: {[m.model for m in ordered_models]}",
            obj=sync
        )

        nqe_matches = ForwardNQEMap.objects.filter(netbox_model__in=ordered_models)
        sync.logger.log_info(
            f"🔎 {nqe_matches.count()} NQE query mapping(s) matched selected models", obj=sync
        )

        user = getattr(sync, "user", None) or getattr(job, "user", None)

        with activate_branch(branch):
            for nqe_map in nqe_matches:
                model_class = nqe_map.netbox_model.model_class()
                model_name = model_class.__name__

                try:
                    results = self.run_nqe_query(
                        network_id=sync.snapshot_data.source.network_id,
                        query_id=nqe_map.query_id,
                        snapshot_id=sid
                    )
                    sync.logger.log_info(f"📥 Fetched {len(results)} items for {model_name} via NQE {nqe_map.query_id}", obj=sync)

                    for item in results:
                        try:
                            instance = model_class()
                            instance.save(using=branch.schema_id)
                            update_object(instance, item, using=branch.schema_id)

                            oc = instance.to_objectchange(action="create")
                            oc.user = user
                            oc.request_id = str(uuid.uuid4())
                            oc.save(using=branch.schema_id)
                            record_applied_change(oc, branch)

                        except Exception as e:
                            sync.logger.log_failure(
                                f"⚠️ Failed to update {model_name} with item {item}: {e}", obj=sync
                            )

                except Exception as e:
                    sync.logger.log_failure(f"❌ Failed NQE query {nqe_map.query_id} → {model_name}: {e}", obj=sync)

        sync.last_synced = local_now()
        sync.status = DataSourceStatusChoices.COMPLETED
        sync.save()
        sync.logger.log_success("🎉 Ingestion complete with NQE data", obj=sync)

        return {}
