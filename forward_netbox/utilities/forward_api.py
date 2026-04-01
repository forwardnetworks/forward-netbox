import json

import httpx

from ..exceptions import ForwardClientError

LATEST_PROCESSED_SNAPSHOT = "latestProcessed"


class ForwardClient:
    def __init__(self, source):
        self.source = source
        params = source.parameters or {}
        self.timeout = params.get("timeout") or 60
        self.verify = params.get("verify", True)
        self.base_url = source.url.rstrip("/")
        self.username = params.get("username")
        self.password = params.get("password")

    def _api_url(self, path):
        base_url = self.base_url
        if base_url.endswith("/api"):
            return f"{base_url}{path}"
        return f"{base_url}/api{path}"

    def _headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "forward-netbox/0.1.0",
        }

    def _auth(self):
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def _request(self, method, path, *, params=None, json_body=None):
        try:
            response = httpx.request(
                method,
                self._api_url(path),
                params=params,
                json=json_body,
                headers=self._headers(),
                auth=self._auth(),
                timeout=self.timeout,
                verify=self.verify,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ForwardClientError(
                f"Forward API request failed with HTTP {exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.HTTPError as exc:
            raise ForwardClientError(f"Forward API request failed: {exc}") from exc
        return response

    def get_networks(self):
        response = self._request("GET", "/networks")
        data = response.json()
        networks = []
        for item in data or []:
            network_id = str(item.get("id", "")).strip()
            name = str(item.get("name", "")).strip()
            if not network_id or not name:
                continue
            networks.append(
                {
                    "id": network_id,
                    "name": name,
                    "label": f"{name} ({network_id})",
                }
            )
        return networks

    def get_snapshots(self, network_id, *, include_archived=False, limit=100):
        response = self._request(
            "GET",
            f"/networks/{network_id}/snapshots",
            params={
                "includeArchived": str(bool(include_archived)).lower(),
                "limit": limit,
            },
        )
        data = response.json() or {}
        snapshots = []
        for item in data.get("snapshots") or []:
            snapshot_id = str(item.get("id", "")).strip()
            if not snapshot_id:
                continue
            state = str(item.get("state", "")).strip()
            created = str(item.get("createdAt", "")).strip()
            processed = str(item.get("processedAt", "")).strip()
            label_parts = [snapshot_id]
            if state:
                label_parts.append(state)
            if processed:
                label_parts.append(processed)
            elif created:
                label_parts.append(created)
            snapshots.append(
                {
                    "id": snapshot_id,
                    "state": state,
                    "created_at": created,
                    "processed_at": processed,
                    "label": " | ".join(label_parts),
                }
            )
        return snapshots

    def get_latest_processed_snapshot(self, network_id):
        response = self._request(
            "GET", f"/networks/{network_id}/snapshots/latestProcessed"
        )
        return response.json() or {}

    def get_latest_processed_snapshot_id(self, network_id):
        snapshot = self.get_latest_processed_snapshot(network_id)
        snapshot_id = str(snapshot.get("id", "")).strip()
        if not snapshot_id:
            raise ForwardClientError(
                "Forward latestProcessed snapshot response did not include an ID."
            )
        return snapshot_id

    def get_snapshot_metrics(self, snapshot_id):
        response = self._request("GET", f"/snapshots/{snapshot_id}/metrics")
        return response.json() or {}

    def run_nqe_query(
        self,
        *,
        query=None,
        query_id=None,
        commit_id=None,
        network_id=None,
        snapshot_id=None,
        parameters=None,
        limit=10000,
        offset=0,
        item_format="JSON",
        column_filters=None,
    ):
        if bool(query) == bool(query_id):
            raise ForwardClientError(
                "Exactly one of `query` or `query_id` must be supplied."
            )

        payload = {
            "parameters": parameters or {},
            "queryOptions": {
                "limit": limit,
                "offset": offset,
                "itemFormat": item_format,
            },
        }
        if column_filters:
            payload["queryOptions"]["columnFilters"] = column_filters
        if query_id:
            payload["queryId"] = query_id
            if commit_id:
                payload["commitId"] = commit_id
        else:
            payload["query"] = query
        req_params = {}
        if network_id:
            req_params["networkId"] = network_id
        if snapshot_id:
            req_params["snapshotId"] = snapshot_id

        response = self._request("POST", "/nqe", params=req_params, json_body=payload)

        data = response.json()
        items = data.get("items") or []
        records = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("fields"), dict):
                records.append(item["fields"])
            elif isinstance(item, dict):
                records.append(item)
            else:
                records.append(json.loads(json.dumps(item)))
        return records
