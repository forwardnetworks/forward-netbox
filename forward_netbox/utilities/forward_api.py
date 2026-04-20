import json

import httpx

from ..exceptions import ForwardClientError
from ..exceptions import ForwardConnectivityError

LATEST_PROCESSED_SNAPSHOT = "latestProcessed"
DEFAULT_FORWARD_API_TIMEOUT_SECONDS = 1200


class ForwardClient:
    def __init__(self, source):
        self.source = source
        params = source.parameters or {}
        self.timeout = params.get("timeout") or DEFAULT_FORWARD_API_TIMEOUT_SECONDS
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
            "User-Agent": "forward-netbox/0.1.6.0",
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
        except httpx.TimeoutException as exc:
            raise ForwardConnectivityError(
                "Forward API request timed out while connecting to Forward."
            ) from exc
        except httpx.RequestError as exc:
            raise ForwardConnectivityError(
                f"Could not connect to Forward API endpoint: {exc}"
            ) from exc
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

    def _parse_nqe_records(self, data):
        items = data.get("items") or []
        records = []
        for item in items:
            if isinstance(item, dict) and isinstance(item.get("fields"), dict):
                records.append(item["fields"])
            elif isinstance(item, dict):
                records.append(item)
            else:
                records.append(json.loads(json.dumps(item)))
        return records, data.get("totalNumItems")

    def _parse_nqe_diff_rows(self, data):
        rows = data.get("rows") or []
        parsed_rows = []
        for row in rows:
            if not isinstance(row, dict):
                parsed_rows.append(json.loads(json.dumps(row)))
                continue
            parsed_rows.append(
                {
                    "type": row.get("type"),
                    "before": row.get("before"),
                    "after": row.get("after"),
                }
            )
        return parsed_rows, data.get("totalNumRows")

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
        fetch_all=False,
    ):
        if bool(query) == bool(query_id):
            raise ForwardClientError(
                "Exactly one of `query` or `query_id` must be supplied."
            )
        if limit < 1:
            raise ForwardClientError("`limit` must be at least 1.")

        def fetch_page(page_offset):
            payload = {
                "parameters": parameters or {},
                "queryOptions": {
                    "limit": limit,
                    "offset": page_offset,
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

            response = self._request(
                "POST",
                "/nqe",
                params=req_params,
                json_body=payload,
            )

            return self._parse_nqe_records(response.json() or {})

        records, total_num_items = fetch_page(offset)
        if not fetch_all:
            return records

        all_records = list(records)
        expected_total = int(total_num_items) if total_num_items is not None else None
        last_page_size = len(records)

        while True:
            if expected_total is not None and len(all_records) >= expected_total:
                return all_records
            if expected_total is None and last_page_size < limit:
                return all_records

            next_offset = offset + len(all_records)
            page_records, page_total = fetch_page(next_offset)
            if expected_total is None and page_total is not None:
                expected_total = int(page_total)
            last_page_size = len(page_records)
            if not page_records:
                if expected_total is not None and len(all_records) < expected_total:
                    raise ForwardClientError(
                        "Forward NQE pagination ended early: "
                        f"fetched {len(all_records)} rows but API reported {expected_total}."
                    )
                return all_records
            all_records.extend(page_records)

    def run_nqe_diff(
        self,
        *,
        query_id,
        before_snapshot_id,
        after_snapshot_id,
        commit_id=None,
        limit=10000,
        offset=0,
        item_format="JSON",
        column_filters=None,
        fetch_all=False,
    ):
        if not query_id:
            raise ForwardClientError("`query_id` must be supplied.")
        if not before_snapshot_id or not after_snapshot_id:
            raise ForwardClientError(
                "Both `before_snapshot_id` and `after_snapshot_id` must be supplied."
            )
        if limit < 1:
            raise ForwardClientError("`limit` must be at least 1.")

        def fetch_page(page_offset):
            payload = {
                "queryId": query_id,
                "options": {
                    "limit": limit,
                    "offset": page_offset,
                    "itemFormat": item_format,
                },
            }
            if commit_id:
                payload["commitId"] = commit_id
            if column_filters:
                payload["options"]["columnFilters"] = column_filters

            response = self._request(
                "POST",
                f"/nqe-diffs/{before_snapshot_id}/{after_snapshot_id}",
                json_body=payload,
            )
            return self._parse_nqe_diff_rows(response.json() or {})

        rows, total_num_rows = fetch_page(offset)
        if not fetch_all:
            return rows

        all_rows = list(rows)
        expected_total = int(total_num_rows) if total_num_rows is not None else None
        last_page_size = len(rows)

        while True:
            if expected_total is not None and len(all_rows) >= expected_total:
                return all_rows
            if expected_total is None and last_page_size < limit:
                return all_rows

            next_offset = offset + len(all_rows)
            page_rows, page_total = fetch_page(next_offset)
            if expected_total is None and page_total is not None:
                expected_total = int(page_total)
            last_page_size = len(page_rows)
            if not page_rows:
                if expected_total is not None and len(all_rows) < expected_total:
                    raise ForwardClientError(
                        "Forward NQE diff pagination ended early: "
                        f"fetched {len(all_rows)} rows but API reported {expected_total}."
                    )
                return all_rows
            all_rows.extend(page_rows)
