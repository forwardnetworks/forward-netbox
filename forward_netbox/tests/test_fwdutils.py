from datetime import datetime, timezone

import httpx
from django.test import SimpleTestCase

from forward_netbox.utilities.fwdutils import ForwardRESTClient


class ForwardRESTClientSnapshotTest(SimpleTestCase):
    def _make_client(self, response_json, *, path_expected):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == path_expected
            return httpx.Response(200, json=response_json)

        transport = httpx.MockTransport(handler)
        session = httpx.Client(base_url="https://example.com", transport=transport)
        self.addCleanup(session.close)
        return ForwardRESTClient(
            base_url="https://example.com",
            token=None,
            verify=True,
            network_id="12345",
            session=session,
        )

    def test_list_snapshots_for_network(self):
        response = {
            "name": "Hybrid Cloud Demo",
            "snapshots": [
                {
                    "id": "753593",
                    "state": "PROCESSED",
                    "creationDateMillis": 1714153610277,
                    "processedAtMillis": 1758101256253,
                }
            ],
        }
        client = self._make_client(response, path_expected="/api/networks/12345/snapshots")

        snapshots = client.list_snapshots()

        self.assertEqual(len(snapshots), 1)
        snap = snapshots[0]
        self.assertEqual(snap["snapshot_id"], "753593")
        self.assertEqual(snap["status"], "loaded")
        self.assertEqual(snap["name"], "Hybrid Cloud Demo - 753593")
        self.assertEqual(snap["network_id"], "12345")
        self.assertEqual(
            snap["start"],
            datetime.fromtimestamp(1714153610277 / 1000, tz=timezone.utc).isoformat(),
        )
        self.assertEqual(
            snap["end"],
            datetime.fromtimestamp(1758101256253 / 1000, tz=timezone.utc).isoformat(),
        )

    def test_list_snapshots_without_network(self):
        records = [
            {"id": "legacy", "status": "loaded"},
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v1/snapshots"
            return httpx.Response(200, json=records)

        session = httpx.Client(base_url="https://example.com", transport=httpx.MockTransport(handler))
        self.addCleanup(session.close)

        client = ForwardRESTClient(
            base_url="https://example.com",
            token=None,
            verify=True,
            network_id=None,
            session=session,
        )

        snapshots = client.list_snapshots()
        self.assertEqual(snapshots, records)
