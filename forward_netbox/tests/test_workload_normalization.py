from django.test import SimpleTestCase

from forward_netbox.utilities.branch_budget import BranchWorkload
from forward_netbox.utilities.workload_normalization import (
    normalize_dependency_workloads,
)


def _workload(model_string, rows, *, sync_mode="full"):
    return BranchWorkload(
        model_string=model_string,
        label=model_string,
        upsert_rows=rows,
        sync_mode=sync_mode,
        query_name=model_string,
        execution_value=f"Q_{model_string}",
    )


class DependencyWorkloadNormalizationTest(SimpleTestCase):
    def test_device_serial_reuses_one_distinct_full_inventory_chassis_serial(self):
        workloads = [
            _workload(
                "dcim.device",
                [
                    {"name": "device-a"},
                    {"name": "device-b"},
                    {"name": "endpoint-a"},
                ],
            ),
            _workload(
                "dcim.inventoryitem",
                [
                    {
                        "device": "device-a",
                        "part_type": "CHASSIS",
                        "serial": "SERIAL-A",
                    },
                    {
                        "device": "device-a",
                        "part_type": "CHASSIS",
                        "serial": "SERIAL-A",
                    },
                    {
                        "device": "device-b",
                        "part_type": "CHASSIS",
                        "serial": "SERIAL-B1",
                    },
                    {
                        "device": "device-b",
                        "part_type": "CHASSIS",
                        "serial": "SERIAL-B2",
                    },
                ],
            ),
        ]

        normalized, summaries = normalize_dependency_workloads(workloads)

        self.assertEqual(normalized[0].upsert_rows[0]["serial"], "SERIAL-A")
        self.assertNotIn("serial", normalized[0].upsert_rows[1])
        self.assertNotIn("serial", normalized[0].upsert_rows[2])
        self.assertEqual(
            summaries[0]["enrichment_counts"], {"single_chassis_serial": 1}
        )
        self.assertEqual(summaries[0]["excluded_row_count"], 0)

    def test_partial_inventory_workload_does_not_claim_device_serial(self):
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("dcim.device", [{"name": "device-a"}]),
                _workload(
                    "dcim.inventoryitem",
                    [
                        {
                            "device": "device-a",
                            "part_type": "CHASSIS",
                            "serial": "SERIAL-A",
                        }
                    ],
                    sync_mode="diff",
                ),
            ]
        )

        self.assertNotIn("serial", normalized[0].upsert_rows[0])
        self.assertEqual(summaries, [])

    def test_cables_require_both_full_workload_devices_and_interfaces(self):
        workloads = [
            _workload(
                "dcim.device",
                [{"name": "device-a"}, {"name": "device-b"}],
            ),
            _workload(
                "dcim.interface",
                [
                    {"device": "device-a", "name": "Ethernet1"},
                    {"device": "device-b", "name": "Ethernet2"},
                ],
            ),
            _workload(
                "dcim.cable",
                [
                    {
                        "device": "device-a",
                        "interface": "Ethernet1",
                        "remote_device": "device-b",
                        "remote_interface": "Ethernet2",
                    },
                    {
                        "device": "device-a",
                        "interface": "Ethernet9",
                        "remote_device": "device-b",
                        "remote_interface": "Ethernet2",
                    },
                    {
                        "device": "device-a",
                        "interface": "Ethernet1",
                        "remote_device": "device-out-of-scope",
                        "remote_interface": "Ethernet3",
                    },
                ],
            ),
        ]

        normalized, summaries = normalize_dependency_workloads(workloads)

        self.assertEqual(len(normalized[2].upsert_rows), 1)
        self.assertEqual(summaries[0]["excluded_row_count"], 2)
        self.assertEqual(
            summaries[0]["reason_counts"],
            {"device_not_in_workload": 1, "interface_not_in_workload": 1},
        )

    def test_cable_selection_preserves_exact_existing_then_is_deterministic(self):
        cable_rows = [
            self._cable("device-a", "Ethernet1", "device-b", "Ethernet1"),
            self._cable("device-a", "Ethernet1", "device-c", "Ethernet1"),
            self._cable("device-d", "Ethernet1", "device-e", "Ethernet1"),
            self._cable("device-d", "Ethernet1", "device-f", "Ethernet1"),
        ]
        device_rows = [
            {"name": f"device-{letter}"} for letter in ("a", "b", "c", "d", "e", "f")
        ]
        interface_rows = [
            {"device": row["name"], "name": "Ethernet1"} for row in device_rows
        ]

        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("dcim.device", device_rows),
                _workload("dcim.interface", interface_rows),
                _workload("dcim.cable", cable_rows),
            ],
            existing_cable_ids_by_endpoint={
                ("device-a", "Ethernet1"): 7,
                ("device-b", "Ethernet1"): 7,
            },
        )

        selected = normalized[2].upsert_rows
        self.assertEqual(len(selected), 2)
        self.assertEqual(selected[0], cable_rows[0])
        self.assertEqual(selected[1], cable_rows[2])
        self.assertEqual(
            summaries[0]["reason_counts"],
            {"competing_candidate": 1, "existing_endpoint_conflict": 1},
        )

    def test_ospf_interface_accepts_supported_alias_and_rejects_missing_parent(self):
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("dcim.device", [{"name": "router-1"}]),
                _workload(
                    "dcim.interface",
                    [{"device": "router-1", "name": "GigabitEthernet0/1"}],
                ),
                _workload(
                    "netbox_routing.ospfinterface",
                    [
                        {"device": "router-1", "local_interface": "Gi0/1"},
                        {"device": "router-1", "local_interface": "Gi0/2"},
                    ],
                ),
            ]
        )

        self.assertEqual(
            normalized[2].upsert_rows,
            [{"device": "router-1", "local_interface": "Gi0/1"}],
        )
        self.assertEqual(
            summaries[0]["reason_counts"], {"interface_not_in_workload": 1}
        )

    def test_partial_parent_workloads_do_not_claim_authoritative_coverage(self):
        cable = self._cable("device-a", "Ethernet1", "device-b", "Ethernet2")

        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("dcim.device", [{"name": "device-a"}], sync_mode="diff"),
                _workload(
                    "dcim.interface",
                    [{"device": "device-a", "name": "Ethernet1"}],
                    sync_mode="diff",
                ),
                _workload("dcim.cable", [cable]),
            ]
        )

        self.assertEqual(normalized[2].upsert_rows, [cable])
        self.assertEqual(summaries, [])

    def test_cves_require_authoritative_vulnerability_coverage(self):
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload(
                    "netbox_dlm.cve",
                    [
                        {"cve_id": "CVE-2026-0001", "cvss_v3_score": 9.1},
                        {"cve_id": "CVE-2026-0002", "cvss_v3_score": 7.5},
                    ],
                ),
                _workload(
                    "netbox_dlm.vulnerability",
                    [{"name": "device-a", "cve_id": "CVE-2026-0002"}],
                ),
            ]
        )

        self.assertEqual(
            normalized[0].upsert_rows,
            [{"cve_id": "CVE-2026-0002", "cvss_v3_score": 7.5}],
        )
        self.assertEqual(
            normalized[0].delete_rows,
            [{"cve_id": "CVE-2026-0001", "cvss_v3_score": 9.1}],
        )
        self.assertEqual(
            summaries[0]["reason_counts"], {"no_in_scope_vulnerability": 1}
        )

    def test_empty_full_vulnerability_workload_removes_all_catalog_cves(self):
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload(
                    "netbox_dlm.cve",
                    [{"cve_id": "CVE-2026-0001", "cvss_v3_score": 9.1}],
                ),
                _workload("netbox_dlm.vulnerability", []),
            ]
        )

        self.assertEqual(normalized[0].upsert_rows, [])
        self.assertEqual(
            normalized[0].delete_rows,
            [{"cve_id": "CVE-2026-0001", "cvss_v3_score": 9.1}],
        )
        self.assertEqual(summaries[0]["excluded_row_count"], 1)

    def test_partial_vulnerability_workload_does_not_prune_catalog_cves(self):
        cve_rows = [{"cve_id": "CVE-2026-0001", "cvss_v3_score": 9.1}]
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("netbox_dlm.cve", cve_rows),
                _workload(
                    "netbox_dlm.vulnerability",
                    [],
                    sync_mode="diff",
                ),
            ]
        )

        self.assertEqual(normalized[0].upsert_rows, cve_rows)
        self.assertEqual(summaries, [])

    def test_software_versions_require_authoritative_association_coverage(self):
        normalized, summaries = normalize_dependency_workloads(
            [
                _workload(
                    "netbox_dlm.softwareversion",
                    [
                        {"platform_slug": "ios-xe", "version": "17.12.7"},
                        {"platform_slug": "eos", "version": "4.31.1"},
                        {"platform_slug": "junos", "version": "23.4R1"},
                    ],
                ),
                _workload(
                    "netbox_dlm.devicesoftware",
                    [
                        {
                            "name": "router-a",
                            "platform_slug": "ios-xe",
                            "version": "17.12.7",
                        }
                    ],
                ),
                _workload(
                    "netbox_dlm.vulnerability",
                    [
                        {
                            "name": "switch-a",
                            "platform_slug": "eos",
                            "version": "4.31.1",
                            "cve_id": "CVE-2026-0001",
                        }
                    ],
                ),
            ]
        )

        self.assertEqual(
            normalized[0].upsert_rows,
            [
                {"platform_slug": "ios-xe", "version": "17.12.7"},
                {"platform_slug": "eos", "version": "4.31.1"},
            ],
        )
        self.assertEqual(
            normalized[0].delete_rows,
            [{"platform_slug": "junos", "version": "23.4R1"}],
        )
        self.assertEqual(
            summaries[0]["reason_counts"],
            {"no_in_scope_device_or_vulnerability": 1},
        )

    def test_empty_full_device_software_removes_all_catalog_versions(self):
        version = {"platform_slug": "ios-xe", "version": "17.12.7"}

        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("netbox_dlm.softwareversion", [version]),
                _workload("netbox_dlm.devicesoftware", []),
            ]
        )

        self.assertEqual(normalized[0].upsert_rows, [])
        self.assertEqual(normalized[0].delete_rows, [version])
        self.assertEqual(summaries[0]["excluded_row_count"], 1)

    def test_partial_software_associations_do_not_prune_catalog_versions(self):
        version = {"platform_slug": "ios-xe", "version": "17.12.7"}

        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("netbox_dlm.softwareversion", [version]),
                _workload("netbox_dlm.devicesoftware", [], sync_mode="diff"),
            ]
        )

        self.assertEqual(normalized[0].upsert_rows, [version])
        self.assertEqual(summaries, [])

    def test_associated_version_missing_from_catalog_query_is_enriched(self):
        associated = {
            "name": "router-a",
            "platform": "IOS_XE",
            "platform_slug": "ios-xe",
            "version": "17.12.7",
            "end_of_support": "2028-03-31",
        }

        normalized, summaries = normalize_dependency_workloads(
            [
                _workload("netbox_dlm.softwareversion", []),
                _workload("netbox_dlm.devicesoftware", [associated]),
            ]
        )

        self.assertEqual(
            normalized[0].upsert_rows,
            [
                {
                    "platform": "IOS_XE",
                    "platform_slug": "ios-xe",
                    "version": "17.12.7",
                    "end_of_support": "2028-03-31",
                }
            ],
        )
        self.assertEqual(
            summaries[0]["enrichment_counts"],
            {"associated_software_version": 1},
        )

    @staticmethod
    def _cable(device, interface, remote_device, remote_interface):
        return {
            "device": device,
            "interface": interface,
            "remote_device": remote_device,
            "remote_interface": remote_interface,
            "status": "connected",
        }
