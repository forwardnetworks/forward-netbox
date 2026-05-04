import csv
import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
from unittest.mock import patch

from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase

from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.module_readiness import derive_module_bay_position
from forward_netbox.utilities.module_readiness import summarize_module_readiness


class ModuleReadinessUtilityTest(TestCase):
    def test_summarize_module_readiness_builds_unique_import_rows(self):
        report = summarize_module_readiness(
            [
                {
                    "device": "device-a",
                    "module_bay": "Slot 1",
                },
                {
                    "device": "device-a",
                    "module_bay": "Slot 2",
                },
                {
                    "device": "device-a",
                    "module_bay": "Slot 2",
                },
                {
                    "device": "device-b",
                    "module_bay": "Slot 1",
                },
            ],
            existing_devices={"device-a"},
            existing_module_bays={("device-a", "Slot 1")},
        )

        self.assertFalse(report.ready)
        self.assertEqual(report.candidate_rows, 4)
        self.assertEqual(report.existing_bay_rows, 1)
        self.assertEqual(report.missing_bay_rows, 2)
        self.assertEqual(report.missing_device_rows, 1)
        self.assertEqual(report.unique_missing_bays, 1)
        self.assertEqual(
            report.module_bay_import_rows,
            (
                {
                    "device": "device-a",
                    "name": "Slot 2",
                    "label": "Slot 2",
                    "position": "2",
                    "description": "Required for optional Forward module import.",
                },
            ),
        )
        self.assertEqual(report.missing_device_names, ("device-b",))

    def test_derive_module_bay_position_uses_trailing_number_only(self):
        self.assertEqual(derive_module_bay_position("Slot 27"), "27")
        self.assertEqual(derive_module_bay_position("Supervisor A"), "")


class ForwardModuleReadinessCommandTest(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin",
            password="password",
        )
        self.source = ForwardSource.objects.create(
            name="module-source",
            type=ForwardSourceDeploymentChoices.CUSTOM,
            url="https://forward.example.com",
            parameters={
                "username": "operator@example.com",
                "password": "secret",
                "verify": True,
                "network_id": "network-test",
            },
        )
        self.sync = ForwardSync.objects.create(
            name="module-sync",
            source=self.source,
            user=self.user,
            parameters={
                "snapshot_id": "snapshot-test",
                "dcim.module": True,
            },
        )
        self._create_device("device-a")

    def _create_device(self, name):
        site, _ = Site.objects.get_or_create(name="site-1", slug="site-1")
        manufacturer, _ = Manufacturer.objects.get_or_create(
            name="vendor-1", slug="vendor-1"
        )
        role, _ = DeviceRole.objects.get_or_create(
            name="role-1", slug="role-1", defaults={"color": "9e9e9e"}
        )
        device_type, _ = DeviceType.objects.get_or_create(
            manufacturer=manufacturer,
            model="model-1",
            slug="model-1",
        )
        return Device.objects.create(
            name=name,
            site=site,
            role=role,
            device_type=device_type,
            status="active",
        )

    def test_command_writes_netbox_module_bay_import_csv(self):
        client = Mock()
        client.run_nqe_query.return_value = [
            {
                "device": "device-a",
                "module_bay": "Slot 1",
                "manufacturer": "Vendor 1",
                "manufacturer_slug": "vendor-1",
                "model": "Line Card",
                "part_number": "LC-1",
                "status": "active",
            }
        ]

        with TemporaryDirectory() as temp_dir, patch.object(
            ForwardSource,
            "get_client",
            return_value=client,
        ):
            stdout = StringIO()
            call_command(
                "forward_module_readiness",
                sync_name="module-sync",
                output_dir=temp_dir,
                stdout=stdout,
            )

            output_dirs = list(Path(temp_dir).iterdir())
            self.assertEqual(len(output_dirs), 1)
            summary = json.loads((output_dirs[0] / "summary.json").read_text())
            with (output_dirs[0] / "netbox-module-bays.csv").open(
                encoding="utf-8"
            ) as input_file:
                csv_rows = list(csv.DictReader(input_file))

        self.assertEqual(summary["candidate_rows"], 1)
        self.assertEqual(summary["missing_bay_rows"], 1)
        self.assertEqual(summary["unique_missing_bays"], 1)
        self.assertEqual(
            csv_rows,
            [
                {
                    "device": "device-a",
                    "name": "Slot 1",
                    "label": "Slot 1",
                    "position": "1",
                    "description": "Required for optional Forward module import.",
                }
            ],
        )
        client.run_nqe_query.assert_called_once()
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["network_id"], "network-test"
        )
        self.assertEqual(
            client.run_nqe_query.call_args.kwargs["snapshot_id"],
            "snapshot-test",
        )
