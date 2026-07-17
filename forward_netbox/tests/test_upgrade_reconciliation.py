from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Platform
from dcim.models import Site
from django.apps import apps
from django.test import TestCase

from forward_netbox.utilities.upgrade_reconciliation import (
    compute_upgrade_reconciliation,
)


class UpgradeReconciliationCoreTest(TestCase):
    def setUp(self):
        self.opengear = Manufacturer.objects.create(name="Opengear", slug="opengear")
        self.avocent = Manufacturer.objects.create(name="Avocent", slug="avocent")
        self.cisco = Manufacturer.objects.create(name="Cisco", slug="cisco")

    def test_classifies_only_unreferenced_legacy_endpoint_device_types(self):
        opengear_legacy = DeviceType.objects.create(
            manufacturer=self.opengear,
            model="Opengear ACM7008-2-M, Linux 5.17 OpenGear/ACM700x Version 5.3.1",
            slug="opengear-acm7008-legacy",
        )
        avocent_legacy = DeviceType.objects.create(
            manufacturer=self.avocent,
            model="Avocent ACS - version: 3.2.1",
            slug="avocent-acs-legacy",
        )
        DeviceType.objects.create(
            manufacturer=self.opengear,
            model="Opengear ACM7008-2-M",
            slug="opengear-acm7008",
        )
        attached_legacy = DeviceType.objects.create(
            manufacturer=self.opengear,
            model="Opengear ACM7004, Linux 5.10 OpenGear Version 5.2",
            slug="opengear-acm7004-attached",
        )
        site = Site.objects.create(name="Upgrade Site", slug="upgrade-site")
        role = DeviceRole.objects.create(name="Console Server", slug="console-server")
        Device.objects.create(
            name="attached-console",
            site=site,
            role=role,
            device_type=attached_legacy,
            status="active",
        )
        Platform.objects.create(name="IOS_XE", slug="ios-xe", manufacturer=self.cisco)
        Platform.objects.create(name="Linux", slug="linux")

        report = compute_upgrade_reconciliation(include_samples=True)

        self.assertTrue(report["read_only"])
        self.assertEqual(report["scope"], "global_netbox_catalog")
        self.assertEqual(report["platforms"]["with_manufacturer"], 1)
        self.assertEqual(report["platforms"]["without_manufacturer"], 1)
        legacy = report["legacy_endpoint_device_types"]
        self.assertEqual(legacy["candidate_count"], 2)
        self.assertEqual(
            {item["model"] for item in legacy["sample"]},
            {opengear_legacy.model, avocent_legacy.model},
        )

    def test_aggregate_report_omits_inventory_samples(self):
        DeviceType.objects.create(
            manufacturer=self.opengear,
            model="Opengear ACM7008-2-M, Linux 5.17 OpenGear Version 5.3",
            slug="opengear-legacy-no-sample",
        )

        report = compute_upgrade_reconciliation(include_samples=False)

        self.assertNotIn("sample", report["legacy_endpoint_device_types"])
        self.assertEqual(report["dlm"]["available"], apps.is_installed("netbox_dlm"))
        software = report["dlm"]["software_versions"]
        self.assertNotIn("catalog_retained_sample", software)
        self.assertNotIn("unreferenced_sample", software)
