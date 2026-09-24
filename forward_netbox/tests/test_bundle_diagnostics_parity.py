"""The support bundle answers the investigation's questions without a shell.

A customer's uncovered count jumped after an upgrade and answering why took a
hand-written script run on their NetBox, because the bundle carried no
breakdown, no timeline, no query bindings and nothing about the rows behind an
ingestion issue. These tests pin that the bundle now carries every one of those
sections, populated, and that exporting it still leaks no device or site name.
"""

import json

from core.models import ObjectChange
from dcim.models import Device
from dcim.models import DeviceRole
from dcim.models import DeviceType
from dcim.models import Manufacturer
from dcim.models import Site
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase
from extras.models import Tag
from ipam.models import VRF

from forward_netbox.models import ForwardIngestion
from forward_netbox.models import ForwardIngestionIssue
from forward_netbox.models import ForwardSource
from forward_netbox.models import ForwardSync
from forward_netbox.utilities.bundle_diagnostics import bundle_diagnostics
from forward_netbox.utilities.bundle_diagnostics import SECTIONS
from forward_netbox.utilities.export_redaction import export_safe_payload


class BundleDiagnosticsParityTest(TestCase):
    def setUp(self):
        self.site = Site.objects.create(name="Secret Street Site", slug="secret-site")
        mfr = Manufacturer.objects.create(name="Avocent", slug="avocent")
        dt = DeviceType.objects.create(manufacturer=mfr, model="ACS8", slug="acs8")
        role = DeviceRole.objects.create(name="console", slug="console")
        tag = Tag.objects.create(name="Forward Uncovered", slug="forward-uncovered")
        self.device = Device.objects.create(
            name="customer-console-01",
            site=self.site,
            device_type=dt,
            role=role,
            status="active",
        )
        self.device.tags.add(tag)
        ObjectChange.objects.create(
            request_id=__import__("uuid").uuid4(),
            user=__import__(
                "users.models", fromlist=["User"]
            ).User.objects.get_or_create(username="diag-test")[0],
            changed_object_type=ContentType.objects.get_for_model(Device),
            changed_object_id=self.device.pk,
            object_repr=self.device.name,
            action="update",
            prechange_data={"name": "Customer-Console-01", "tags": []},
            postchange_data={
                "name": "customer-console-01",
                "tags": ["Forward Uncovered"],
            },
        )
        # The site-relabeling duplicate pattern: same name, two sites, one
        # copy still carrying a tag the other lost.
        other_site = Site.objects.create(
            name="Renamed Secret Site", slug="renamed-site"
        )
        self.duplicate = Device.objects.create(
            name="Customer-Console-01",
            site=other_site,
            device_type=dt,
            role=role,
            status="active",
        )

        self.vrf = VRF.objects.create(name="customer-vrf")
        source = ForwardSource.objects.create(
            name="src", url="https://fwd.example.invalid"
        )
        self.sync = ForwardSync.objects.create(name="sync", source=source)
        ingestion = ForwardIngestion.objects.create(sync=self.sync)
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            model="ipam.ipaddress",
            exception="ForwardSyncDataError",
            message=f"IP address #9 was not moved to device #{self.device.pk}",
            raw_data={},
        )
        ForwardIngestionIssue.objects.create(
            ingestion=ingestion,
            model="ipam.vrf",
            exception="ForwardDependencySkipError",
            message=f"Affected NetBox row: pk {self.vrf.pk}.",
            raw_data={"netbox_pk": self.vrf.pk},
        )

    def test_every_section_is_present_and_computed(self):
        out = bundle_diagnostics(self.sync)

        self.assertEqual(out["errors"], {})
        for name in SECTIONS:
            self.assertIsNotNone(out[name], name)

    def test_the_sections_answer_the_investigation(self):
        out = bundle_diagnostics(self.sync)

        self.assertEqual(out["uncovered"]["count"], 1)
        self.assertEqual(
            out["uncovered"]["by_manufacturer"], [{"value": "Avocent", "count": 1}]
        )
        # Change history stores tag NAMES; a slug match here read zero.
        self.assertEqual(
            sum(n for _, n in out["uncovered_tag_timeline"]["gained_by_hour"]), 1
        )
        self.assertEqual(out["device_renames"]["case_only"], 1)
        self.assertEqual(out["console_servers"]["count"], 2)
        self.assertEqual(out["issue_references"]["devices"][0]["pk"], self.device.pk)
        self.assertEqual(out["issue_references"]["vrfs"][0]["pk"], self.vrf.pk)
        self.assertGreater(len(out["nqe_map_bindings"]), 0)

    def test_duplicate_device_names_finds_the_site_relabel_pattern(self):
        out = bundle_diagnostics(self.sync)

        dupes = out["duplicate_device_names"]
        self.assertEqual(dupes["distinct_duplicated_names"], 1)
        group = dupes["groups"][0]
        self.assertEqual(group["count"], 2)
        self.assertEqual(group["distinct_sites"], 2)
        self.assertEqual(group["same_site_repeats"], {})
        # One copy is tagged forward-uncovered, the other is not.
        self.assertTrue(group["tag_sets_differ"])
        self.assertEqual(
            sorted(d["pk"] for d in group["devices"]),
            sorted([self.device.pk, self.duplicate.pk]),
        )

    def test_exported_diagnostics_carry_no_names(self):
        text = json.dumps(
            export_safe_payload({"diagnostics": bundle_diagnostics(self.sync)}),
            default=str,
        )

        self.assertNotIn("customer-console-01", text)
        self.assertNotIn("Secret Street Site", text)
        self.assertNotIn("Renamed Secret Site", text)
        self.assertNotIn("customer-vrf", text)
        self.assertIn("Avocent", text)
