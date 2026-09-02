# Slice six of the adapter-only drift comparison: netbox-dlm.
#
# Five of its seven sub-models are wired. On the reporting deployment
# `netbox_dlm.vulnerability` is the second-largest uncompared model at 37,795
# rows, behind only `dcim.inventoryitem`.
#
# Almost every DLM write goes through `_upsert_values_from_defaults` or
# `_coalesce_update_or_create`, both of which the preview runner overrides, so
# the firewall covers them. The exception is `cve.affected_software.add()` in
# the vulnerability path - an M2M write reached directly, invisible to the
# firewall, the same shape as `device.tags.add` in the tagged-item path.
#
# `inventoryitemsoftware` and `inventoryitemroleplatform` are deliberately NOT
# wired: their dependency chains have not been audited for the
# writes-behind-a-runner-call trap, and absence from `_ADAPTER_COMPARISONS` is
# the documented "no comparison" answer.
import unittest

from django.test import TestCase

from forward_netbox.utilities.drift_comparison import compare_model_rows

try:  # pragma: no cover - exercised by the skip
    from netbox_dlm.models import CVE
    from netbox_dlm.models import DeviceSoftware
    from netbox_dlm.models import SoftwareVersion
    from netbox_dlm.models import Vulnerability

    DLM_INSTALLED = True
except Exception:  # noqa: BLE001 - optional plugin
    DLM_INSTALLED = False


@unittest.skipUnless(DLM_INSTALLED, "netbox-dlm is not installed")
class DlmPreviewTest(TestCase):
    @classmethod
    def setUpTestData(cls):
        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Platform
        from dcim.models import Site

        site = Site.objects.create(name="DLM Drift", slug="dlm-drift")
        mfr = Manufacturer.objects.create(name="DLM Drift Mfr", slug="dlm-drift-mfr")
        dtype = DeviceType.objects.create(
            manufacturer=mfr, model="DLM Drift DT", slug="dlm-drift-dt"
        )
        role = DeviceRole.objects.create(
            name="DLM Drift Role", slug="dlm-drift-role", color="9e9e9e"
        )
        cls.platform = Platform.objects.create(
            name="DLM Drift Platform", slug="dlm-drift-platform"
        )
        cls.device = Device.objects.create(
            name="dlm-drift-dev",
            site=site,
            device_type=dtype,
            role=role,
            platform=cls.platform,
            status="active",
        )

    def _row(self, **extra):
        row = {
            "device": "dlm-drift-dev",
            "name": "dlm-drift-dev",
            "platform": "DLM Drift Platform",
            "platform_slug": "dlm-drift-platform",
            "version": "17.9.1",
            "cve_id": "CVE-2026-0001",
            "severity": "High",
            "description": "",
        }
        row.update(extra)
        return row

    def _software_version(self):
        return SoftwareVersion.objects.create(platform=self.platform, version="17.9.1")

    def _cve(self):
        return CVE.objects.create(cve_id="CVE-2026-0001", severity="High")

    def _cve_row(self, **extra):
        """A row for the CVE path specifically.

        `_row()` carries `name` for the device-software path, and the CVE apply
        reads that same key as the CVE's own name - so a shared row makes a
        matching CVE look drifted. Two meanings, one key; the fixture has to
        pick one.
        """
        row = self._row(name="")
        row.update(extra)
        return row

    # --- the negative space -------------------------------------------------

    def test_a_vulnerability_preview_writes_nothing(self):
        versions = SoftwareVersion.objects.count()
        cves = CVE.objects.count()
        findings = Vulnerability.objects.count()
        device_software = DeviceSoftware.objects.count()

        result = compare_model_rows(None, "netbox_dlm.vulnerability", [self._row()])

        self.assertEqual(SoftwareVersion.objects.count(), versions)
        self.assertEqual(CVE.objects.count(), cves)
        self.assertEqual(Vulnerability.objects.count(), findings)
        self.assertEqual(DeviceSoftware.objects.count(), device_software)
        self.assertEqual(result["creates"], 1)

    def test_a_vulnerability_preview_adds_no_affected_software_link(self):
        """The M2M the firewall cannot see.

        `cve.affected_software.add()` is reached directly rather than through a
        `runner.` call, so only the explicit preview flag stops it.
        """
        version = self._software_version()
        cve = self._cve()
        self.assertEqual(cve.affected_software.count(), 0)

        compare_model_rows(None, "netbox_dlm.vulnerability", [self._row()])

        cve.refresh_from_db()
        self.assertEqual(cve.affected_software.count(), 0)
        self.assertNotIn(version, cve.affected_software.all())

    def test_a_cve_preview_creates_no_cve(self):
        before = CVE.objects.count()

        result = compare_model_rows(None, "netbox_dlm.cve", [self._cve_row()])

        self.assertEqual(CVE.objects.count(), before)
        self.assertEqual(result["creates"], 1)

    # --- classification -----------------------------------------------------

    def test_an_absent_cve_is_a_create(self):
        result = compare_model_rows(None, "netbox_dlm.cve", [self._cve_row()])

        self.assertEqual(
            result, {"creates": 1, "updates": 0, "unchanged": 0, "rejected": 0}
        )

    def test_a_matching_cve_is_unchanged(self):
        CVE.objects.create(
            cve_id="CVE-2026-0001", name="", description="", severity="High"
        )

        result = compare_model_rows(None, "netbox_dlm.cve", [self._cve_row()])

        self.assertEqual(result["unchanged"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_drifted_cve_is_an_update(self):
        CVE.objects.create(
            cve_id="CVE-2026-0001", name="", description="", severity="Low"
        )

        result = compare_model_rows(None, "netbox_dlm.cve", [self._cve_row()])

        self.assertEqual(result["updates"], 1)
        self.assertEqual(result["creates"], 0)

    def test_a_vulnerability_whose_cve_is_absent_is_a_create(self):
        # No CVE and no SoftwareVersion in NetBox: the finding cannot already
        # exist, so it is a create rather than an unresolvable row.
        result = compare_model_rows(None, "netbox_dlm.vulnerability", [self._row()])

        self.assertEqual(result["creates"], 1)
        self.assertEqual(result["rejected"], 0)

    def test_a_softwareversion_with_no_device_basis_is_declined(self):
        """`create=False` means the apply never creates this row.

        The catalogue map only enriches versions that already have a
        device-scoped basis, so an absent one is a row the apply declines - not
        drift it will resolve on the next run.
        """
        result = compare_model_rows(None, "netbox_dlm.softwareversion", [self._row()])

        self.assertEqual(result["creates"], 0)
        self.assertEqual(result["rejected"], 1)

    def test_a_softwareversion_that_exists_is_measured(self):
        self._software_version()

        result = compare_model_rows(None, "netbox_dlm.softwareversion", [self._row()])

        self.assertEqual(result["rejected"], 0)
        self.assertEqual(result["creates"], 0)

    # --- the two that decline ------------------------------------------------

    def test_the_once_unaudited_dlm_models_are_now_measured(self):
        # Their chains were audited after this slice and wired up; see
        # `test_dlm_inventoryitem_drift_comparison`. Pinned here so the
        # stand-in cannot rot the way `softwareversion` did for slice six.
        for model_string in (
            "netbox_dlm.inventoryitemsoftware",
            "netbox_dlm.inventoryitemroleplatform",
        ):
            self.assertIsNotNone(
                compare_model_rows(None, model_string, []),
                model_string,
            )
