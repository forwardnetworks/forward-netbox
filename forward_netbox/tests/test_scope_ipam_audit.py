from types import SimpleNamespace

from dcim.models import Site
from django.test import TestCase
from ipam.models import Prefix
from ipam.models import VLAN
from ipam.models import VRF

from forward_netbox.utilities.scope_ipam_audit import audit_global_ipam_scope
from forward_netbox.utilities.scope_ipam_audit import audit_model_rows
from forward_netbox.utilities.scope_ipam_audit import DELETE_ELIGIBLE_TAG_SLUG
from forward_netbox.utilities.scope_ipam_audit import tag_delete_eligible_ipam


class ScopeIpamAuditTest(TestCase):
    def test_vrf_stale_detection_by_name(self):
        VRF.objects.create(name="keep")
        VRF.objects.create(name="stale")

        result = audit_model_rows("ipam.vrf", [{"name": "keep", "rd": None}])

        self.assertEqual(result["netbox_count"], 2)
        self.assertEqual(result["stale_count"], 1)
        self.assertEqual(result["stale_sample"], ["stale"])

    def test_prefix_matches_global_and_vrf_scoped(self):
        keep_vrf = VRF.objects.create(name="keepvrf")
        VRF.objects.create(name="gonevrf")
        Prefix.objects.create(prefix="10.0.0.0/24")  # global, kept
        Prefix.objects.create(prefix="10.1.0.0/24", vrf=keep_vrf)  # vrf-scoped, kept
        Prefix.objects.create(prefix="10.9.9.0/24")  # global, stale
        Prefix.objects.create(
            prefix="10.2.0.0/24", vrf=VRF.objects.get(name="gonevrf")
        )  # vrf-scoped, stale

        rows = [
            {"prefix": "10.0.0.0/24"},
            {"prefix": "10.1.0.0/24", "vrf": "keepvrf"},
        ]
        result = audit_model_rows("ipam.prefix", rows)

        self.assertEqual(result["netbox_count"], 4)
        self.assertEqual(result["stale_count"], 2)
        self.assertCountEqual(result["stale_sample"], ["10.9.9.0/24", "10.2.0.0/24"])

    def test_prefix_cidr_normalized_both_sides(self):
        # NetBox stores canonical CIDR; a non-canonical Forward row must still match.
        Prefix.objects.create(prefix="10.5.0.0/24")
        result = audit_model_rows("ipam.prefix", [{"prefix": "10.5.0.5/24"}])
        self.assertEqual(result["stale_count"], 0)

    def test_vlan_stale_by_site_vid_and_unmatchable_skipped(self):
        site = Site.objects.create(name="DC1", slug="dc1")
        VLAN.objects.create(vid=100, name="v100", site=site)  # kept
        VLAN.objects.create(vid=200, name="v200", site=site)  # stale
        VLAN.objects.create(vid=300, name="v300")  # no site -> unmatchable

        result = audit_model_rows("ipam.vlan", [{"site": "DC1", "vid": 100}])

        self.assertEqual(result["netbox_count"], 3)
        self.assertEqual(result["unmatchable_count"], 1)
        self.assertEqual(result["stale_count"], 1)
        self.assertEqual(result["stale_sample"], ["v200 (200)"])

    def test_audit_global_ipam_scope_filters_enabled_and_injects_rows(self):
        VRF.objects.create(name="present")
        VRF.objects.create(name="absent")
        sync = SimpleNamespace(get_model_strings=lambda: ["ipam.vrf", "dcim.device"])

        canned = {"ipam.vrf": [{"name": "present", "rd": None}]}
        payload = audit_global_ipam_scope(
            sync,
            client=None,
            logger=None,
            fetch_rows=lambda s, c, lg, model: canned.get(model, []),
        )

        self.assertEqual(payload["models_audited"], ["ipam.vrf"])
        self.assertEqual(payload["total_stale"], 1)
        self.assertEqual(payload["results"][0]["stale_sample"], ["absent"])

    def test_audit_model_rows_exposes_stale_pks(self):
        keep = VRF.objects.create(name="keep")
        stale = VRF.objects.create(name="stale")
        result = audit_model_rows("ipam.vrf", [{"name": "keep", "rd": None}])
        self.assertEqual(result["stale_pks"], [stale.pk])
        self.assertNotIn(keep.pk, result["stale_pks"])

    def _vrf_sync(self):
        return SimpleNamespace(get_model_strings=lambda: ["ipam.vrf", "dcim.device"])

    def test_tag_delete_eligible_tags_stale_and_skips_kept(self):
        keep = VRF.objects.create(name="present")
        stale = VRF.objects.create(name="absent")
        canned = {"ipam.vrf": [{"name": "present", "rd": None}]}

        payload = tag_delete_eligible_ipam(
            self._vrf_sync(),
            client=None,
            logger=None,
            fetch_rows=lambda s, c, lg, model: canned.get(model, []),
        )

        self.assertEqual(payload["tag_slug"], DELETE_ELIGIBLE_TAG_SLUG)
        self.assertEqual(payload["total_eligible"], 1)
        self.assertTrue(stale.tags.filter(slug=DELETE_ELIGIBLE_TAG_SLUG).exists())
        self.assertFalse(keep.tags.filter(slug=DELETE_ELIGIBLE_TAG_SLUG).exists())

    def test_tag_delete_eligible_is_self_healing(self):
        stale = VRF.objects.create(name="absent")
        VRF.objects.create(name="present")

        def fetch_only_present(s, c, lg, model):
            return [{"name": "present", "rd": None}]

        # First run: `absent` is stale -> tagged.
        tag_delete_eligible_ipam(
            self._vrf_sync(), None, None, fetch_rows=fetch_only_present
        )
        self.assertTrue(stale.tags.filter(slug=DELETE_ELIGIBLE_TAG_SLUG).exists())

        # `absent` returns to Forward -> tag is reconciled off on the next run.
        def fetch_both(s, c, lg, model):
            return [
                {"name": "present", "rd": None},
                {"name": "absent", "rd": None},
            ]

        payload = tag_delete_eligible_ipam(
            self._vrf_sync(), None, None, fetch_rows=fetch_both
        )
        self.assertEqual(payload["total_eligible"], 0)
        self.assertEqual(payload["results"][0]["untagged"], 1)
        self.assertFalse(stale.tags.filter(slug=DELETE_ELIGIBLE_TAG_SLUG).exists())

    def test_tag_delete_eligible_skips_empty_forward_fetch(self):
        # An empty/failed fetch must not flag every NetBox object as eligible.
        survivor = VRF.objects.create(name="lonely")
        payload = tag_delete_eligible_ipam(
            self._vrf_sync(),
            None,
            None,
            fetch_rows=lambda s, c, lg, model: [],
        )
        self.assertEqual(payload["total_eligible"], 0)
        self.assertEqual(payload["models_tagged"], [])
        self.assertEqual(
            payload["skipped"],
            [{"model": "ipam.vrf", "reason": "empty_forward_fetch"}],
        )
        self.assertFalse(survivor.tags.filter(slug=DELETE_ELIGIBLE_TAG_SLUG).exists())
