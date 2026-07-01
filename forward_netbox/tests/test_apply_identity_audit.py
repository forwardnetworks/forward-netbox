from types import SimpleNamespace

from dcim.models import Site
from django.test import TestCase

from forward_netbox.utilities.apply_identity_audit import audit_apply_identity
from forward_netbox.utilities.apply_identity_audit import audit_model_identity


class ApplyIdentityAuditTest(TestCase):
    def test_clean_when_keys_match(self):
        Site.objects.create(name="Keep", slug="keep")
        rows = [{"slug": "keep", "name": "Keep"}]
        result = audit_model_identity("dcim.site", rows)
        self.assertEqual(result["would_create_count"], 0)
        self.assertEqual(result["would_delete_count"], 0)
        self.assertFalse(result["churn_suspect"])

    def test_slug_mismatch_is_flagged_as_churn(self):
        # Same logical site, but NetBox stored a different slug/name than Forward
        # now computes -> the apply engine would delete the orphan and create the
        # new one every sync. This is the 1-created/1-deleted signature.
        Site.objects.create(name="Old Name", slug="old-slug")
        rows = [{"slug": "new-slug", "name": "New Name"}]
        result = audit_model_identity("dcim.site", rows)
        self.assertEqual(result["would_create_count"], 1)
        self.assertEqual(result["would_delete_count"], 1)
        self.assertTrue(result["churn_suspect"])
        self.assertIn("old-slug", result["would_delete_sample"][0])
        self.assertIn("new-slug", result["would_create_sample"][0])

    def test_name_set_match_is_not_false_churn(self):
        # Forward slug differs but the NAME still matches -> the apply engine
        # matches on the name lookup_set, so it is NOT churn.
        Site.objects.create(name="Shared", slug="stored-slug")
        rows = [{"slug": "different-slug", "name": "Shared"}]
        result = audit_model_identity("dcim.site", rows)
        self.assertEqual(result["would_delete_count"], 0)
        self.assertEqual(result["would_create_count"], 0)
        self.assertFalse(result["churn_suspect"])

    def test_churn_pairs_names_the_object_and_diff(self):
        # The 1/1 churn signature should pinpoint the SAME logical object and the
        # exact key disagreement (here: underscore vs dash in the slug).
        # Both keys must differ for it to be churn (a matching name would match
        # on the name lookup_set): NetBox slug dc_one / name "DC 1"; Forward
        # slug dc-one / name "DC One".
        Site.objects.create(name="DC 1", slug="dc_one")
        rows = [{"slug": "dc-one", "name": "DC One"}]
        result = audit_model_identity("dcim.site", rows)
        self.assertTrue(result["churn_suspect"])
        self.assertEqual(len(result["churn_pairs"]), 1)
        pair = result["churn_pairs"][0]
        self.assertIn("dc_one", pair["netbox"])
        self.assertIn("dc-one", pair["forward"])
        self.assertEqual(pair["differs_on"], "underscore-vs-dash")
        self.assertGreater(pair["similarity"], 0.8)

    def test_device_name_churn_is_audited(self):
        from dcim.models import Device, DeviceRole, DeviceType, Manufacturer

        site = Site.objects.create(name="S", slug="s")
        mfr = Manufacturer.objects.create(name="M", slug="m")
        dt = DeviceType.objects.create(manufacturer=mfr, model="DT", slug="dt")
        role = DeviceRole.objects.create(name="R", slug="r")
        Device.objects.create(name="fw01-vsys1", device_type=dt, role=role, site=site)
        # Forward now emits a trailing-space variant -> name-key flip-flop.
        result = audit_model_identity("dcim.device", [{"name": "fw01-vsys1 "}])
        self.assertTrue(result["churn_suspect"])
        self.assertEqual(result["churn_pairs"][0]["differs_on"], "whitespace")

    def test_audit_filters_to_enabled_models(self):
        Site.objects.create(name="A", slug="a")
        sync = SimpleNamespace(get_model_strings=lambda: ["dcim.site", "dcim.device"])
        canned = {"dcim.site": [{"slug": "a", "name": "A"}]}
        payload = audit_apply_identity(
            sync, fetch_rows=lambda s, model: canned.get(model, [])
        )
        # dcim.device is now an audited model; with no device rows it is clean.
        self.assertEqual(payload["models_audited"], ["dcim.site", "dcim.device"])
        self.assertEqual(payload["churn_suspect_models"], [])
