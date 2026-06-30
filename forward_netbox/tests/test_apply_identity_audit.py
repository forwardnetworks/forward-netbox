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

    def test_audit_filters_to_enabled_models(self):
        Site.objects.create(name="A", slug="a")
        sync = SimpleNamespace(get_model_strings=lambda: ["dcim.site", "dcim.device"])
        canned = {"dcim.site": [{"slug": "a", "name": "A"}]}
        payload = audit_apply_identity(
            sync, fetch_rows=lambda s, model: canned.get(model, [])
        )
        self.assertEqual(payload["models_audited"], ["dcim.site"])
        self.assertEqual(payload["churn_suspect_models"], [])
