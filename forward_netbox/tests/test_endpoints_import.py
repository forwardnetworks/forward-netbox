from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import _default_query_parameters
from forward_netbox.utilities.query_registry import _read_query


class EndpointImportWiringTest(SimpleTestCase):
    """The device query gains an opt-in SNMP-endpoint branch (Avocent etc.)."""

    def test_device_query_declares_sync_endpoints_default_off(self):
        params = _default_query_parameters("forward_devices.nqe")
        self.assertIn("sync_endpoints", params)
        self.assertIs(params["sync_endpoints"], False)

    def test_device_query_declares_device_tag_scope_params(self):
        params = _default_query_parameters("forward_devices.nqe")
        for key in (
            "forward_netbox_shard_keys",
            "device_tag_include_tags",
            "device_tag_include_match",
            "device_tag_exclude_tags",
        ):
            self.assertIn(key, params)

    def test_device_query_source_has_endpoint_branch(self):
        src = _read_query("forward_devices.nqe")
        # Opt-in gate + endpoint source + MIB-2 identity + Avocent overlay.
        self.assertIn("sync_endpoints: Bool", src)
        self.assertIn("network.endpoints", src)
        self.assertIn("where sync_endpoints", src)
        self.assertIn("1.3.6.1.2.1.1.1", src)  # sysDescr
        self.assertIn("1.3.6.1.2.1.1.2", src)  # sysObjectId
        self.assertIn("10418", src)  # Avocent enterprise OID overlay
        # Endpoints are scoped by the same device tags (they carry tagNames).
        self.assertIn("endpoint.tagNames", src)

    def test_device_query_still_emits_all_required_device_fields(self):
        src = _read_query("forward_devices.nqe")
        for field in (
            "manufacturer:",
            "device_type:",
            "device_type_slug:",
            "site:",
            "site_slug:",
            "role:",
            "role_slug:",
            "role_color:",
            "status:",
            "manufacturer_slug:",
        ):
            self.assertIn(field, src)


from forward_netbox.utilities.health import (  # noqa: E402
    _elevate_optin_pinned_query_drift,
)


class OptInPinnedDriftElevationTest(SimpleTestCase):
    """Enabling an opt-in feature on a stale pinned query must warn, not stay silent."""

    def _pinned(self, filename):
        return {
            "expected_filename": filename,
            "status": "direct_query_id_unverified",
            "status_label": "Org-managed (pinned)",
            "severity": "info",
            "message": "Org-managed pinned.",
        }

    def test_endpoints_enabled_elevates_pinned_device_query(self):
        drift = [self._pinned("forward_devices.nqe")]
        _elevate_optin_pinned_query_drift(drift, {"sync_endpoints": True})
        self.assertEqual(drift[0]["severity"], "warn")
        self.assertEqual(drift[0]["status"], "direct_query_id_optin_stale_risk")
        self.assertIn("Refresh Query IDs", drift[0]["remediation"])
        # Badge label must track the elevated status, not the stale build-time one.
        self.assertNotEqual(drift[0].get("status_label"), "Org-managed (pinned)")
        self.assertIn("predate", drift[0].get("status_label", ""))

    def test_endpoints_disabled_leaves_pinned_device_query_silent(self):
        drift = [self._pinned("forward_devices.nqe")]
        _elevate_optin_pinned_query_drift(drift, {"sync_endpoints": False})
        self.assertEqual(drift[0]["severity"], "info")
        self.assertEqual(drift[0]["status"], "direct_query_id_unverified")

    def test_nonempty_device_tags_elevate_pinned_feature_tag_query(self):
        drift = [self._pinned("forward_device_feature_tags.nqe")]
        _elevate_optin_pinned_query_drift(drift, {"sync_device_tags": ["Mgmt_Lo0"]})
        self.assertEqual(drift[0]["severity"], "warn")

    def test_empty_device_tags_leave_feature_tag_query_silent(self):
        drift = [self._pinned("forward_device_feature_tags.nqe")]
        _elevate_optin_pinned_query_drift(drift, {"sync_device_tags": []})
        self.assertEqual(drift[0]["severity"], "info")

    def test_non_pinned_modes_untouched(self):
        drift = [
            {
                "expected_filename": "forward_devices.nqe",
                "status": "bundled_raw_match",
                "severity": "pass",
            }
        ]
        _elevate_optin_pinned_query_drift(drift, {"sync_endpoints": True})
        self.assertEqual(drift[0]["severity"], "pass")


from django.test import TestCase  # noqa: E402

from forward_netbox.forms import ForwardSourceForm  # noqa: E402


class EndpointFormRenderTest(TestCase):
    """The opt-in toggle must actually render in the source form."""

    def test_sync_endpoints_toggle_is_in_a_fieldset(self):
        form = ForwardSourceForm()
        self.assertIn("sync_endpoints", form.fields)
        rendered = []
        for fs in form.fieldsets:
            rendered.extend(
                str(name)
                for name in (
                    getattr(fs, "items", None) or getattr(fs, "fields", None) or []
                )
            )
        self.assertIn(
            "sync_endpoints",
            rendered,
            "sync_endpoints field exists but is not in any FieldSet, so the "
            "form never renders the toggle.",
        )
