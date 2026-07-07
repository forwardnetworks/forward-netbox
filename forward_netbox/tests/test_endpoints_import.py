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
