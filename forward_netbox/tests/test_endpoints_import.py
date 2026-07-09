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
        # Wording is a "can't verify locally" heads-up pointing at the live
        # check, not a confirmed failure.
        self.assertIn("Live Query Drift", drift[0]["remediation"])
        self.assertIn("can't inspect locally", drift[0]["message"])
        # Badge label must track the elevated status, not the stale build-time one.
        self.assertNotEqual(drift[0].get("status_label"), "Org-managed (pinned)")
        self.assertIn("verify", drift[0].get("status_label", ""))

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


from forward_netbox.utilities.query_fetch_execution import (  # noqa: E402
    ForwardQueryFetcher,
)


class EndpointScopeUnionTest(TestCase):
    """Tag-scoped syncs must keep opt-in endpoint rows, not silently drop them.

    scoped_device_names was built from network.devices only, so the local scope
    filter removed every endpoint row (and prune would delete them). With
    sync_endpoints on, endpoint names join the scoped set; exclude tags still
    apply to the endpoint probe.
    """

    def _fetcher(self, client):
        from unittest.mock import Mock

        return ForwardQueryFetcher(Mock(), client, Mock())

    def test_scope_union_includes_endpoint_names(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["ACI"]}],  # device scope
            [{"name": "avocent-1"}, {"name": "avocent-2"}],  # endpoint probe
        ]
        fetcher = self._fetcher(client)
        names, _sites, _matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["ACI"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
        )
        self.assertEqual(names, {"dev-1", "avocent-1", "avocent-2"})
        self.assertFalse(failed)

    def test_scope_without_endpoints_flag_is_unchanged(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = [
            {"name": "dev-1", "site": "dc1", "tagNames": ["ACI"]}
        ]
        fetcher = self._fetcher(client)
        names, _sites, _matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["ACI"],
            exclude_tags=[],
            include_match="any",
        )
        self.assertEqual(names, {"dev-1"})
        self.assertFalse(failed)
        client.run_nqe_query.assert_called_once()

    def test_endpoint_probe_honors_exclude_tags(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = [{"name": "avocent-1"}]
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_endpoint_names(
            network_id="n", snapshot_id="s", exclude_tags=["Decom"]
        )
        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn("network.endpoints", query)
        self.assertIn('"Decom" in endpoint.tagNames', query)

    def test_endpoint_probe_failure_warns_and_returns_none(self):
        from unittest.mock import Mock

        from forward_netbox.exceptions import ForwardClientError

        client = Mock()
        client.run_nqe_query.side_effect = ForwardClientError("boom")
        fetcher = self._fetcher(client)
        names = fetcher._resolve_scoped_endpoint_names(
            network_id="n", snapshot_id="s", exclude_tags=[]
        )
        self.assertIsNone(names)
        fetcher.logger.log_warning.assert_called_once()

    def test_endpoint_probe_failure_flags_scope_as_failed(self):
        # If the probe fails but the query still emitted endpoint rows, the
        # local filter would drop them — and prune would DELETE previously
        # imported endpoints. The failure flag lets resolve_context disable
        # endpoint emission for the run instead.
        from unittest.mock import Mock

        from forward_netbox.exceptions import ForwardClientError

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["ACI"]}],
            ForwardClientError("boom"),
        ]
        fetcher = self._fetcher(client)
        names, _sites, _matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["ACI"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
        )
        self.assertEqual(names, {"dev-1"})
        self.assertTrue(failed)


class EndpointBranchIncludeScopeRemovalTest(SimpleTestCase):
    """The endpoint branch must not gate on device INCLUDE tags (exclude only)."""

    def test_endpoint_branches_do_not_filter_include_tags(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            src = _read_query(filename)
            endpoint_branch = src.split("network.endpoints", 1)[1]
            self.assertIn("device_tag_exclude_tags", endpoint_branch, filename)
            self.assertNotIn("device_tag_include_tags", endpoint_branch, filename)


from forward_netbox.utilities.query_fetch_execution import (  # noqa: E402
    _sanitize_device_rows,
)


class DeviceRowSanitizeTest(SimpleTestCase):
    """Endpoint rows carry raw sysDescr as device_type — clamp to NetBox limits.

    Observed in the field: sysDescr up to 251 chars rejected with "Ensure this
    value has at most 100 characters", and a symbol-only sysDescr slugified to
    "" triggering "At least one coalesce lookup must be provided".
    """

    def test_long_device_type_and_slug_clamped_to_100(self):
        row = {
            "name": "ep-1",
            "device_type": "X" * 251,
            "device_type_slug": "x" * 251,
        }
        _sanitize_device_rows("dcim.device", [row])
        self.assertLessEqual(len(row["device_type"]), 100)
        self.assertLessEqual(len(row["device_type_slug"]), 100)

    def test_empty_slug_gets_fallback_from_value(self):
        row = {"name": "ep-2", "device_type": "ACS 6000", "device_type_slug": ""}
        _sanitize_device_rows("dcim.device", [row])
        self.assertEqual(row["device_type_slug"], "acs-6000")

    def test_symbol_only_value_gets_unknown_slug(self):
        row = {"name": "ep-3", "device_type": "!!##!!", "device_type_slug": ""}
        _sanitize_device_rows("dcim.device", [row])
        self.assertEqual(row["device_type_slug"], "unknown")

    def test_all_taxonomy_pairs_clamped(self):
        row = {
            "name": "ep-4",
            "manufacturer": "M" * 150,
            "manufacturer_slug": "m" * 150,
            "platform": "P" * 150,
            "platform_slug": "p" * 150,
            "role": "R" * 150,
            "role_slug": "r" * 150,
            "site": "S" * 150,
            "site_slug": "s" * 150,
        }
        _sanitize_device_rows("dcim.device", [row])
        for field in row:
            if field != "name":
                self.assertLessEqual(len(row[field]), 100, field)

    def test_normal_rows_and_other_models_untouched(self):
        row = {"name": "dev-1", "device_type": "ISR4331", "device_type_slug": "isr4331"}
        _sanitize_device_rows("dcim.device", [row])
        self.assertEqual(row["device_type"], "ISR4331")
        iface_row = {"name": "Gi0/0", "device": "X" * 251}
        _sanitize_device_rows("dcim.interface", [iface_row])
        self.assertEqual(len(iface_row["device"]), 251)
