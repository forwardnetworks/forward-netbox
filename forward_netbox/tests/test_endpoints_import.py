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
            "status_label": "Direct ID (fixed)",
            "severity": "info",
            "message": "Org-managed pinned.",
        }

    def test_endpoints_enabled_elevates_pinned_device_query(self):
        drift = [self._pinned("forward_devices.nqe")]
        _elevate_optin_pinned_query_drift(drift, {"sync_endpoints": True})
        self.assertEqual(drift[0]["severity"], "warn")
        self.assertEqual(drift[0]["status"], "direct_query_id_optin_stale_risk")
        self.assertIn("Publish Bundled Queries", drift[0]["remediation"])
        # Wording is a "can't verify locally" heads-up pointing at the live
        # check, not a confirmed failure.
        self.assertIn("Live Query Drift", drift[0]["remediation"])
        self.assertIn("can't inspect locally", drift[0]["message"])
        # Badge label must track the elevated status, not the stale build-time one.
        self.assertNotEqual(drift[0].get("status_label"), "Direct ID (fixed)")
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
from forward_netbox.models import ForwardSource  # noqa: E402


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

    def test_new_sources_default_endpoint_include_scope_on(self):
        form = ForwardSourceForm()
        self.assertIs(form.fields["scope_endpoints_by_include_tags"].initial, True)

    def test_existing_sources_preserve_endpoint_include_scope_off(self):
        source = ForwardSource.objects.create(
            name="existing-source",
            url="https://forward.example.invalid",
            parameters={
                "device_tag_include_tags": ["Prod"],
                "scope_endpoints_by_include_tags": False,
                "scope_endpoints_by_include_tags_configured": True,
            },
        )

        form = ForwardSourceForm(instance=source)

        self.assertIs(
            form.fields["scope_endpoints_by_include_tags"].initial,
            False,
        )

    def test_legacy_sources_with_include_tags_fail_closed(self):
        source = ForwardSource.objects.create(
            name="legacy-source",
            url="https://forward.example.invalid",
            parameters={
                "device_tag_include_tags": ["Prod"],
                "scope_endpoints_by_include_tags": False,
            },
        )

        form = ForwardSourceForm(instance=source)

        self.assertIs(form.fields["scope_endpoints_by_include_tags"].initial, True)


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

    def test_scope_union_carries_endpoint_include_tags_for_netbox_tagging(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["ACI"]}],
            [
                {"name": "opengear-1", "tagNames": ["ACI", "Console"]},
                {"name": "opengear-2", "tagNames": ["Console"]},
            ],
        ]
        fetcher = self._fetcher(client)

        names, _sites, matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["ACI"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
        )

        self.assertEqual(names, {"dev-1", "opengear-1", "opengear-2"})
        self.assertEqual(matched, {"dev-1": ["ACI"], "opengear-1": ["ACI"]})
        self.assertFalse(failed)
        endpoint_query = client.run_nqe_query.call_args_list[1].kwargs["query"]
        self.assertIn("tagNames: endpoint.tagNames", endpoint_query)

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


class EndpointBranchIncludeScopeTest(SimpleTestCase):
    """The endpoint include gate is opt-in: by default the endpoint branch
    filters by exclude tags only (2.4.4 behavior); include tags apply only
    behind scope_endpoints_by_include_tags, honoring the "all"/"any" match.
    """

    def test_endpoint_branches_gate_include_tags_behind_toggle(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            src = _read_query(filename)
            self.assertIn("scope_endpoints_by_include_tags: Bool", src, filename)
            endpoint_branch = src.split("network.endpoints", 1)[1]
            self.assertIn("device_tag_exclude_tags", endpoint_branch, filename)
            # Include tags are referenced ONLY behind the opt-in toggle: the
            # gate short-circuits to true when the toggle is off.
            self.assertIn(
                "where !scope_endpoints_by_include_tags", endpoint_branch, filename
            )
            self.assertIn("isEmpty(device_tag_include_tags)", endpoint_branch, filename)
            # Both match modes are honored ("all" = no include tag missing;
            # otherwise "any").
            self.assertIn(
                'device_tag_include_match == "all"', endpoint_branch, filename
            )
            self.assertIn(
                "select !(tag in endpoint.tagNames)", endpoint_branch, filename
            )
            self.assertIn(
                'device_tag_include_match != "all"', endpoint_branch, filename
            )

    def test_endpoint_branches_exclude_cimc_management_controllers(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            endpoint_branch = _read_query(filename).split("network.endpoints", 1)[1]
            self.assertIn("endpoint.profileName", endpoint_branch, filename)
            self.assertIn('matches(endpointProfileName, "*cimc*")', endpoint_branch)
            self.assertIn(
                'matches(sdLower, "*cisco integrated management controller*")',
                endpoint_branch,
            )
            self.assertIn("where !isCimc", endpoint_branch, filename)

    def test_device_query_declares_toggle_default_off(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            params = _default_query_parameters(filename)
            self.assertIn("scope_endpoints_by_include_tags", params, filename)
            self.assertIs(params["scope_endpoints_by_include_tags"], False, filename)


class EndpointIdentityClampTest(SimpleTestCase):
    """Endpoint identity must be stable and clamped in NQE, not Python.

    sysDescr includes firmware and build metadata on common console servers.
    Hardware identity strips those volatile suffixes; non-console endpoints use
    one manufacturer-scoped generic model rather than arbitrary sysDescr text.
    """

    def test_endpoint_branches_normalize_model_and_guard_slugs(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            src = _read_query(filename)
            endpoint_branch = src.split("network.endpoints", 1)[1]
            self.assertIn("let opengear_model", endpoint_branch, filename)
            self.assertIn("re`,.*`", endpoint_branch, filename)
            self.assertIn("re` [0-9]+\\.[0-9]+.*$`", endpoint_branch, filename)
            self.assertIn("let avocent_model", endpoint_branch, filename)
            self.assertIn("re` - version:.*`", endpoint_branch, filename)
            self.assertIn('ep_manuf + " SNMP Endpoint"', endpoint_branch, filename)
            self.assertIn(
                "let ep_model = substring(ep_model_raw, 0, 100)",
                endpoint_branch,
                filename,
            )
            self.assertIn(
                'if ep_model_slug_raw == "" then "unknown"', endpoint_branch, filename
            )
            self.assertIn(
                'if ep_manuf_raw == "" then "Unknown"', endpoint_branch, filename
            )
            # The select must emit the clamped values, never raw sysDescr.
            self.assertIn("device_type: ep_model,", endpoint_branch, filename)
            self.assertNotIn("device_type: sysDescr", endpoint_branch, filename)

    def test_opengear_is_a_console_server_with_stable_manufacturer(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            endpoint_branch = _read_query(filename).split("network.endpoints", 1)[1]
            self.assertIn("1.3.6.1.4.1.25049.*", endpoint_branch, filename)
            self.assertIn('matches(sdLower, "*opengear*")', endpoint_branch, filename)
            self.assertIn('if isOpengear then "Opengear"', endpoint_branch, filename)
            self.assertIn(
                "let isConsoleServer = isAvocent || isOpengear",
                endpoint_branch,
                filename,
            )
            self.assertIn(
                'let ep_role = if isConsoleServer then "Console Server"',
                endpoint_branch,
                filename,
            )


class BlankDeviceTypeGuardTest(SimpleTestCase):
    """No row may reach NetBox with a blank device_type (rejects DeviceType.model).

    Two sources of blank device_type: a device with no resolved model
    (device.platform.model null/empty) and an SNMP endpoint reporting a
    present-but-empty sysDescr. Both must fall back in the query.
    """

    def test_device_branch_guards_empty_or_null_model(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            device_branch = _read_query(filename).split("network.endpoints", 1)[0]
            self.assertIn('"Unknown"', device_branch, filename)
            # null-safe guard, not just == "" (model stringifies to null).
            self.assertIn("isPresent(", device_branch, filename)

    def test_endpoint_branch_guards_empty_sysdescr(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            endpoint_branch = _read_query(filename).split("network.endpoints", 1)[1]
            # An empty (present-but-"") sysDescr must fall back, not become "".
            self.assertIn(
                'let hasDescr = isPresent(sysDescrOpt) && sysDescrOpt != ""',
                endpoint_branch,
                filename,
            )
            self.assertIn(
                'if hasDescr then sysDescrOpt else "SNMP Endpoint"',
                endpoint_branch,
                filename,
            )
            # A missing sysDescr must NOT masquerade as a vendor named "SNMP":
            # the manufacturer falls back to "Unknown" instead.
            self.assertIn('else "Unknown"', endpoint_branch, filename)


class AvocentUnificationTest(SimpleTestCase):
    """One physical vendor = one platform. Avocent/Cyclades/AlterPath fold into
    'Avocent' via enterprise OIDs 10418 + 2925 and product-name signatures, and a
    multiline sysDescr is whitespace-collapsed before the first-token manufacturer
    is derived (so 'Cisco\\nTechnical\\nCopyright...' can't leak in)."""

    def _endpoint_branch(self, filename):
        return _read_query(filename).split("network.endpoints", 1)[1]

    def test_avocent_overlay_covers_both_oids_and_brand_signatures(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            branch = self._endpoint_branch(filename)
            self.assertIn("1.3.6.1.4.1.10418.*", branch, filename)
            self.assertIn("1.3.6.1.4.1.2925.*", branch, filename)
            for sig in ("*avocent*", "*cyclades*", "*alterpath*"):
                self.assertIn(sig, branch, filename)

    def test_multiline_sysdescr_is_collapsed_before_tokenizing(self):
        for filename in (
            "forward_devices.nqe",
            "forward_devices_with_netbox_aliases.nqe",
        ):
            branch = self._endpoint_branch(filename)
            self.assertIn(
                'replaceRegexMatches(sysDescr, re`\\s+`, " ")', branch, filename
            )
            # The first-token manufacturer derives from the cleaned string.
            self.assertIn(
                'replaceRegexMatches(sysDescrClean, re` .*`, "")', branch, filename
            )

    def test_endpoint_branches_stay_byte_identical(self):
        # The two device queries share the endpoint branch verbatim; the parity
        # is what lets a single fix land in both.
        base = _read_query("forward_devices.nqe").split("network.endpoints", 1)[1]
        alias = _read_query("forward_devices_with_netbox_aliases.nqe").split(
            "network.endpoints", 1
        )[1]
        self.assertEqual(base, alias)


from forward_netbox.utilities.forward_api import (  # noqa: E402
    build_endpoint_tag_scope_where,
)


class EndpointIncludeScopeProbeTest(TestCase):
    """The endpoint scope probe must apply the same include gate the query
    branch does (a drift would make the local filter drop rows the branch
    emits — with prune enabled, DELETEs of previously imported endpoints).
    """

    def _fetcher(self, client):
        from unittest.mock import Mock

        return ForwardQueryFetcher(Mock(), client, Mock())

    def test_probe_toggle_off_is_exclude_only(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = [{"name": "avocent-1"}]
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_endpoint_names(
            network_id="n",
            snapshot_id="s",
            exclude_tags=["Decom"],
            include_tags=["N.Patel"],
            include_match="any",
            scope_endpoints_by_include_tags=False,
        )
        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn('"Decom" in endpoint.tagNames', query)
        # 2.4.4 regression guard: the include tag must NOT gate the probe when
        # the toggle is off.
        self.assertNotIn("N.Patel", query)

    def test_probe_toggle_on_applies_include_any(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = [{"name": "avocent-1"}]
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_endpoint_names(
            network_id="n",
            snapshot_id="s",
            exclude_tags=[],
            include_tags=["N.Patel", "B.Chalasani"],
            include_match="any",
            scope_endpoints_by_include_tags=True,
        )
        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn(
            'where ("N.Patel" in endpoint.tagNames || "B.Chalasani" in endpoint.tagNames)',
            query,
        )

    def test_probe_toggle_on_applies_include_all(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = []
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_endpoint_names(
            network_id="n",
            snapshot_id="s",
            exclude_tags=[],
            include_tags=["N.Patel", "B.Chalasani"],
            include_match="all",
            scope_endpoints_by_include_tags=True,
        )
        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn('where "N.Patel" in endpoint.tagNames', query)
        self.assertIn('where "B.Chalasani" in endpoint.tagNames', query)

    def test_probe_excludes_cimc_by_profile_and_sysdescr(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.return_value = []
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_endpoint_names(
            network_id="n",
            snapshot_id="s",
            exclude_tags=[],
        )

        query = client.run_nqe_query.call_args.kwargs["query"]
        self.assertIn("endpoint.profileName", query)
        self.assertIn('matches(endpointProfileName, "*cimc*")', query)
        self.assertIn("*cisco integrated management controller*", query)
        self.assertIn("where !isCimc", query)

    def test_builder_mirrors_device_scope_semantics(self):
        # Golden parity with build_device_tag_scope_where, targeting
        # endpoint.tagNames.
        self.assertEqual(
            build_endpoint_tag_scope_where(["A"], ["X"], "any"),
            [
                'where ("A" in endpoint.tagNames)',
                'where !("X" in endpoint.tagNames)',
            ],
        )
        self.assertEqual(
            build_endpoint_tag_scope_where(["A", "B"], [], "all"),
            [
                'where "A" in endpoint.tagNames',
                'where "B" in endpoint.tagNames',
            ],
        )
        self.assertEqual(
            build_endpoint_tag_scope_where([], ["X"], "any"),
            ['where !("X" in endpoint.tagNames)'],
        )

    def test_scope_union_threads_toggle_to_probe(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["N.Patel"]}],
            [{"name": "avocent-1"}],
        ]
        fetcher = self._fetcher(client)
        names, _sites, _matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["N.Patel"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
            scope_endpoints_by_include_tags=True,
        )
        self.assertEqual(names, {"dev-1", "avocent-1"})
        self.assertFalse(failed)
        endpoint_query = client.run_nqe_query.call_args_list[1].kwargs["query"]
        self.assertIn('"N.Patel" in endpoint.tagNames', endpoint_query)


class ScopeMaskingWarningTest(TestCase):
    """A tag scope that matches 0 collected devices while endpoints still
    import used to present as a confusing partial import (devices appear,
    interfaces/IPs empty). The resolver must say so explicitly.
    """

    def _fetcher(self, client):
        from unittest.mock import Mock

        return ForwardQueryFetcher(Mock(), client, Mock())

    def test_zero_collected_devices_with_endpoints_warns(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [],  # device scope: no collected device carries the tag
            [],  # all-backfilled probe (fires whenever the scope is empty)
            [{"name": "avocent-1"}, {"name": "avocent-2"}],  # endpoint probe
        ]
        fetcher = self._fetcher(client)
        names, _sites, _matched, failed = fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["N.Patel"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
        )
        self.assertEqual(names, {"avocent-1", "avocent-2"})
        self.assertFalse(failed)
        warnings = [
            str(call.args[0]) for call in fetcher.logger.log_warning.call_args_list
        ]
        self.assertTrue(
            any(
                "matched 0 collected devices" in message
                and "interfaces and IP addresses" in message
                for message in warnings
            ),
            warnings,
        )

    def test_matched_devices_do_not_warn_about_masking(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["N.Patel"]}],
            [{"name": "avocent-1"}],
        ]
        fetcher = self._fetcher(client)
        fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["N.Patel"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
        )
        warnings = [
            str(call.args[0]) for call in fetcher.logger.log_warning.call_args_list
        ]
        self.assertFalse(
            any("matched 0 collected devices" in message for message in warnings),
            warnings,
        )

    def test_include_scope_bypass_warns_before_untagged_endpoints_import(self):
        from unittest.mock import Mock

        client = Mock()
        client.run_nqe_query.side_effect = [
            [{"name": "dev-1", "site": "dc1", "tagNames": ["ACI"]}],
            [{"name": "opengear-1", "tagNames": []}],
        ]
        fetcher = self._fetcher(client)

        fetcher._resolve_scoped_tag_scope(
            network_id="n",
            snapshot_id="s",
            include_tags=["ACI"],
            exclude_tags=[],
            include_match="any",
            sync_endpoints=True,
            scope_endpoints_by_include_tags=False,
        )

        warnings = [
            str(call.args[0]) for call in fetcher.logger.log_warning.call_args_list
        ]
        self.assertTrue(
            any(
                "not constrained by the configured include tags" in message
                and "Scope SNMP Endpoints by Include Tags" in message
                for message in warnings
            ),
            warnings,
        )


class EndpointScopeToggleFormTest(TestCase):
    """The opt-in include-scope toggle must render and round-trip."""

    def test_toggle_is_in_a_fieldset(self):
        form = ForwardSourceForm()
        self.assertIn("scope_endpoints_by_include_tags", form.fields)
        rendered = []
        for fs in form.fieldsets:
            rendered.extend(
                str(name)
                for name in (
                    getattr(fs, "items", None) or getattr(fs, "fields", None) or []
                )
            )
        self.assertIn("scope_endpoints_by_include_tags", rendered)


class ScopeEndpointsAllowlistTest(TestCase):
    """clean_forward_source must accept the new source key (bool only)."""

    def _source(self, **parameters):
        from forward_netbox.models import ForwardSource

        return ForwardSource(
            name="allowlist-src",
            type="saas",
            url="https://fwd.app",
            parameters={
                "username": "user@example.com",
                "password": "secret",
                "verify": True,
                **parameters,
            },
        )

    def test_bool_value_is_accepted(self):
        from forward_netbox.utilities.model_validation import clean_forward_source

        clean_forward_source(self._source(scope_endpoints_by_include_tags=True))

    def test_configured_marker_bool_is_accepted(self):
        from forward_netbox.utilities.model_validation import clean_forward_source

        clean_forward_source(
            self._source(scope_endpoints_by_include_tags_configured=True)
        )

    def test_non_bool_value_is_rejected(self):
        from django.core.exceptions import ValidationError

        from forward_netbox.utilities.model_validation import clean_forward_source

        with self.assertRaises(ValidationError):
            clean_forward_source(self._source(scope_endpoints_by_include_tags="yes"))

    def test_non_bool_configured_marker_is_rejected(self):
        from django.core.exceptions import ValidationError

        from forward_netbox.utilities.model_validation import clean_forward_source

        with self.assertRaises(ValidationError):
            clean_forward_source(
                self._source(scope_endpoints_by_include_tags_configured="yes")
            )


class DuplicateDeviceNameLookupTest(TestCase):
    """NetBox Device.name is unique per site, not globally; a duplicate name
    must resolve deterministically instead of raising MultipleObjectsReturned
    and failing the whole apply workload (field report: SNMP endpoints raise
    collision odds).
    """

    def test_duplicate_name_resolves_to_earliest_and_warns(self):
        from unittest.mock import Mock

        from dcim.models import Device
        from dcim.models import DeviceRole
        from dcim.models import DeviceType
        from dcim.models import Manufacturer
        from dcim.models import Site

        from forward_netbox.utilities.sync_primitives import get_device_by_name

        manufacturer = Manufacturer.objects.create(name="M", slug="m")
        device_type = DeviceType.objects.create(
            manufacturer=manufacturer, model="T", slug="t"
        )
        role = DeviceRole.objects.create(name="R", slug="r")
        site_a = Site.objects.create(name="Site A", slug="site-a")
        site_b = Site.objects.create(name="Site B", slug="site-b")
        first = Device.objects.create(
            name="dup-1", device_type=device_type, role=role, site=site_a
        )
        Device.objects.create(
            name="dup-1", device_type=device_type, role=role, site=site_b
        )

        runner = Mock()
        runner._device_by_name_cache = {}
        runner._missing_device_by_name_cache = set()

        device = get_device_by_name(runner, "dup-1")

        self.assertEqual(device.pk, first.pk)
        runner.logger.log_warning.assert_called_once()
        self.assertIn(
            "Multiple NetBox devices share the name",
            str(runner.logger.log_warning.call_args.args[0]),
        )
