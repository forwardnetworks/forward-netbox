import unittest
from unittest.mock import patch

from django.apps import apps
from django.test import SimpleTestCase

from forward_netbox.utilities.apply_engine_decision import (
    COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.merge_set_based import (
    SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.version_series import series_matches

# An optional NetBox plugin upgrade must not silently disable the fast paths.
#
# The runtime gates for the fast baseline, set-based merge and COPY/SQL each
# pinned every optional distribution to one exact version and compared the whole
# tuple. Upgrading netbox-dlm therefore turned the fast baseline off with no
# error an operator could see — a first sync going from minutes to hours — and
# the install pin (`netbox-dlm = "0.4.1"`) refused the upgrade outright.
#
# Each entry now lists the versions validated against that engine. netbox-dlm
# 0.5.0 qualifies on inspection of its schema delta against 0.4.1: it adds
# `SoftwareVersion.release_designation` (CharField, blank) and a unique
# constraint on (platform, release_designation) conditioned on that field being
# non-empty. This plugin never sets it, so its rows carry '' and the constraint
# cannot apply to them; the coalesce key it does use, (platform, version), is
# unchanged.
#
# 0.9.1 qualifies on the same inspection against 0.8.0: its `models.py` delta is
# entirely direct model imports becoming lazy string references
# (`to=Platform` -> `to='dcim.Platform'`), which Django resolves identically and
# which generates no migration. No field, model or constraint changed, so the
# whole surface this plugin binds to - `SoftwareVersion.platform`, `.version`,
# `.alias`, and the (platform, version) coalesce key - is untouched.
#
# 0.9.0 is deliberately absent, and the test below pins its absence. It shipped
# roughly half an hour before 0.9.1 with a NoReverseMatch on every one of its
# view URLs; 0.9.1 is that fix. Skipping a version that cannot render is not the
# same as failing to validate it.


class OptionalDistributionVersionSetTest(SimpleTestCase):
    """The validated optional-distribution sets, whatever they currently hold.

    On NetBox 4.7 they hold one entry: netbox-dlm 0.10.0, the first release of
    any optional plugin to raise its ceiling past 4.6.99. netbox-cisco-aci,
    netbox-peering-manager, netbox-routing and netbox-validity still declare a
    max_version in the 4.6 series, and NetBox refuses to start with a plugin
    outside its declared range, so none of them can be installed on this
    runtime. Claiming a validated version for a plugin nobody can install would
    be a claim about a runtime nobody can assemble.

    The 4.6 validations of netbox-dlm 0.4.1 through 0.9.1 are deliberately NOT
    carried over: they were evidence about 4.6, and none of those releases can
    boot here anyway. They are recorded in
    `docs/03_Plans/active/2026-09-02-netbox-4.7-runtime.md`.
    """

    def test_both_gates_read_the_same_declaration(self):
        # One source, two consumers. When these diverge the fast paths disable
        # themselves silently, which is the failure this module exists to catch.
        self.assertIs(
            COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
            SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
        )

    def test_only_netbox_dlm_0_10_0_is_validated_on_this_runtime(self):
        self.assertEqual(
            dict(COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS),
            {"netbox-dlm": frozenset({"0.10.0"})},
            "the other four optional plugins cannot be installed on NetBox "
            "4.7, so a validated version for one is a claim about an "
            "unbuildable runtime; and no netbox-dlm before 0.10.0 can boot here",
        )

    def test_any_entry_that_returns_names_a_real_version(self):
        # Guards the way back in: an empty frozenset would accept nothing while
        # reading as configured, and a bare string would accept every substring.
        for name, supported in COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS.items():
            with self.subTest(distribution=name):
                self.assertIsInstance(supported, frozenset)
                self.assertTrue(supported)
                for version in supported:
                    self.assertRegex(version, r"^\d+\.\d+")

    def test_an_unvalidated_version_is_refused_whatever_the_set_holds(self):
        # The gates fail closed; widening must never become "any version".
        for gate, supported in (
            ("copy_sql", COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS),
            ("set_based", SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS),
        ):
            with self.subTest(gate=gate):
                self.assertNotIn(
                    "999.999.999", supported.get("netbox-dlm", frozenset())
                )


class FastBaselineRuntimeTupleTest(SimpleTestCase):
    def test_any_patch_in_the_series_is_accepted(self):
        # The policy: 4.6.x and 1.1.x, not an exact pin. A patch release must
        # not silently switch an engine off.
        for version in ("4.6.5", "4.6.6", "4.6.12", "4.6"):
            self.assertTrue(series_matches(version, "4.6"), version)
        # The prerelease stays: anyone who installed the beta before 1.2.0
        # shipped must not be refused by a series check that only ever saw
        # release strings.
        for version in ("1.2.0b1", "1.2.0", "1.2.9"):
            self.assertTrue(series_matches(version, "1.2"), version)

    def test_a_different_series_is_still_refused(self):
        # Permissive within a series is not permissive across one.
        for version in ("4.6.8", "4.5.9", "5.0.0", "", None, "4.70.1"):
            self.assertFalse(series_matches(version, "4.7"), version)
        for version in ("1.1.3", "1.0.9", "2.0.0"):
            self.assertFalse(series_matches(version, "1.2"), version)

    def _decide(self, *, optional_plugins=None, plugin_apps=None, netbox="4.7.0"):
        """A runtime tuple in the 4.7 shape.

        The default is the validated tuple: forward_netbox, Branching and
        netbox-dlm 0.10.0. Two things can be varied under it - the netbox-dlm
        version (the thing customers change under us) and whether an UNEXPECTED
        app is present - and both must fail closed.
        """
        from forward_netbox.utilities import fast_baseline

        tuple_ = {
            "netbox": netbox,
            "branching": "1.2.0",
            "forward_netbox": fast_baseline.forward_config.version,
            "optional_plugins": (
                {"netbox-dlm": "0.10.0"}
                if optional_plugins is None
                else optional_plugins
            ),
            "plugin_apps": sorted(
                plugin_apps or {"forward_netbox", "netbox_branching", "netbox_dlm"}
            ),
        }
        with patch.object(
            fast_baseline, "fast_baseline_runtime_tuple", return_value=tuple_
        ):
            return fast_baseline._runtime_decision()

    def test_the_validated_runtime_keeps_the_fast_baseline(self):
        self.assertNotEqual(self._decide().reason_code, "unsupported_runtime_tuple")

    def test_a_later_netbox_patch_keeps_the_fast_baseline(self):
        # Series matching, not an exact pin: a 4.7 patch must not silently drop
        # a deployment onto the slow path.
        self.assertNotEqual(
            self._decide(netbox="4.7.9").reason_code, "unsupported_runtime_tuple"
        )

    def test_a_netbox_dlm_validated_only_on_4_6_fails_closed(self):
        # 0.9.1 was validated against 4.6 and cannot boot on 4.7. A runtime
        # reporting it is one this was never checked against, whatever the
        # 2.9.x lane says about the same version.
        decision = self._decide(optional_plugins={"netbox-dlm": "0.9.1"})

        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_runtime_tuple")

    def test_netbox_dlm_absent_from_plugins_fails_closed(self):
        # The set is an exact match in both directions: an app the validated
        # tuple expects and PLUGINS lacks is as much a mismatch as a stranger.
        decision = self._decide(
            optional_plugins={},
            plugin_apps={"forward_netbox", "netbox_branching"},
        )

        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_runtime_tuple")

    def test_an_unexpected_plugin_app_fails_closed(self):
        # The app set is exact equality: an extra app is an unvalidated runtime
        # even when it declares no version at all.
        decision = self._decide(
            plugin_apps={"forward_netbox", "netbox_branching", "some_other_plugin"}
        )

        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_runtime_tuple")

    def test_the_rejection_detail_stays_serialisable(self):
        # `expected` holds sets; it is persisted as job evidence, so it must
        # render as sorted lists rather than blow up on JSON encoding.
        import json

        detail = self._decide(
            plugin_apps={"forward_netbox", "netbox_branching", "some_other_plugin"}
        ).context

        json.dumps(detail)
        self.assertEqual(
            detail["expected"]["optional_plugins"], {"netbox-dlm": ["0.10.0"]}
        )


class FastBaselineFieldContractTest(SimpleTestCase):
    """Series matching needs a behavioural backstop for the direct loader.

    The fast baseline `bulk_create`s straight into main, bypassing branch audit
    and the per-object save path. Accepting any 4.6.x means a future patch could
    add a column it never populates. Its search-index contract is already read
    live from `get_indexer`, so the gap was the model fields themselves.
    """

    def test_the_live_runtime_matches_the_recorded_contract(self):
        from forward_netbox.utilities.fast_baseline import (
            fast_baseline_field_contract_drift,
        )

        self.assertEqual(fast_baseline_field_contract_drift(), [])

    def test_a_new_required_field_fails_closed(self):
        from unittest.mock import patch as _patch

        from forward_netbox.utilities import fast_baseline

        contract = dict(fast_baseline.FAST_BASELINE_REQUIRED_FIELD_CONTRACT)
        contract["dcim.site"] = ("name", "slug", "a_field_netbox_added_later")
        with _patch.object(
            fast_baseline, "FAST_BASELINE_REQUIRED_FIELD_CONTRACT", contract
        ):
            drift = fast_baseline.fast_baseline_field_contract_drift()

        self.assertEqual([entry["model"] for entry in drift], ["dcim.site"])

    @unittest.skipUnless(apps.is_installed("netbox_dlm"), "netbox-dlm is not installed")
    def test_an_optional_field_does_not_trip_it(self):
        # netbox-dlm 0.5.0 adds SoftwareVersion.release_designation as a blank
        # CharField. bulk_create fills that in without help, so it must not be
        # treated as drift — otherwise the check would refuse the very upgrade
        # this release exists to allow.
        from forward_netbox.utilities.fast_baseline import _required_field_names
        from netbox_dlm.models import SoftwareVersion

        self.assertNotIn("release_designation", _required_field_names(SoftwareVersion))

    def test_an_uninstalled_optional_model_is_skipped(self):
        from unittest.mock import patch as _patch

        from forward_netbox.utilities import fast_baseline

        contract = dict(fast_baseline.FAST_BASELINE_REQUIRED_FIELD_CONTRACT)
        contract["nonexistent_plugin.thing"] = ("x",)
        with _patch.object(
            fast_baseline, "FAST_BASELINE_REQUIRED_FIELD_CONTRACT", contract
        ):
            self.assertEqual(fast_baseline.fast_baseline_field_contract_drift(), [])
