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
from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.apply_engine_decision import (
    COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.merge_set_based import (
    SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
)
from forward_netbox.utilities.version_series import series_matches


class OptionalDistributionVersionSetTest(SimpleTestCase):
    def test_every_gate_accepts_both_validated_dlm_versions(self):
        for name, supported in (
            ("copy_sql", COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS),
            ("set_based", SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS),
        ):
            with self.subTest(gate=name):
                self.assertIn("0.4.1", supported["netbox-dlm"])
                self.assertIn("0.5.0", supported["netbox-dlm"])

    def test_an_unvalidated_version_is_still_refused(self):
        # The gates fail closed; widening must not become "any version".
        for supported in (
            COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
            SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
        ):
            self.assertNotIn("0.6.0", supported["netbox-dlm"])
            self.assertNotIn("0.3.3", supported["netbox-dlm"])

    def test_the_other_distributions_stay_single_valued(self):
        # Only netbox-dlm has a second validated version so far.
        for supported in (
            COPY_SQL_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
            SET_BASED_MERGE_SUPPORTED_OPTIONAL_DISTRIBUTIONS,
        ):
            for name in (
                "netbox-cisco-aci",
                "netbox-peering-manager",
                "netbox-routing",
            ):
                self.assertEqual(len(supported[name]), 1, name)


class FastBaselineRuntimeTupleTest(SimpleTestCase):
    def test_any_patch_in_the_series_is_accepted(self):
        # The policy: 4.6.x and 1.1.x, not an exact pin. A patch release must
        # not silently switch an engine off.
        for version in ("4.6.5", "4.6.6", "4.6.12", "4.6"):
            self.assertTrue(series_matches(version, "4.6"), version)
        for version in ("1.1.1", "1.1.2", "1.1.9"):
            self.assertTrue(series_matches(version, "1.1"), version)

    def test_a_different_series_is_still_refused(self):
        # Permissive within a series is not permissive across one.
        for version in ("4.7.0", "4.5.9", "5.0.0", "", None, "4.60.1"):
            self.assertFalse(series_matches(version, "4.6"), version)
        for version in ("1.2.0", "1.0.9", "2.0.0"):
            self.assertFalse(series_matches(version, "1.1"), version)

    def _decide(self, dlm_version):
        from forward_netbox.utilities import fast_baseline

        tuple_ = {
            "netbox": "4.6.5",
            "branching": "1.1.1",
            "forward_netbox": fast_baseline.forward_config.version,
            "optional_plugins": {
                "netbox-cisco-aci": "0.4.0",
                "netbox-dlm": dlm_version,
                "netbox-peering-manager": "0.3.0",
                "netbox-routing": "0.4.3",
            },
            "plugin_apps": sorted(
                {
                    "forward_netbox",
                    "netbox_branching",
                    "netbox_cisco_aci",
                    "netbox_dlm",
                    "netbox_peering_manager",
                    "netbox_routing",
                }
            ),
        }
        with patch.object(
            fast_baseline, "fast_baseline_runtime_tuple", return_value=tuple_
        ):
            return fast_baseline._runtime_decision()

    def test_a_later_netbox_patch_keeps_the_fast_baseline(self):
        from forward_netbox.utilities import fast_baseline

        tuple_ = {
            "netbox": "4.6.9",
            "branching": "1.1.2",
            "forward_netbox": fast_baseline.forward_config.version,
            "optional_plugins": {
                "netbox-cisco-aci": "0.4.0",
                "netbox-dlm": "0.5.0",
                "netbox-peering-manager": "0.3.0",
                "netbox-routing": "0.4.3",
            },
            "plugin_apps": sorted(
                {
                    "forward_netbox",
                    "netbox_branching",
                    "netbox_cisco_aci",
                    "netbox_dlm",
                    "netbox_peering_manager",
                    "netbox_routing",
                }
            ),
        }
        with patch.object(
            fast_baseline, "fast_baseline_runtime_tuple", return_value=tuple_
        ):
            self.assertNotEqual(
                fast_baseline._runtime_decision().reason_code,
                "unsupported_runtime_tuple",
            )

    def test_the_previously_pinned_version_is_still_supported(self):
        self.assertNotEqual(
            self._decide("0.4.1").reason_code, "unsupported_runtime_tuple"
        )

    def test_the_upgraded_version_no_longer_disables_the_fast_baseline(self):
        # The customer-visible symptom: upgrading netbox-dlm made a first sync
        # fall back to the slow path with nothing explaining why.
        self.assertNotEqual(
            self._decide("0.5.0").reason_code, "unsupported_runtime_tuple"
        )

    def test_an_unvalidated_version_still_fails_closed(self):
        decision = self._decide("0.6.0")

        self.assertFalse(decision.enabled)
        self.assertEqual(decision.reason_code, "unsupported_runtime_tuple")

    def test_the_rejection_detail_stays_serialisable(self):
        # `expected` holds sets now; it is persisted as job evidence, so it must
        # render as sorted lists rather than blow up on JSON encoding.
        import json

        detail = self._decide("0.6.0").context

        json.dumps(detail)
        self.assertEqual(
            detail["expected"]["optional_plugins"]["netbox-dlm"], ["0.4.1", "0.5.0"]
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
