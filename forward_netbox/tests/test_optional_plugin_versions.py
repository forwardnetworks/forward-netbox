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
