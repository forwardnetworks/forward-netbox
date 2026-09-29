"""The fast paths run on any subset of the validated plugins.

The runtime rule used to be exact equality with every validated plugin, so a
customer without the ACI and Peering Manager plugins - neither of which they
use - lost the fast baseline, COPY/SQL and the set-based merge, and every
first sync took hours. An absent optional plugin contributes no tables,
signal receivers or triggers these paths could meet; an EXTRA plugin can, so
that still refuses, as does an installed optional plugin at a version nobody
validated. The subset rule is only safe because the fast baseline no longer
assumes every optional plugin is importable, which these tests also pin.
"""

from unittest.mock import patch

from django.test import SimpleTestCase

from forward_netbox.utilities.validated_runtime import OPTIONAL_PLUGIN_APP_DISTRIBUTIONS
from forward_netbox.utilities.validated_runtime import plugin_runtime_mismatch
from forward_netbox.utilities.validated_runtime import REQUIRED_PLUGIN_APPS
from forward_netbox.utilities.validated_runtime import VALIDATED_OPTIONAL_DISTRIBUTIONS
from forward_netbox.utilities.validated_runtime import VALIDATED_PLUGIN_APPS


def _validated_versions(apps):
    return {
        OPTIONAL_PLUGIN_APP_DISTRIBUTIONS[app]: sorted(
            VALIDATED_OPTIONAL_DISTRIBUTIONS[OPTIONAL_PLUGIN_APP_DISTRIBUTIONS[app]]
        )[-1]
        for app in apps
        if app in OPTIONAL_PLUGIN_APP_DISTRIBUTIONS
    }


class PluginRuntimeRuleTest(SimpleTestCase):
    def test_every_optional_plugin_absent_is_supported(self):
        self.assertIsNone(plugin_runtime_mismatch(REQUIRED_PLUGIN_APPS, {}))

    def test_the_customer_runtime_is_supported(self):
        # No ACI, no Peering Manager; routing, DLM and Validity at validated
        # versions.
        apps = VALIDATED_PLUGIN_APPS - {"netbox_cisco_aci", "netbox_peering_manager"}
        self.assertIsNone(plugin_runtime_mismatch(apps, _validated_versions(apps)))

    def test_an_absent_plugin_version_is_not_judged(self):
        apps = REQUIRED_PLUGIN_APPS | {"netbox_dlm"}
        versions = {**_validated_versions(apps), "netbox-cisco-aci": None}
        self.assertIsNone(plugin_runtime_mismatch(apps, versions))

    def test_an_extra_plugin_still_refuses(self):
        reason, detail = plugin_runtime_mismatch(
            VALIDATED_PLUGIN_APPS | {"stranger"},
            _validated_versions(VALIDATED_PLUGIN_APPS),
        )
        self.assertEqual(reason, "unsupported_plugin_app_tuple")
        self.assertEqual(detail["unexpected"], ["stranger"])

    def test_a_missing_required_plugin_refuses(self):
        reason, detail = plugin_runtime_mismatch({"forward_netbox"}, {})
        self.assertEqual(reason, "unsupported_plugin_app_tuple")
        self.assertEqual(detail["missing_required"], ["netbox_branching"])

    def test_an_installed_optional_plugin_at_an_unvalidated_version_refuses(self):
        apps = REQUIRED_PLUGIN_APPS | {"netbox_dlm"}
        reason, detail = plugin_runtime_mismatch(apps, {"netbox-dlm": "9.9.9"})
        self.assertEqual(reason, "unsupported_optional_plugin_version")
        self.assertEqual(detail["distribution"], "netbox-dlm")

    def test_an_installed_optional_plugin_with_unreadable_metadata_refuses(self):
        apps = REQUIRED_PLUGIN_APPS | {"netbox_routing"}
        reason, _detail = plugin_runtime_mismatch(apps, {"netbox-routing": None})
        self.assertEqual(reason, "unsupported_optional_plugin_version")

    def test_every_validated_optional_app_maps_to_a_validated_distribution(self):
        # Every validated optional app has a distribution and a validated
        # version set. The map may hold MORE: an optional plugin this runtime
        # cannot install (ACI on NetBox 4.7) is known but deliberately not
        # validated, and installing it is refused as an unexpected app.
        validated_optional = VALIDATED_PLUGIN_APPS - REQUIRED_PLUGIN_APPS
        self.assertLessEqual(validated_optional, set(OPTIONAL_PLUGIN_APP_DISTRIBUTIONS))
        for app in validated_optional:
            self.assertIn(
                OPTIONAL_PLUGIN_APP_DISTRIBUTIONS[app], VALIDATED_OPTIONAL_DISTRIBUTIONS
            )

    def test_an_installed_but_unvalidated_optional_plugin_is_refused_not_raised(self):
        reason, detail = plugin_runtime_mismatch(
            REQUIRED_PLUGIN_APPS | {"netbox_cisco_aci"}, {}
        )
        self.assertEqual(reason, "unsupported_plugin_app_tuple")
        self.assertEqual(detail["unexpected"], ["netbox_cisco_aci"])


class GatesUseTheRuleTest(SimpleTestCase):
    """Each gate enables on a subset runtime and refuses an extra plugin."""

    def _gates(self, plugins):
        from forward_netbox.utilities.apply_engine_decision import (
            _copy_sql_runtime_supported,
        )
        from forward_netbox.utilities.merge_set_based import _runtime_tuple_decision

        with patch("django.conf.settings.PLUGINS", sorted(plugins)):
            copy_ok, copy_reason, _ = _copy_sql_runtime_supported()
            merge = _runtime_tuple_decision()
        return (copy_ok, copy_reason), (merge.enabled, merge.reason_code)

    def test_a_subset_runtime_enables_copy_sql_and_the_set_based_merge(self):
        subset = VALIDATED_PLUGIN_APPS - {"netbox_cisco_aci", "netbox_peering_manager"}
        (copy_ok, _), (merge_ok, _) = self._gates(subset)
        self.assertTrue(copy_ok)
        self.assertTrue(merge_ok)

    def test_an_extra_plugin_refuses_both(self):
        (copy_ok, copy_reason), (merge_ok, merge_reason) = self._gates(
            VALIDATED_PLUGIN_APPS | {"stranger"}
        )
        self.assertFalse(copy_ok)
        self.assertEqual(copy_reason, "unsupported_plugin_app_tuple")
        self.assertFalse(merge_ok)
        self.assertEqual(merge_reason, "unsupported_plugin_app_tuple")

    def test_the_fast_baseline_decision_enables_on_a_subset_runtime(self):
        from forward_netbox.utilities.fast_baseline import _runtime_decision

        subset = VALIDATED_PLUGIN_APPS - {"netbox_cisco_aci", "netbox_peering_manager"}
        with patch("django.conf.settings.PLUGINS", sorted(subset)):
            decision = _runtime_decision()
        self.assertTrue(decision.enabled, decision.reason_code)


class AbsentPluginHazardsTest(SimpleTestCase):
    def test_an_uninstalled_model_resolves_to_none_not_lookuperror(self):
        from forward_netbox.utilities.fast_baseline import _installed_model
        from forward_netbox.utilities.fast_baseline import _target_models

        self.assertIsNone(_installed_model("no_such_plugin", "Thing"))
        self.assertEqual(
            _target_models(["no_such_plugin.thing"]), (None, "no_such_plugin.thing")
        )

    def test_side_models_skip_an_uninstalled_app(self):
        from forward_netbox.utilities import fast_baseline

        real = fast_baseline.apps.get_model

        def without_dcim_modules(app_label, model_name=None):
            if model_name == "ModuleType":
                raise LookupError(model_name)
            return real(app_label, model_name)

        with patch.object(
            fast_baseline.apps, "get_model", side_effect=without_dcim_modules
        ):
            models = fast_baseline._side_models({"dcim.module"})
        self.assertNotIn("ModuleType", [model.__name__ for model in models])

    def test_no_routing_rows_needs_no_routing_plugin(self):
        import builtins

        from forward_netbox.utilities.fast_baseline_models import (
            _adapter_workload_contract,
        )

        real_import = builtins.__import__

        def no_routing(name, *args, **kwargs):
            if name.startswith("netbox_routing"):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=no_routing):
            ok, _context = _adapter_workload_contract({})
        self.assertTrue(ok)
