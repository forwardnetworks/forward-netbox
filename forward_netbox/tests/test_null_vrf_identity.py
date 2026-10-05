from unittest import TestCase

from forward_netbox.exceptions import ForwardQueryError
from forward_netbox.utilities.sync_contracts import MODEL_SYNC_CONTRACTS
from forward_netbox.utilities.sync_contracts import row_coalesce_field_is_complete
from forward_netbox.utilities.sync_contracts import validate_row_shape_for_model

# Models whose bundled query emits a null `vrf` for the global routing table
# (`vrf: ... else null`) or for a route outside any VRF. Their default coalesce
# set names `vrf`, so a null `vrf` must still identify the row: a global route
# is a real, ordinary row, not an incomplete one.
GLOBAL_TABLE_MODELS = (
    "netbox_routing.staticroute",
    "netbox_routing.bgppeer",
    "netbox_routing.bgpaddressfamily",
    "netbox_routing.bgppeeraddressfamily",
    "netbox_routing.ospfinstance",
    "netbox_peering_manager.peeringsession",
)


def _identity_row(model_string):
    contract = MODEL_SYNC_CONTRACTS[model_string]
    row = {field: "x" for field in contract.required_fields}
    for field_set in contract.default_coalesce_fields:
        for field in field_set:
            row[field] = "x"
    row["vrf"] = None
    return row


class NullVrfIdentityTest(TestCase):
    def test_a_global_row_satisfies_the_default_coalesce_set(self):
        for model_string in GLOBAL_TABLE_MODELS:
            with self.subTest(model=model_string):
                contract = MODEL_SYNC_CONTRACTS[model_string]
                sets = [list(s) for s in contract.default_coalesce_fields]
                try:
                    validate_row_shape_for_model(
                        model_string, _identity_row(model_string), sets
                    )
                except ForwardQueryError as exc:
                    self.fail(f"{model_string} rejects a global-table row: {exc}")

    def test_the_static_route_row_shape_the_bundled_query_emits_is_accepted(self):
        validate_row_shape_for_model(
            "netbox_routing.staticroute",
            {
                "device": "sw-a",
                "os": "OS.NXOS",
                "vrf": None,
                "family": "ip",
                "args": "0.0.0.0/0 10.0.0.1",
            },
            [["device", "vrf", "family", "args"]],
        )

    def test_a_missing_vrf_key_is_still_rejected(self):
        row = _identity_row("netbox_routing.staticroute")
        del row["vrf"]
        with self.assertRaises(ForwardQueryError):
            validate_row_shape_for_model(
                "netbox_routing.staticroute",
                row,
                [["device", "vrf", "family", "args"]],
            )

    def test_other_identity_fields_stay_required(self):
        for field in ("device", "family", "args"):
            with self.subTest(field=field):
                row = _identity_row("netbox_routing.staticroute")
                row[field] = None
                with self.assertRaises(ForwardQueryError):
                    validate_row_shape_for_model(
                        "netbox_routing.staticroute",
                        row,
                        [["device", "vrf", "family", "args"]],
                    )

    def test_a_null_vrf_is_complete_only_for_models_that_have_a_global_table(self):
        self.assertFalse(
            row_coalesce_field_is_complete("dcim.device", {"vrf": None}, "vrf")
        )
        self.assertTrue(
            row_coalesce_field_is_complete(
                "netbox_routing.staticroute", {"vrf": None}, "vrf"
            )
        )


class RowShapeFailureIsNamedTest(TestCase):
    def test_a_row_shape_refusal_is_named_not_unrecognized(self):
        from forward_netbox.utilities.diagnostics import failure_reason

        identity = ForwardQueryError(
            "Row for `netbox_routing.staticroute` does not satisfy any configured "
            "coalesce field set."
        )
        required = ForwardQueryError(
            "Row for `netbox_routing.staticroute` is missing required fields: args."
        )
        self.assertEqual(failure_reason(identity), "row-identity-incomplete")
        self.assertIn("shape-error", failure_reason(required))


class RoutingAdapterReadinessTest(TestCase):
    def test_every_routing_model_reports_its_adapters_present(self):
        from forward_netbox.utilities.plugin_integrations.registry import (
            integration_adapter_contract_summary,
        )

        summary = integration_adapter_contract_summary()
        routing = next(
            value for key, value in summary.items() if key.startswith("routing")
        )
        self.assertEqual(routing["gaps"], [], routing["gaps"])
        models = {entry["model"] for entry in routing["models"]}
        self.assertIn("netbox_routing.staticroute", models)
        self.assertIn("netbox_routing.routemapentry", models)
