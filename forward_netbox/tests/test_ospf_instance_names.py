from unittest import TestCase

from forward_netbox.utilities.sync_routing_impl import _other_ospf_instance_names
from forward_netbox.utilities.sync_routing_impl import free_ospf_instance_name
from forward_netbox.utilities.sync_routing_impl import ospf_instance_name


class OspfInstanceNameTest(TestCase):
    def test_the_global_table_keeps_the_name_it_always_had(self):
        self.assertEqual(
            ospf_instance_name("sw-a", "default", None), "sw-a OSPF default"
        )
        self.assertEqual(ospf_instance_name("sw-a", "1", ""), "sw-a OSPF 1")

    def test_the_same_process_in_two_vrfs_gets_two_names(self):
        # netbox-routing 0.5.0 enforces unique (device, name) on OSPF instances.
        names = {
            ospf_instance_name("sw-a", "default", None),
            ospf_instance_name("sw-a", "default", "BLUE"),
            ospf_instance_name("sw-a", "default", "RED"),
        }
        self.assertEqual(len(names), 3)

    def test_a_long_name_is_cut_in_the_base_never_in_the_vrf(self):
        name = ospf_instance_name("d" * 120, "default", "BLUE")
        self.assertEqual(len(name), 100)
        self.assertTrue(name.endswith(" (BLUE)"))
        other = ospf_instance_name("d" * 120, "default", "RED")
        self.assertNotEqual(name, other)


class FreeOspfInstanceNameTest(TestCase):
    def test_a_free_name_is_kept(self):
        self.assertEqual(
            free_ospf_instance_name("sw-a OSPF 1", {"sw-a OSPF 2"}, 1), "sw-a OSPF 1"
        )

    def test_a_name_another_instance_holds_gets_the_process_id(self):
        self.assertEqual(
            free_ospf_instance_name("sw-a OSPF 1", {"sw-a OSPF 1"}, 7),
            "sw-a OSPF 1 #7",
        )

    def test_the_variant_is_stepped_when_it_is_taken_too(self):
        taken = {"sw-a OSPF 1", "sw-a OSPF 1 #7"}
        self.assertEqual(
            free_ospf_instance_name("sw-a OSPF 1", taken, 7), "sw-a OSPF 1 #7-2"
        )

    def test_a_long_name_is_cut_in_the_base_never_in_the_suffix(self):
        desired = "d" * 100
        name = free_ospf_instance_name(desired, {desired}, 12)
        self.assertEqual(len(name), 100)
        self.assertTrue(name.endswith(" #12"))

    def test_the_result_does_not_depend_on_set_order(self):
        taken = {"x", "x #3"}
        self.assertEqual(
            free_ospf_instance_name("x", taken, 3),
            free_ospf_instance_name("x", set(reversed(sorted(taken))), 3),
        )


class _Rows:
    """Just enough of a queryset for `_other_ospf_instance_names`."""

    def __init__(self, rows):
        self.rows = rows

    def filter(self, **lookup):
        def keep(row):
            for key, value in lookup.items():
                if key == "vrf__isnull":
                    if (row["vrf"] is None) != value:
                        return False
                elif row[key] != value:
                    return False
            return True

        return _Rows([row for row in self.rows if keep(row)])

    def exclude(self, pk):
        return _Rows([row for row in self.rows if row["pk"] != pk])

    def values_list(self, field, flat=True):
        return _Rows([row[field] for row in self.rows])

    def first(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class OtherOspfInstanceNamesTest(TestCase):
    def _model(self, rows):
        class Model:
            objects = _Rows(rows)

        return Model

    def test_the_rows_own_instance_is_not_a_clash(self):
        model = self._model(
            [
                {"pk": 1, "device": "d", "vrf": None, "process_id": 1, "name": "a"},
                {"pk": 2, "device": "d", "vrf": "BLUE", "process_id": 1, "name": "b"},
            ]
        )
        self.assertEqual(_other_ospf_instance_names(model, "d", None, 1), {"b"})
        self.assertEqual(_other_ospf_instance_names(model, "d", "BLUE", 1), {"a"})

    def test_a_new_instance_clashes_with_every_name_on_the_device(self):
        model = self._model(
            [
                {"pk": 1, "device": "d", "vrf": None, "process_id": 1, "name": "a"},
                {"pk": 2, "device": "e", "vrf": None, "process_id": 1, "name": "z"},
            ]
        )
        self.assertEqual(_other_ospf_instance_names(model, "d", None, 9), {"a"})


class EnsureOspfInstanceClashTest(TestCase):
    """The name lookup runs only after netbox-routing rejects a clash."""

    def setUp(self):
        from types import SimpleNamespace

        self.device = SimpleNamespace(name="sw-a")

    def _run(self, upsert_side_effect, taken=()):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from unittest.mock import patch

        from forward_netbox.utilities import sync_routing_impl

        model = SimpleNamespace(objects=_Rows(list(taken)))
        runner = Mock()
        runner._optional_model.return_value = model
        runner._model_field_values.side_effect = lambda _model, values: values
        runner._upsert_values_from_defaults.side_effect = upsert_side_effect
        row = {"device": "sw-a", "process_id": "1", "router_id": "10.0.0.1"}
        with (
            patch.object(
                sync_routing_impl, "lookup_device_for_routing", return_value=self.device
            ),
            patch.object(sync_routing_impl, "routing_vrf", return_value=None),
        ):
            result = sync_routing_impl.ensure_ospf_instance(runner, row)
        return runner, result

    def test_a_row_that_clashes_with_nothing_costs_no_lookup(self):
        runner, result = self._run(lambda *a, **k: ("inst", True))

        self.assertEqual(result, "inst")
        self.assertEqual(runner._upsert_values_from_defaults.call_count, 1)

    def test_a_name_clash_is_retried_under_a_free_name(self):
        from django.core.exceptions import ValidationError

        names = []

        def upsert(label, model, *, values, coalesce_sets):
            names.append(values["name"])
            if len(names) == 1:
                raise ValidationError("Name must be unique per device")
            return ("inst", True)

        holder = {
            "pk": 9,
            "device": self.device,
            "vrf": None,
            "process_id": 99,
            "name": "sw-a OSPF 1",
        }
        _, result = self._run(upsert, taken=[holder])

        self.assertEqual(result, "inst")
        self.assertEqual(names, ["sw-a OSPF 1", "sw-a OSPF 1 #1"])

    def test_any_other_validation_error_is_not_swallowed(self):
        from django.core.exceptions import ValidationError

        def upsert(*args, **kwargs):
            raise ValidationError("Router ID is not valid")

        with self.assertRaises(ValidationError):
            self._run(upsert)
