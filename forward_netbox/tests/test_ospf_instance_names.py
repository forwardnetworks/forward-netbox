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
