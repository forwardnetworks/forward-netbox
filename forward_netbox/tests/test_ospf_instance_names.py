from unittest import TestCase

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
