from django.test import SimpleTestCase

from forward_netbox.utilities.query_registry import read_builtin_query_source


class SubinterfaceQueryTest(SimpleTestCase):
    # A device has far more subinterfaces than it has addresses: on the network
    # this was measured on, over a million subinterfaces and 21,343 carrying an
    # address. Only an addressed subinterface needs a NetBox interface, because
    # the address is what has to attach somewhere.
    def test_only_a_subinterface_with_an_address_becomes_an_interface(self):
        source = read_builtin_query_source("forward_interfaces.nqe")
        branch = source.split("subinterface_interfaces(", 1)[1].split("@query", 1)[0]

        self.assertIn(
            "isPresent(subinterface.ipv4) && length(subinterface.ipv4.addresses) > 0",
            branch,
        )
        self.assertIn(
            "isPresent(subinterface.ipv6) && length(subinterface.ipv6.addresses) > 0",
            branch,
        )

    def test_a_firewalls_base_unit_is_not_a_second_interface(self):
        # A subinterface named like its parent is the parent itself; emitting it
        # would collide on (device, name).
        source = read_builtin_query_source("forward_interfaces.nqe")
        branch = source.split("subinterface_interfaces(", 1)[1].split("@query", 1)[0]

        self.assertIn("where subinterface.name != interface.name", branch)

    def test_the_ip_queries_attach_a_subinterface_address_to_the_subinterface(self):
        for filename in (
            "forward_ip_addresses_ipv4.nqe",
            "forward_ip_addresses_ipv6.nqe",
        ):
            source = read_builtin_query_source(filename)
            subinterface_source = source.split("select {", 1)[1].split("};", 1)[0]

            self.assertIn(
                "interface: subinterface.name,", subinterface_source, filename
            )
