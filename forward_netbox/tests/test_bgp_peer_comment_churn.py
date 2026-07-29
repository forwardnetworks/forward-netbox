"""A moved BGP counter must not rewrite a peer whose configuration is unchanged.

`bgp_peer_comments` rendered `session_state`, `advertised_prefixes` and
`received_prefixes` — values that move between snapshots on a healthy, entirely
unchanged session. Because `comments` drives change detection, every sync
rewrote every peer: 360 UPDATEs, 360 ObjectChanges and 360 branch changes to
stage and merge, on every run, for peers whose configuration never changed.
"""

from django.test import SimpleTestCase

from forward_netbox.utilities.sync_routing_impl import bgp_peer_comments

VOLATILE_FIELDS = ("session_state", "advertised_prefixes", "received_prefixes")


def _row(**overrides):
    row = {
        "router_id": "10.0.0.1",
        "peer_type": "EBGP",
        "peer_device": "edge-2",
        "peer_vrf": "default",
        "peer_router_id": "10.0.0.2",
        "session_state": "ESTABLISHED",
        "advertised_prefixes": 1423,
        "received_prefixes": 87,
    }
    row.update(overrides)
    return row


class BGPPeerCommentChurnTest(SimpleTestCase):
    def test_only_the_counters_moving_produces_identical_comments(self):
        # The exact churn case: a healthy session whose prefix counts drifted.
        before = bgp_peer_comments(_row())
        after = bgp_peer_comments(
            _row(advertised_prefixes=1477, received_prefixes=91)
        )

        self.assertEqual(before, after)

    def test_no_volatile_counter_is_rendered(self):
        rendered = bgp_peer_comments(_row())
        for value in ("ESTABLISHED", "1423", "87"):
            self.assertNotIn(value, rendered)

    def test_a_session_state_change_alone_does_not_rewrite_the_peer(self):
        self.assertEqual(
            bgp_peer_comments(_row()),
            bgp_peer_comments(_row(session_state="IDLE")),
        )

    def test_stable_descriptive_state_is_still_reported(self):
        rendered = bgp_peer_comments(_row())
        for expected in (
            "Router ID: 10.0.0.1",
            "Peer type: EBGP",
            "Peer device: edge-2",
            "Peer VRF: default",
            "Peer router ID: 10.0.0.2",
        ):
            self.assertIn(expected, rendered)

    def test_a_real_configuration_change_still_changes_the_comments(self):
        # The guard must not have made comments inert.
        self.assertNotEqual(
            bgp_peer_comments(_row()),
            bgp_peer_comments(_row(peer_vrf="mgmt")),
        )

    def test_absent_values_are_omitted_rather_than_rendered_empty(self):
        rendered = bgp_peer_comments(
            {"router_id": "10.0.0.1", "peer_type": None, "peer_device": ""}
        )
        self.assertIn("Router ID: 10.0.0.1", rendered)
        self.assertNotIn("Peer type", rendered)
        self.assertNotIn("Peer device", rendered)
