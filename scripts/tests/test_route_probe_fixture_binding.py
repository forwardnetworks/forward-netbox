# The artifact route probe binds each registered detail route to a fixture pk
# by matching the route's short name against the fixture model names. It used
# to take the FIRST match, and `forwardingestionissue` starts with
# `forwardingestion` - so the issue's routes were requested with the
# ingestion's pk, the object did not exist, and the probe failed the release
# with `installed detail route ... returned HTTP 404`. It reads as a broken
# route and is really a mis-addressed request.
#
# The probe module calls `django.setup()` against `/opt/netbox` at import, so
# it can only be imported inside the container. The rule is compiled out of
# the shipped source instead, which keeps the test on the real function rather
# than a copy of it.
import ast
import unittest
from pathlib import Path

PROBE = Path(__file__).resolve().parents[1] / "validate_installed_routes.py"


def _fixture_model_for():
    tree = ast.parse(PROBE.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "fixture_model_for":
            namespace = {}
            exec(compile(ast.Module([node], []), str(PROBE), "exec"), namespace)
            return namespace["fixture_model_for"]
    raise AssertionError("validate_installed_routes.fixture_model_for is gone")


FIXTURES = {
    "forwardsource": 1,
    "forwardsync": 2,
    "forwardingestion": 3,
    "forwardingestionissue": 4,
    "forwardvalidationrun": 5,
    "forwarddeviceanalysis": 6,
    "forwardnqemap": 7,
    "forwarddriftpolicy": 8,
}


class RouteProbeFixtureBindingTest(unittest.TestCase):
    """`invoke harness-test` runs `unittest discover`, so these are TestCases."""

    def setUp(self):
        self.fixture_model_for = _fixture_model_for()

    def test_the_longer_model_name_wins_its_own_routes(self):
        for route in (
            "forwardingestionissue",
            "forwardingestionissue_list",
            "forwardingestionissue_delete",
        ):
            with self.subTest(route=route):
                self.assertEqual(
                    self.fixture_model_for(route, FIXTURES),
                    "forwardingestionissue",
                )

    def test_the_shorter_model_keeps_its_own_routes(self):
        for route in ("forwardingestion", "forwardingestion_delete"):
            with self.subTest(route=route):
                self.assertEqual(
                    self.fixture_model_for(route, FIXTURES), "forwardingestion"
                )

    def test_an_unknown_route_binds_to_nothing(self):
        self.assertIsNone(self.fixture_model_for("dcim_device", FIXTURES))

    def test_every_fixture_name_resolves_to_itself(self):
        # A new model whose name is a prefix of another must not silently take
        # the other's pk; this fails the moment a fixture is added that cannot
        # win its own exact route name.
        for name in FIXTURES:
            with self.subTest(model=name):
                self.assertEqual(self.fixture_model_for(name, FIXTURES), name)
