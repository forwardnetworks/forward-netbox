# Six identical `(ForwardDependencySkipError)` rows told a customer nothing
# about which parent was missing. The vocabulary existed the whole time - it
# lives in `record_aggregated_skip_warning` - and never reached the database,
# because `failure_reason` returns "" for this exception so `detail` was just
# the class name.
#
# 16 of 24 raisers named nothing. Listing them by hand is what let them drift
# apart in the first place, so this reads the raisers out of the source: adding
# one without a dependency fails here rather than in a customer's issue list.
import ast
import pathlib

from django.test import SimpleTestCase

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _skip_raisers():
    for path in sorted(PLUGIN_ROOT.rglob("*.py")):
        if "tests" in path.parts or "migrations" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or not isinstance(node.exc, ast.Call):
                continue
            name = getattr(node.exc.func, "id", None) or getattr(
                node.exc.func, "attr", None
            )
            if name == "ForwardDependencySkipError":
                yield path, node


class SkipRaisersNameTheirDependencyTest(SimpleTestCase):
    def test_every_raiser_names_the_model_it_is_waiting_on(self):
        unnamed = [
            f"{path.relative_to(PLUGIN_ROOT)}:{node.lineno}"
            for path, node in _skip_raisers()
            if "dependency" not in {kw.arg for kw in node.exc.keywords}
        ]
        self.assertEqual(
            unnamed,
            [],
            "dependency skips that record only the exception class: "
            f"{unnamed}. Pass dependency='<app.model>' so the issue row says "
            "what the row was waiting on.",
        )

    def test_the_dependency_matches_the_guard_that_admitted_the_raise(self):
        # A wrong slug is worse than none: it names a model that is not the one
        # holding the row up. Where the raise sits under
        # `if runner._dependency_failed("app.model", key)`, that literal IS the
        # answer, so the two must agree.
        mismatches = []
        for path in sorted(PLUGIN_ROOT.rglob("*.py")):
            if "tests" in path.parts or "migrations" in path.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.If):
                    continue
                test = node.test
                if not (
                    isinstance(test, ast.Call)
                    and getattr(test.func, "attr", None) == "_dependency_failed"
                    and test.args
                    and isinstance(test.args[0], ast.Constant)
                ):
                    continue
                guard = test.args[0].value
                for child in ast.walk(node):
                    if not (
                        isinstance(child, ast.Raise) and isinstance(child.exc, ast.Call)
                    ):
                        continue
                    name = getattr(child.exc.func, "id", None) or getattr(
                        child.exc.func, "attr", None
                    )
                    if name != "ForwardDependencySkipError":
                        continue
                    declared = next(
                        (
                            kw.value.value
                            for kw in child.exc.keywords
                            if kw.arg == "dependency"
                            and isinstance(kw.value, ast.Constant)
                        ),
                        None,
                    )
                    if declared != guard:
                        mismatches.append(
                            f"{path.relative_to(PLUGIN_ROOT)}:{child.lineno} "
                            f"guard={guard!r} dependency={declared!r}"
                        )
        self.assertEqual(
            mismatches, [], f"dependency disagrees with guard: {mismatches}"
        )

    def test_there_are_still_raisers_to_check(self):
        # Guards the two tests above against silently passing on an empty set,
        # which is how a structural check quietly stops testing anything.
        self.assertGreater(len(list(_skip_raisers())), 15)
