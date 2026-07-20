from __future__ import annotations

import ast
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKER_PATHS = (
    REPO_ROOT / "forward_netbox/jobs.py",
    REPO_ROOT / "forward_netbox/models.py",
)
WORKER_UTILITY_EXCLUSIONS = {
    "health.py",
    "health_summary_blocks.py",
}


def _is_named_exception(node, name):
    if isinstance(node, ast.Name):
        return node.id == name
    if isinstance(node, ast.Tuple):
        return any(_is_named_exception(element, name) for element in node.elts)
    return False


def _is_broad_exception(node):
    return node is None or _is_named_exception(node, "Exception")


def _ends_with_bare_raise(handler):
    return (
        bool(handler.body)
        and isinstance(handler.body[-1], ast.Raise)
        and (handler.body[-1].exc is None)
    )


def _has_named_timeout_reraise(handler):
    timeout_names = {
        target.id
        for node in handler.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
        and "JobTimeoutException" in ast.unparse(node.value)
    }
    if not timeout_names:
        return False
    return any(
        isinstance(node, ast.Raise) and node.exc is None for node in ast.walk(handler)
    )


class JobTimeoutBoundaryTest(unittest.TestCase):
    def test_worker_broad_exception_boundaries_propagate_rq_timeout(self):
        utility_paths = sorted(
            path
            for path in (REPO_ROOT / "forward_netbox/utilities").rglob("*.py")
            if path.name not in WORKER_UTILITY_EXCLUSIONS
        )
        failures = []
        for path in (*WORKER_PATHS, *utility_paths):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Try):
                    continue
                timeout_handler_indexes = {
                    index
                    for index, handler in enumerate(node.handlers)
                    if _is_named_exception(handler.type, "JobTimeoutException")
                }
                for index, handler in enumerate(node.handlers):
                    if not _is_broad_exception(handler.type):
                        continue
                    has_prior_timeout_handler = any(
                        timeout_index < index
                        for timeout_index in timeout_handler_indexes
                    )
                    preserves_exception = _ends_with_bare_raise(
                        handler
                    ) or _has_named_timeout_reraise(handler)
                    if not has_prior_timeout_handler and not preserves_exception:
                        failures.append(
                            f"{path.relative_to(REPO_ROOT)}:{handler.lineno}"
                        )

        self.assertEqual(
            failures,
            [],
            "Broad worker exception handlers must re-raise JobTimeoutException "
            "before fallback, isolation, or exception translation.",
        )


if __name__ == "__main__":
    unittest.main()
