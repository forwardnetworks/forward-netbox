from __future__ import annotations

import ast
import unittest
from pathlib import Path

DOC_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs/02_Reference/apply-engine-model-matrix.md"
)
APPLY_ENGINE_PATH = (
    Path(__file__).resolve().parents[2]
    / "forward_netbox/utilities/apply_engine_decision.py"
)


class ApplyEngineModelMatrixDocTest(unittest.TestCase):
    def _read_apply_engine_constants(self):
        source = APPLY_ENGINE_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        constants: dict[str, object] = {}

        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            name = node.targets[0].id
            if name not in {
                "BULK_ORM_ENABLED_MODELS",
                "ADAPTER_MODEL_BLOCKERS",
            }:
                continue
            constants[name] = ast.literal_eval(node.value)

        constants["ADAPTER_REQUIRED_MODELS"] = set(constants["ADAPTER_MODEL_BLOCKERS"])
        return constants

    def _parse_doc_sets(self):
        safe_models: set[str] = set()
        adapter_models: dict[str, str] = {}
        section: str | None = None

        for raw_line in DOC_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if line == "## Bulk ORM Safe Set":
                section = "safe"
                continue
            if line == "## Adapter-Required Set":
                section = "adapter"
                continue
            if line.startswith("## "):
                section = None
                continue
            if not line.startswith("- `"):
                continue
            if section == "adapter" and "` | `" in line:
                # Adapter-required line: `model` | `blocker`
                model_part, blocker_part = line.split("` | `", maxsplit=1)
                model = model_part.replace("- `", "", 1).strip()
                blocker = blocker_part.rsplit("`", maxsplit=1)[0].strip()
                adapter_models[model] = blocker
                continue
            if section == "safe":
                model = line.strip("- ").strip("`")
                safe_models.add(model)

        return safe_models, adapter_models

    def test_doc_matches_bulk_orm_enabled_models(self):
        constants = self._read_apply_engine_constants()
        safe_models, _adapter_models = self._parse_doc_sets()
        self.assertEqual(safe_models, set(constants["BULK_ORM_ENABLED_MODELS"]))

    def test_doc_matches_adapter_required_models_and_blockers(self):
        constants = self._read_apply_engine_constants()
        _safe_models, adapter_models = self._parse_doc_sets()
        self.assertEqual(set(adapter_models), set(constants["ADAPTER_REQUIRED_MODELS"]))
        expected_blockers = {
            model_string: blocker["blocker_code"]
            for model_string, blocker in constants["ADAPTER_MODEL_BLOCKERS"].items()
        }
        self.assertEqual(adapter_models, expected_blockers)


if __name__ == "__main__":
    unittest.main()
