from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "validate_sbom", Path(__file__).resolve().parents[1] / "validate_sbom.py"
)
validate_sbom = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(validate_sbom)


class ValidateSbomTest(unittest.TestCase):
    def _write(self, components):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "sbom.json"
        path.write_text(
            json.dumps({"bomFormat": "CycloneDX", "components": components}),
            encoding="utf-8",
        )
        return path

    def _components(self):
        required = dict(validate_sbom.REQUIRED_COMPONENTS)
        required["forward-netbox"] = "2.6.0"
        components = [
            {"name": name, "version": version} for name, version in required.items()
        ]
        components.extend(
            {"name": f"transitive-{index}", "version": "1.0"} for index in range(20)
        )
        return components

    def test_accepts_exact_installed_runtime_components(self):
        result = validate_sbom.validate_sbom(
            self._write(self._components()),
            "2.6.0",
        )

        self.assertGreaterEqual(result["component_count"], 20)
        self.assertEqual(result["netbox_version"], "4.6.5")

    def test_rejects_runtime_version_mismatch(self):
        components = self._components()
        for component in components:
            if component["name"] == "netbox":
                component["version"] = "4.6.4"

        with self.assertRaisesRegex(ValueError, "required-component mismatch"):
            validate_sbom.validate_sbom(self._write(components), "2.6.0")


if __name__ == "__main__":
    unittest.main()
