"""Unit tests for the layering gate (scripts/layer_gate.py).

Pins the import-attribution rules: only relative ``from . import x`` aliases
enter the module's own namespace, so ``from gateway import adapter`` must not
be misattributed to this repo, and the mid-tier set now includes transport.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import layer_gate


def _write_module(root: Path, name: str, source: str) -> Path:
    path = root / name
    path.write_text(source, encoding="utf-8")
    return path


class TestImportAttribution(unittest.TestCase):
    def _imports(self, source, name="mid.py"):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_module(Path(tmp), name, source)
            return layer_gate._imported_repo_modules(path)

    def test_absolute_import_alias_is_not_attributed(self):
        # `from gateway import adapter` imports Hermes' adapter, not ours; the
        # alias must not satisfy the adapter-import check.
        imports = self._imports("from gateway import adapter\n")
        self.assertNotIn("adapter", imports)

    def test_relative_import_adapter_is_attributed(self):
        imports = self._imports("from . import adapter\n")
        self.assertIn("adapter", imports)

    def test_relative_dotted_import_adapter_is_attributed(self):
        imports = self._imports("from .adapter import MeshtasticAdapter\n")
        self.assertIn("adapter", imports)

    def test_plain_import_adapter_is_attributed(self):
        imports = self._imports("import adapter\n")
        self.assertIn("adapter", imports)

    def test_dual_import_pattern_is_attributed(self):
        imports = self._imports(
            "try:\n    from . import transport\nexcept ImportError:\n    import transport\n"
        )
        self.assertIn("transport", imports)


class TestTransportNoAdapterImport(unittest.TestCase):
    """transport is a non-hub sibling now: it may not import adapter."""

    def test_transport_importing_adapter_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _write_module(root, "adapter.py", "x = 1\n")
            transport = _write_module(root, "transport.py", "import adapter\n")
            with patch.object(layer_gate, "_MODULES", [adapter, transport]):
                self.assertEqual(layer_gate.main(), 1)

    def test_transport_not_importing_adapter_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _write_module(root, "adapter.py", "x = 1\n")
            transport = _write_module(root, "transport.py", "import os\n")
            with patch.object(layer_gate, "_MODULES", [adapter, transport]):
                self.assertEqual(layer_gate.main(), 0)

    def test_unparseable_module_fails_cleanly(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            adapter = _write_module(root, "adapter.py", "x = 1\n")
            broken = _write_module(root, "transport.py", "def broken(:\n")
            with patch.object(layer_gate, "_MODULES", [adapter, broken]):
                self.assertEqual(layer_gate.main(), 1)


if __name__ == "__main__":
    unittest.main()
