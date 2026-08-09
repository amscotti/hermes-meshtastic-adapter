"""Unit tests for the extraction gate (scripts/extraction_gate.py).

Pins the malformed-pyproject behavior (clean FAIL, not a crash) and the
stray-root-file forcing behavior (a scratch ``*.py`` is treated as a module
and must be registered + tested or the gate FAILs).
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import extraction_gate


class TestCoverageSource(unittest.TestCase):
    def test_missing_coverage_key_returns_empty(self):
        # A malformed/renamed pyproject key must yield [] (clean FAIL later),
        # never a KeyError traceback.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pyproject.toml").write_text("[tool]\nother = 1\n", encoding="utf-8")
            with patch.object(extraction_gate, "REPO_ROOT", root):
                self.assertEqual(extraction_gate._coverage_source(), [])

    def test_missing_pyproject_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(extraction_gate, "REPO_ROOT", Path(tmp)):
                self.assertEqual(extraction_gate._coverage_source(), [])

    def test_non_dict_intermediate_returns_empty(self):
        # Valid TOML but a wrong shape — an intermediate key is a scalar
        # string (e.g. `[tool]\ncoverage = "not-a-table"`), so the chained
        # `.get` must not crash with AttributeError; clean [] downstream.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "pyproject.toml").write_text(
                '[tool]\ncoverage = "not-a-table"\n', encoding="utf-8"
            )
            with patch.object(extraction_gate, "REPO_ROOT", root):
                self.assertEqual(extraction_gate._coverage_source(), [])


class TestMain(unittest.TestCase):
    def test_stray_scratch_py_fails_cleanly(self):
        # A stray root *.py is assumed to be a module: it must be in the
        # coverage source and have a test file, or the gate FAILs (not crashes).
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scratch.py").write_text("x = 1\n", encoding="utf-8")
            (root / "pyproject.toml").write_text(
                "[tool.coverage.run]\nsource = []\n", encoding="utf-8"
            )
            with patch.object(extraction_gate, "REPO_ROOT", root):
                self.assertEqual(extraction_gate.main(), 1)

    def test_registered_and_tested_module_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "widget.py").write_text("x = 1\n", encoding="utf-8")
            (root / "test_widget.py").write_text("import unittest\n", encoding="utf-8")
            (root / "pyproject.toml").write_text(
                '[tool.coverage.run]\nsource = ["widget"]\n', encoding="utf-8"
            )
            with patch.object(extraction_gate, "REPO_ROOT", root):
                self.assertEqual(extraction_gate.main(), 0)

    def test_module_without_coverage_entry_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "widget.py").write_text("x = 1\n", encoding="utf-8")
            (root / "test_widget.py").write_text("import unittest\n", encoding="utf-8")
            (root / "pyproject.toml").write_text(
                "[tool.coverage.run]\nsource = []\n", encoding="utf-8"
            )
            with patch.object(extraction_gate, "REPO_ROOT", root):
                self.assertEqual(extraction_gate.main(), 1)

    def test_legacy_module_gaining_own_test_file_fails_until_shrink_acked(self):
        """A grandfathered module that gains test_<module>.py must FAIL until
        it is removed from LEGACY_COVERAGE_BY_INTEGRATION (shrink acknowledged).
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "schemas.py").write_text("x = 1\n", encoding="utf-8")
            (root / "test_schemas.py").write_text("import unittest\n", encoding="utf-8")
            (root / "pyproject.toml").write_text(
                '[tool.coverage.run]\nsource = ["schemas"]\n', encoding="utf-8"
            )
            with (
                patch.object(extraction_gate, "REPO_ROOT", root),
                patch.object(
                    extraction_gate,
                    "LEGACY_COVERAGE_BY_INTEGRATION",
                    {"schemas"},
                ),
            ):
                self.assertEqual(extraction_gate.main(), 1)


if __name__ == "__main__":
    unittest.main()
