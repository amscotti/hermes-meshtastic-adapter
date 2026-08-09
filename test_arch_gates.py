"""Unit tests for the architecture-gate runner (scripts/check_arch_gates.py).

Pins the exception-isolation contract: a gate that raises is recorded as a
FAIL and the remaining gates still run and are reported, so one broken gate
cannot hide the status of the others.
"""

import contextlib
import io
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import check_arch_gates


def _stub(name, rc=0, raises=False):
    def main(argv=None):
        if raises:
            raise RuntimeError(f"{name} exploded")
        return rc

    return SimpleNamespace(main=main)


class TestExceptionIsolation(unittest.TestCase):
    def test_crashing_gate_does_not_mask_others(self):
        modules = {
            "cc_gate": _stub("cc_gate", raises=True),
            "layer_gate": _stub("layer_gate", rc=0),
            "extraction_gate": _stub("extraction_gate", rc=0),
        }
        buf = io.StringIO()
        with (
            patch(
                "check_arch_gates.importlib.import_module", side_effect=lambda name: modules[name]
            ),
            contextlib.redirect_stdout(buf),
        ):
            rc = check_arch_gates.main()
        out = buf.getvalue()
        self.assertEqual(rc, 1)
        self.assertIn("Complexity: FAIL", out)
        self.assertIn("cc_gate raised: RuntimeError: cc_gate exploded", out)
        # The other gates still ran and were reported.
        self.assertIn("Layering: PASS", out)
        self.assertIn("Extraction: PASS", out)
        self.assertIn("Architecture gates: FAIL (complexity)", out)

    def test_unimportable_gate_does_not_mask_others(self):
        # A gate module that fails to IMPORT (SyntaxError/deleted) must be
        # recorded as FAIL — not crash the whole runner. The import lives
        # inside the isolation boundary (the documented contract).
        stubs = {
            "layer_gate": _stub("layer_gate", rc=0),
            "extraction_gate": _stub("extraction_gate", rc=0),
        }

        def fake_import(name):
            if name == "cc_gate":
                raise SyntaxError("broken gate")
            return stubs[name]

        buf = io.StringIO()
        with (
            patch("check_arch_gates.importlib.import_module", side_effect=fake_import),
            contextlib.redirect_stdout(buf),
        ):
            rc = check_arch_gates.main()
        out = buf.getvalue()
        self.assertEqual(rc, 1)
        self.assertIn("Complexity: FAIL", out)
        self.assertIn("cc_gate raised: SyntaxError", out)
        # The other gates still ran and were reported.
        self.assertIn("Layering: PASS", out)
        self.assertIn("Extraction: PASS", out)
        self.assertIn("Architecture gates: FAIL (complexity)", out)

    def test_all_passing_gates_return_zero(self):
        modules = {name: _stub(name, rc=0) for _, _, name in check_arch_gates.GATES}
        with patch(
            "check_arch_gates.importlib.import_module", side_effect=lambda name: modules[name]
        ):
            self.assertEqual(check_arch_gates.main(), 0)

    def test_runner_registers_named_gates(self):
        keys = [key for key, _, _ in check_arch_gates.GATES]
        self.assertEqual(keys, ["complexity", "layering", "extraction"])


if __name__ == "__main__":
    unittest.main()
