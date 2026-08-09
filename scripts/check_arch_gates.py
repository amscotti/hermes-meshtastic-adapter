#!/usr/bin/env python3
"""Combined architecture-gate runner (complexity, layering, extraction).

Run from the repo root:

    .venv/bin/python scripts/check_arch_gates.py

These three gates are the permanent CI set. They stop the failure modes this
repo actually hit — complexity regrowth in the hub, layering cycles, and
silent coverage drops — without LOC/doc-presence bureaucracy.

Prints PASS/FAIL per gate and exits nonzero if any gate fails. Stdlib-only,
so CI can run ``python scripts/check_arch_gates.py`` without the project venv.

Each gate runs under exception isolation: a gate that raises is recorded as a
FAIL and the remaining gates still run, so one broken gate cannot hide the
others.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Permanent set (names match the script purpose — no letter codes):
#   complexity — no god-function regrowth; adapter mean cap (cc_gate.py)
#   layering   — adapter is the only hub (layer_gate.py)
#   extraction — coverage source + test file (extraction_gate.py)
GATES = [
    ("complexity", "Complexity", "cc_gate"),
    ("layering", "Layering", "layer_gate"),
    ("extraction", "Extraction", "extraction_gate"),
]


def main(argv: list[str] | None = None) -> int:
    results: list[tuple[str, bool]] = []
    for key, label, module_name in GATES:
        try:
            # Import lives inside the isolation boundary too: a gate that fails
            # to import (SyntaxError, deleted, etc.) must be recorded as a FAIL
            # so the remaining gates still run — not crash the whole runner.
            module = importlib.import_module(module_name)
            ok = module.main() == 0
        except Exception as exc:
            # Isolation boundary (not a swallow): the exception is reported and
            # recorded as a FAIL so the remaining gates still execute.
            print(f"  {module_name} raised: {type(exc).__name__}: {exc}")
            ok = False
        results.append((key, ok))
        print(f"{label}: {'PASS' if ok else 'FAIL'}")

    failures = [key for key, ok in results if not ok]
    if failures:
        print(f"Architecture gates: FAIL ({', '.join(failures)})")
        return 1
    print("Architecture gates: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
