"""Extraction-completeness gate (stdlib only).

Every top-level repo module (all ``*.py`` except ``__init__.py``, ``test_*.py``
and ``scripts/``) must:

  (a) be registered in ``[tool.coverage.run].source`` in pyproject.toml —
      otherwise coverage silently drops to 0% for it (the documented gotcha);
  (b) have a matching ``test_<module>.py`` file — or be in the legacy list of
      modules still covered only by the integration file (test_meshtastic.py),
      which MUST shrink as per-domain tests are split out.

The shrink is enforced, not just documented: the gate FAILS if a module in the
legacy list gains its own ``test_<module>.py`` without first being removed
from ``LEGACY_COVERAGE_BY_INTEGRATION`` (acknowledging the shrink).

Also fails if a source entry points at a module that does not exist (stale
entries / broken ``meshtastic_tools`` mapping).

The module list is any ``*.py`` dropped at the repo root (except
``__init__``/``test_*``), so a stray scratch/notes file is treated as a real
module and forces a coverage entry plus a ``test_<module>.py`` or a FAIL.
That is deliberate — a file left at the root is assumed to be a module and
must be covered — not a bug to paper over. A missing or malformed
``pyproject.toml`` coverage key FAILs cleanly instead of crashing.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# mesh_tools.py is loaded dynamically as `meshtastic_tools` (adapter
# convention) — the coverage source entry uses that name.
COVERAGE_NAME = {"mesh_tools": "meshtastic_tools"}

# Modules whose tests still live in the integration file. Covered by
# test_meshtastic.py today; per-domain test files must take over over time.
# A legacy entry that gains its own test file FAILS the gate until it is
# removed from this set (the shrink must be acknowledged, not silent).
# mesh_tools already has test_mesh_tools.py, so it is NOT in this set.
LEGACY_COVERAGE_BY_INTEGRATION = {
    "adapter",
    "mock_interface",
    "schemas",
    "telemetry_db",
}


def _coverage_source() -> list[str]:
    try:
        with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError):
        # A missing/unreadable pyproject means every module is unregistered —
        # a clean FAIL via the missing-from-source checks, not a crash.
        return []
    try:
        return list(data.get("tool", {}).get("coverage", {}).get("run", {}).get("source", []))
    except (AttributeError, TypeError):
        # Valid TOML but a wrong shape — e.g. `[tool]\ncoverage = "str"` makes
        # an intermediate a non-dict scalar, so `.get` is not callable. Treat
        # as "no coverage config" (clean FAIL downstream), not a crash.
        return []


def _repo_modules() -> list[str]:
    return [
        path.stem
        for path in sorted(REPO_ROOT.glob("*.py"))
        if path.stem != "__init__" and not path.stem.startswith("test_")
    ]


def main(argv: list[str] | None = None) -> int:
    source = _coverage_source()
    modules = _repo_modules()
    failures: list[str] = []
    stale: list[str] = []

    for stem in modules:
        coverage_name = COVERAGE_NAME.get(stem, stem)
        if coverage_name not in source:
            failures.append(f"{stem}.py missing from [tool.coverage.run].source")
        test_file = REPO_ROOT / f"test_{stem}.py"
        if not test_file.exists() and stem not in LEGACY_COVERAGE_BY_INTEGRATION:
            failures.append(f"{stem}.py has no test_{stem}.py")

    for entry in source:
        filename = next((name for name, alias in COVERAGE_NAME.items() if alias == entry), entry)
        if not (REPO_ROOT / f"{filename}.py").exists():
            stale.append(entry)

    for stem in sorted(LEGACY_COVERAGE_BY_INTEGRATION):
        if (REPO_ROOT / f"test_{stem}.py").exists():
            failures.append(
                f"{stem}.py has its own test_{stem}.py — remove it from "
                "LEGACY_COVERAGE_BY_INTEGRATION (the shrink must be acknowledged)"
            )

    print(f"  modules: {', '.join(modules)}")
    print(f"  coverage source entries: {', '.join(source)}")
    print(
        f"  integration-covered (legacy, must shrink): "
        f"{', '.join(sorted(LEGACY_COVERAGE_BY_INTEGRATION))}"
    )
    if stale:
        print(f"  stale coverage entries: {', '.join(stale)}")
        failures.append(f"coverage source lists nonexistent module(s): {', '.join(stale)}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"Extraction gate: FAIL ({len(failures)} violation(s))")
        return 1
    print("Extraction gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
