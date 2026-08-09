"""Layering gate (stdlib ``ast`` only).

Parses every import statement (including the ``try/except ImportError``
dual-import pattern) in the repo's top-level ``.py`` modules and enforces the
dependency direction:

  (a) leaf modules (chunking, schemas, telemetry_db, node_freshness,
      mock_interface) must NOT import any of the fourteen repo modules;
  (b) the eight mid-tier modules — ack_state, mesh_tools, inbound,
      mesh_helpers, send_path, connection, solicited, transport — must NOT
      import adapter (they take adapter state as parameters, never as
      imports; transport is a blocking-transport sibling of adapter);
  (c) adapter.py may import anything (it is the orchestrator).

``mesh_tools.py`` may import telemetry_db/schemas; adapter's runtime
``importlib`` load of mesh_tools is invisible to AST by design, and so is any
other dynamic ``importlib``/``__import__`` usage — a documented limitation,
not a silent hole.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

REPO_MODULES = {
    "adapter",
    "ack_state",
    "connection",
    "inbound",
    "mesh_helpers",
    "mesh_tools",
    "send_path",
    "solicited",
    "schemas",
    "telemetry_db",
    "chunking",
    "mock_interface",
    "node_freshness",
    "transport",
}

# Leaf modules: extracted pure/persistence/mock concerns — they must never
# depend on any sibling, or the adapter coupling grows back.
LEAF_MODULES = {
    "chunking",
    "schemas",
    "telemetry_db",
    "node_freshness",
    "mock_interface",
}

# Adapter access is restricted to the adapter itself (and gateway internals).
# Mid-tier siblings take adapter state as parameters, never as imports.
# transport is included (2026-08-02): it is a blocking-transport sibling of
# the adapter, not a leaf — but only adapter may import adapter.
NO_ADAPTER_IMPORTS = {
    "ack_state",
    "mesh_tools",
    "inbound",
    "mesh_helpers",
    "send_path",
    "connection",
    "solicited",
    "transport",
}

_MODULES = sorted(REPO_ROOT.glob("*.py"))


def _imported_repo_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module.split(".")[0])
            elif node.level:
                # Relative sibling import `from . import x`: the names enter
                # this module's namespace. For `from .adapter import y` the
                # module above already recorded "adapter"; the aliases of an
                # absolute import (`from gateway import adapter`) are NOT this
                # module's own imports and must not be attributed to it.
                for alias in node.names:
                    imported.add(alias.name.split(".")[0])
    return imported & REPO_MODULES


def main(argv: list[str] | None = None) -> int:
    failures: list[str] = []
    for path in _MODULES:
        stem = path.stem
        if stem not in REPO_MODULES:
            continue
        try:
            imports = _imported_repo_modules(path)
        except (OSError, SyntaxError, UnicodeDecodeError, ValueError) as exc:
            failures.append(f"{path.name} cannot be parsed: {type(exc).__name__}: {exc}")
            continue
        if stem in LEAF_MODULES and imports:
            failures.append(
                f"{path.name} (leaf) imports repo modules: {', '.join(sorted(imports))}"
            )
        if stem in NO_ADAPTER_IMPORTS and "adapter" in imports:
            failures.append(f"{path.name} imports adapter")

    for path in _MODULES:
        if path.stem in REPO_MODULES:
            try:
                imports = _imported_repo_modules(path)
            except (OSError, SyntaxError, UnicodeDecodeError, ValueError):
                continue
            print(f"  {path.name}: imports {', '.join(sorted(imports)) or '—'}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"Layering gate: FAIL ({len(failures)} violation(s))")
        return 1
    print("Layering gate: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
