"""Complexity gate (stdlib ``ast`` only).

Per-function McCabe-style cyclomatic complexity for every ``.py`` module at
the repo root. Fails when:

  (a) any function has cc >= ``CC_FAIL_THRESHOLD`` and is not in the
      grandfather allowlist below;
  (b) ``adapter.py``'s mean per-function cc exceeds ``ADAPTER_MEAN_LIMIT``.

Counting (intentional McCabe approximation, not radon-exact): base 1, +1 per
``if``/``elif``/``for``/``while``/``except``/``with``/``assert``/``bool-op``
(each ``BoolOp`` adds a flat +1 regardless of operand count — a deliberate
simplification vs. radon/mccabe, which count n-1 operators for n operands;
kept lenient so a chained boolean does not trip the threshold on its own),
ternary (``IfExp``)/comprehension/``lambda``/``match``-case, plus +1 per
comprehension ``if`` filter; ``async for``/``async with`` count like their
sync counterparts. Nested functions are counted separately (they never add to
the enclosing function's score).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Grandfather allowlist: (module, qualified function) -> measured cc.
# Functions are qualified with their full path (Class.method.helper) so a NEW
# cc >= 20 method in a DIFFERENT class with the same bare name cannot inherit
# an exemption.
# MUST STAY EMPTY — the last four adapter functions (_reconnect_loop 30->13,
# _disconnect_impl 38->17, _send_immediate 32->16, _send_text_serialized
# 22->7) were cleared by the connection.py/send_path.py extractions
# (2026-08-01). Re-adding an entry requires an explicit decision and a
# shrinking plan; the gate otherwise fails any function >= CC_FAIL_THRESHOLD.
# Drift (a re-added entry whose measured cc no longer matches) FAILS the gate
# too — it is not a warning.
GRANDFATHER_ALLOWLIST: dict[tuple[str, str], int] = {}

CC_FAIL_THRESHOLD = 20
# adapter.py mean per-function cc cap. 5.3 (pre-P3) -> 4.5 at the P3.5+
# milestone: the extractions brought the measured mean to ~3.6, so 4.5 is a
# deliberate tripwire for future growth, not a free pass.
ADAPTER_MEAN_LIMIT = 4.5

_MODULES = sorted(REPO_ROOT.glob("*.py"))

_DECISION_NODES = (
    ast.If,  # each elif is itself an If inside orelse, so elifs count too
    ast.For,
    ast.While,
    ast.ExceptHandler,
    ast.With,
    ast.Assert,
    ast.BoolOp,  # flat +1 per BoolOp (lenient vs. radon's n-1) — see module docstring
    ast.IfExp,  # ternary `x if c else y` — a branch, like an if
    ast.AsyncFor,  # distinct AST node in 3.11; counts like `for`
    ast.AsyncWith,  # distinct AST node in 3.11; counts like `with`
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
    ast.Lambda,
    ast.Match,
    ast.match_case,  # no ast.MatchCase alias in some 3.11 builds
)

_NESTED_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


class _ComplexityVisitor(ast.NodeVisitor):
    """Counts decision points in one function body; never descends into
    nested defs/lambdas (each is measured separately)."""

    def __init__(self) -> None:
        self.complexity = 1

    def visit(self, node: ast.AST) -> None:
        if isinstance(node, _NESTED_FUNCTIONS):
            return
        if isinstance(node, _DECISION_NODES):
            self.complexity += 1
        if isinstance(node, ast.comprehension):
            # `[i for i in x if c]`: each filter is a branch (radon counts
            # these); the comprehension expression itself is already counted.
            self.complexity += len(node.ifs)
        self.generic_visit(node)


def _complexity_of(func: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> int:
    visitor = _ComplexityVisitor()
    body = func.body if isinstance(func, _NESTED_FUNCTIONS[:2]) else [func.body]
    for stmt in body:
        visitor.visit(stmt)
    return visitor.complexity


def _module_functions(path: Path) -> list[tuple[str, int]]:
    """(qualified name, cc) per function in ``path``.

    Methods are qualified with their full path (``Class.method.helper``) so
    the allowlist cannot be inherited by a same-named function elsewhere;
    module-level functions keep their bare name. Nested functions are still
    counted separately (they never add to the enclosing function's score).
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions: list[tuple[str, int]] = []

    def collect(node: ast.AST, qualname: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{qualname}.{child.name}" if qualname else child.name
                functions.append((name, _complexity_of(child)))
                collect(child, name)
            elif isinstance(child, ast.Lambda):
                # Qualify lambdas with their enclosing path too, so a
                # same-bare-`<lambda>` in another class/method cannot inherit
                # an allowlist exemption.
                name = f"{qualname}.<lambda>" if qualname else "<lambda>"
                functions.append((name, _complexity_of(child)))
                collect(child, name)
            elif isinstance(child, ast.ClassDef):
                collect(child, f"{qualname}.{child.name}" if qualname else child.name)
            else:
                collect(child, qualname)

    collect(tree, "")
    return functions


def main(argv: list[str] | None = None) -> int:
    failures: list[str] = []
    adapter_mean = 0.0
    total_functions = 0

    if not _MODULES:
        # An empty module set means REPO_ROOT resolved wrong (script moved/
        # copied out of scripts/) — iterating nothing would print a misleading
        # PASS. Fail loudly instead.
        failures.append("no modules found — REPO_ROOT may be wrong")

    for path in _MODULES:
        try:
            functions = _module_functions(path)
        except (OSError, SyntaxError, UnicodeDecodeError, ValueError) as exc:
            failures.append(f"{path.name} cannot be parsed: {type(exc).__name__}: {exc}")
            continue
        total_functions += len(functions)
        for name, cc in functions:
            if cc >= CC_FAIL_THRESHOLD and (path.name, name) not in GRANDFATHER_ALLOWLIST:
                failures.append(f"{path.name}:{name} cc={cc} (>= {CC_FAIL_THRESHOLD})")

        for (module, fname), stored_cc in GRANDFATHER_ALLOWLIST.items():
            if module != path.name:
                continue
            # Iterate the raw per-function list (like enforcement does) so a
            # duplicate name cannot last-wins its way out of a drift failure.
            measured = [cc for name, cc in functions if name == fname]
            if not measured:
                failures.append(f"allowlisted {module}:{fname} no longer exists; drop the entry")
            else:
                for cc in measured:
                    if cc != stored_cc:
                        failures.append(
                            f"allowlisted {module}:{fname} cc drifted "
                            f"(stored {stored_cc}, measured {cc}); update the allowlist"
                        )

        if functions:
            mean = sum(cc for _, cc in functions) / len(functions)
            max_cc = max(cc for _, cc in functions)
            max_name = max(functions, key=lambda pair: pair[1])[0]
        else:
            mean = 0.0
            max_cc = 0
            max_name = "-"
        print(
            f"  {path.name}: {len(functions)} functions, mean cc {mean:.2f}, max cc {max_cc} ({max_name})"
        )
        if path.name == "adapter.py":
            adapter_mean = mean

    print(f"  adapter.py mean cc: {adapter_mean:.2f} (limit {ADAPTER_MEAN_LIMIT})")
    if adapter_mean > ADAPTER_MEAN_LIMIT:
        failures.append(f"adapter.py mean cc {adapter_mean:.2f} exceeds {ADAPTER_MEAN_LIMIT}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"Complexity gate: FAIL ({len(failures)} violation(s))")
        return 1
    print(f"Complexity gate: PASS ({total_functions} functions across {len(_MODULES)} modules)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
