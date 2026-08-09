"""Unit tests for the complexity gate (scripts/cc_gate.py).

These pin the counting contract (which decision-point AST nodes add to a
function's score) and the allowlist naming/qualification rules, so a
regression that silently makes the gate more lenient fails CI.
"""

import ast
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

import cc_gate

FIXTURE = """\
def probe(x, y, items):
    e = 1 if x else 2
    if x and y:
        return 0
    while x:
        break
    with y:
        pass
    return [i for i in items if e > 0]


def ternary(x):
    return 1 if x else 2


def async_fixture(items):
    async for i in items:
        pass


def comprehension_ifs(items):
    return [i for i in items if i]


def nested(flag):
    def inner(c):
        return c if flag else not c

    return inner


class Cls:
    def method(self):
        def helper(self):
            return 1 if self else 0

        return helper
"""


class TestCountingContract(unittest.TestCase):
    """The visitor counts what standard McCabe (radon) counts: ternaries,
    async loops, comprehension-if filters, alongside if/for/with/..."""

    def setUp(self):
        self.tree = ast.parse(FIXTURE)

    def _fn(self, name):
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                return node
        self.fail(f"no function {name!r} in fixture")

    def test_ternary_ifexp_counts(self):
        # base 1 + IfExp = 2 (previously the ternary was invisible).
        self.assertEqual(cc_gate._complexity_of(self._fn("ternary")), 2)

    def test_async_for_counts(self):
        self.assertEqual(cc_gate._complexity_of(self._fn("async_fixture")), 2)

    def test_comprehension_if_counts(self):
        # base 1 + ListComp + comprehension-ifs = 3.
        self.assertEqual(cc_gate._complexity_of(self._fn("comprehension_ifs")), 3)

    def test_probe_total_counts_all_constructs(self):
        # IfExp + If + BoolOp + While + With + ListComp + comp-if = 8.
        self.assertEqual(cc_gate._complexity_of(self._fn("probe")), 8)

    def test_nested_function_does_not_add_to_enclosing(self):
        self.assertEqual(cc_gate._complexity_of(self._fn("nested")), 1)
        self.assertEqual(cc_gate._complexity_of(self._fn("inner")), 2)

    def test_two_operand_boolop_counts_one(self):
        # `a and b` is one BoolOp ⇒ flat +1 (base 1 ⇒ cc 2).
        tree = ast.parse("def f(a, b):\n    return a and b\n")
        func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
        self.assertEqual(cc_gate._complexity_of(func), 2)

    def test_chained_boolop_counts_flat_one(self):
        # `a and b and c and d` is one BoolOp with 4 operands. The gate counts
        # a flat +1 (a deliberate simplification vs. radon/mccabe's n-1=3),
        # documented in the module docstring. Pinning this keeps the docstring
        # honest: a future "fix" toward n-1 must change both this test and the
        # docstring together, and will trip the threshold on real adapters.
        tree = ast.parse("def f(a, b, c, d):\n    return a and b and c and d\n")
        func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
        self.assertEqual(cc_gate._complexity_of(func), 2)


class TestModuleFunctionsQualification(unittest.TestCase):
    """Nested functions are qualified with their full path, so two helpers of
    the same name in different classes/methods cannot collide in the
    allowlist."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "fixture.py"
        self.path.write_text(FIXTURE, encoding="utf-8")
        self.functions = dict(cc_gate._module_functions(self.path))

    def test_method_nested_helper_is_class_method_qualified(self):
        self.assertIn("Cls.method.helper", self.functions)
        self.assertNotIn("Cls.helper", self.functions)

    def test_module_level_function_keeps_bare_name(self):
        self.assertIn("probe", self.functions)
        self.assertNotIn(".probe", self.functions)

    def test_lambda_qualified_by_scope(self):
        # Two lambdas in different classes/methods must get distinct qualified
        # names so a bare `<lambda>` allowlist entry cannot exempt all of them
        # (the "cannot inherit an exemption" guarantee extends to lambdas).
        source = (
            "class A:\n"
            "    def method(self):\n"
            "        return lambda: 1\n"
            "class B:\n"
            "    def method(self):\n"
            "        return lambda: 2\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.py"
            path.write_text(source, encoding="utf-8")
            names = [name for name, _ in cc_gate._module_functions(path)]
        self.assertIn("A.method.<lambda>", names)
        self.assertIn("B.method.<lambda>", names)
        self.assertNotIn("<lambda>", names)


class TestGateEnforcement(unittest.TestCase):
    def _write(self, source, name="fixture.py"):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        path = Path(self._tmp.name) / name
        path.write_text(source, encoding="utf-8")
        return path

    def test_function_at_threshold_fails(self):
        path = self._write("def heavy():\n    if True:\n        pass\n")
        with (
            patch.object(cc_gate, "_MODULES", [path]),
            patch.object(cc_gate, "GRANDFATHER_ALLOWLIST", {}),
            patch.object(cc_gate, "CC_FAIL_THRESHOLD", 2),
        ):
            self.assertEqual(cc_gate.main(), 1)

    def test_allowlist_drift_fails_gate(self):
        # A re-added grandfather entry whose measured cc no longer matches the
        # stored value must FAIL (previously it only printed a warning).
        path = self._write("def drifted():\n    if True:\n        pass\n")
        with (
            patch.object(cc_gate, "_MODULES", [path]),
            patch.object(cc_gate, "GRANDFATHER_ALLOWLIST", {("fixture.py", "drifted"): 1}),
        ):
            self.assertEqual(cc_gate.main(), 1)

    def test_allowlist_entry_missing_fails_gate(self):
        # An allowlist entry pointing at a function that no longer exists must
        # fail (previously a warning only).
        path = self._write("def unrelated():\n    pass\n")
        with (
            patch.object(cc_gate, "_MODULES", [path]),
            patch.object(cc_gate, "GRANDFATHER_ALLOWLIST", {("fixture.py", "gone"): 5}),
        ):
            self.assertEqual(cc_gate.main(), 1)

    def test_unparseable_module_fails_cleanly(self):
        path = self._write("def broken(:\n    pass\n")
        with (
            patch.object(cc_gate, "_MODULES", [path]),
            patch.object(cc_gate, "GRANDFATHER_ALLOWLIST", {}),
        ):
            self.assertEqual(cc_gate.main(), 1)

    def test_empty_modules_list_fails(self):
        # An empty _MODULES (REPO_ROOT resolved wrong / script copied out of
        # scripts/) must FAIL loudly — not print a misleading PASS over zero
        # functions and zero modules.
        buf = io.StringIO()
        with (
            patch.object(cc_gate, "_MODULES", []),
            patch.object(cc_gate, "GRANDFATHER_ALLOWLIST", {}),
            contextlib.redirect_stdout(buf),
        ):
            self.assertEqual(cc_gate.main(), 1)
        self.assertIn("no modules found", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
