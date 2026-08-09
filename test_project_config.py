"""Project-config / CI invariants (C17).

Pins the machine-verifiable claims that would otherwise rot silently:

  * CI runs ``unittest discover`` (not a hand-maintained file list), so a new
    ``test_*.py`` — including the arch-gate tests — can never drop out of CI.
  * coverage ``exclude_lines`` re-includes the ``if TYPE_CHECKING:`` default
    that ``exclude_lines`` replacement would otherwise drop (#3).
  * the OpenCode review job is least-privilege: no
    ``--dangerously-skip-permissions``, a pinned opencode-ai version, scoped
    token, a bash allowlist of only the read-only ``gh`` calls the review
    needs, and the comment is posted by the workflow (not the sandboxed
    model) so a prompt-injected payload has no exfiltration path.
  * every ``MESHTASTIC_*`` env var read by a source module is documented in
    README.md (#6).
  * pyproject ``dependencies`` and requirements.txt declare the same
    specifiers (#7).
  * CLAUDE.md's per-module "imports only …" claims match the modules' actual
    AST import sets (#8).
  * GitHub Actions are SHA-pinned (no mutable ``@vN`` tag pins) so a
    compromised tag cannot execute code in CI.

Stdlib only — it never imports Hermes or the meshtastic library.
"""

import ast
import json
import re
import tomllib
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

_SOURCE_MODULES = [
    path
    for path in sorted(REPO_ROOT.glob("*.py"))
    if path.stem != "__init__" and not path.stem.startswith("test_")
]


def _walk(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _walk(item)
        else:
            yield item


def _workflow_text(name: str) -> str:
    return (WORKFLOW_DIR / name).read_text(encoding="utf-8")


class TestCiUsesUnittestDiscovery(unittest.TestCase):
    """Arch-gate and other test_*.py files must run in CI via discovery,
    and a new test_*.py must not be silently skipped by a stale file list."""

    WORKFLOWS = ("ci.yml", "hermes-drift.yml")

    def test_unit_tests_use_discovery_not_a_file_list(self):
        for name in self.WORKFLOWS:
            with self.subTest(workflow=name):
                text = _workflow_text(name)
                self.assertIn("unittest discover", text)

    def test_every_root_test_file_is_discovered(self):
        expected = sorted(p.stem for p in REPO_ROOT.glob("test_*.py"))
        suite = unittest.TestLoader().discover(str(REPO_ROOT), pattern="test_*.py")
        discovered = sorted({t.id().split(".")[0] for t in _walk(suite)})
        self.assertEqual(discovered, expected)


class TestCoverageExcludesTypeChecking(unittest.TestCase):
    """Review finding #3: exclude_lines replaces coverage's defaults, so the
    ``if TYPE_CHECKING:`` exclusion must be re-included explicitly."""

    def test_exclude_lines_reincludes_type_checking(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
            data = tomllib.load(fh)
        exclude = data["tool"]["coverage"]["report"]["exclude_lines"]
        self.assertIn("if TYPE_CHECKING:", exclude)
        self.assertIn("pragma: no cover", exclude)
        self.assertIn("if __name__ == .__main__.:", exclude)


class TestGithubActionsAreShaPinned(unittest.TestCase):
    """Supply-chain: a compromised/yanked mutable action tag
    (``actions/checkout@v4`` re-pointed at a malicious commit) would execute
    arbitrary code in CI with access to the workflow token. Every ``uses:``
    must pin a full 40-char commit SHA (the immovable identifier); the tag is
    kept as a trailing comment for readability and Dependabot updates."""

    _TAG_PIN = re.compile(r"uses:\s+[\w./-]+@v?\d[^\s#]*")
    _SHA_PIN = re.compile(r"uses:\s+[\w./-]+@([0-9a-f]{40})(?:\s*#.*)?$")

    def _workflow_paths(self):
        return sorted(WORKFLOW_DIR.glob("*.yml"))

    def test_no_mutable_tag_pins(self):
        offenders = []
        for path in self._workflow_paths():
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if self._TAG_PIN.search(line) and not self._SHA_PIN.search(line):
                    offenders.append(f"{path.name}:{lineno}: {line.strip()}")
        self.assertEqual(offenders, [], "found mutable action tag pins:\n" + "\n".join(offenders))

    def test_all_uses_are_sha_pinned(self):
        bad = []
        for path in self._workflow_paths():
            for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if not stripped.startswith("uses:"):
                    continue
                if not self._SHA_PIN.search(line):
                    bad.append(f"{path.name}:{lineno}: {line.strip()}")
        self.assertEqual(bad, [], "found non-SHA-pinned action uses:\n" + "\n".join(bad))


class TestOpenCodeReviewLeastPrivilege(unittest.TestCase):
    """Review findings #4/#5: the reviewer consumes attacker-influenceable PR
    content, so the job must not grant unrestricted command execution, and the
    model must have no path to post or mutate the PR (a write path is an
    exfiltration channel — see the HIGH finding on `gh pr comment`)."""

    def _review_yml(self) -> str:
        return _workflow_text("opencode-review.yml")

    def test_no_dangerously_skip_permissions(self):
        self.assertNotIn("dangerously-skip-permissions", self._review_yml())

    def test_opencode_ai_is_pinned(self):
        text = self._review_yml()
        self.assertNotIn("opencode-ai@latest", text)
        self.assertRegex(text, r"opencode-ai@")

    def test_opencode_ai_version_in_package_json_is_exact_pin(self):
        # The workflow installs `opencode-ai@<package.json value>`, so the
        # value must be a bare version — `^1.18.11` / `~1.18.11` / `latest`
        # would resolve to a floating version, a reproducibility/supply-chain
        # regression the workflow text alone would not catch.
        pkg = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))
        version = pkg["devDependencies"]["opencode-ai"]
        self.assertRegex(version, r"^\d+\.\d+\.\d+$", f"opencode-ai pin is not exact: {version!r}")

    def test_token_scoped_to_pull_requests(self):
        text = self._review_yml()
        self.assertIn("contents: read", text)
        self.assertIn("pull-requests: write", text)
        self.assertNotIn("contents: write", text)

    def test_permission_config_allows_only_read_only_gh(self):
        # The sandboxed model may only run the two READ-ONLY `gh pr` calls.
        # `gh pr comment` is intentionally absent: posting the review is the
        # workflow's job, so a prompt-injected payload has no command that
        # can publish to (and therefore no channel to exfiltrate secrets via)
        # the PR.
        cfg = json.loads((REPO_ROOT / ".github" / "opencode-review.json").read_text())
        bash = cfg["permission"]["bash"]
        self.assertEqual(bash["*"], "deny")
        self.assertEqual(bash["gh pr view *"], "allow")
        self.assertEqual(bash["gh pr diff *"], "allow")
        for pattern in bash:
            self.assertNotIn("comment", pattern, f"write path leaked into allowlist: {pattern!r}")
        self.assertEqual(cfg["permission"]["edit"], "deny")

    def test_workflow_uses_pinned_config(self):
        self.assertIn("OPENCODE_CONFIG", self._review_yml())
        self.assertIn("opencode-review.json", self._review_yml())

    def test_workflow_posts_review_from_captured_stdout(self):
        # The model has no `gh pr comment` permission, so the workflow itself
        # must capture the model's stdout and post it. This is the structural
        # fix that closes the exfiltration path opened by granting the model
        # an inline-comment command.
        text = self._review_yml()
        self.assertRegex(text, r"gh pr comment .* --body-file")
        self.assertIn("RUNNER_TEMP", text)

    def test_review_prompt_flags_diff_as_untrusted(self):
        # Both PR description and diff are attacker-controlled; the prompt
        # must warn about both, not only the description. A diff containing a
        # prompt-injection payload in a code comment or string literal is just
        # as dangerous as one in the PR body.
        text = self._review_yml()
        self.assertIn("untrusted", text.lower())
        # The diff caveat specifically — not just the description's. The
        # prompt wraps the caveat across lines ("The diff\n  is untrusted"),
        # so match across whitespace including newlines.
        self.assertRegex(text, r"(?i)diff[\s\S]{0,40}untrusted")


class TestEnvVarsDocumentedInReadme(unittest.TestCase):
    """Review finding #6: every MESHTASTIC_* env read in a source module must
    appear in README.md's env documentation."""

    def test_all_env_reads_are_in_readme(self):
        readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
        missing = sorted(
            {
                match
                for module in _SOURCE_MODULES
                for match in re.findall(
                    r"['\"](MESHTASTIC_[A-Z0-9_]+)['\"]", module.read_text(encoding="utf-8")
                )
                if f"`{match}`" not in readme
            }
        )
        self.assertEqual(missing, [])


class TestDependenciesMatchRequirements(unittest.TestCase):
    """Review finding #7: pyproject dependencies and requirements.txt must not
    drift (the auto-install path consumes requirements.txt; package metadata
    consumes pyproject).

    Comparison normalizes cosmetic differences (whitespace around commas,
    specifier reordering) so a benign reformat on one side does not produce a
    confusing failure while a real semantic drift is still caught. The full
    PEP 440 comparison lives in ``packaging`` which is not a stdlib
    dependency, so we normalize the specifier *syntax* (name + comma-joined,
    comma-split, stripped, sorted, space-free) — sufficient for the
    machine-editable pair this guards."""

    @staticmethod
    def _normalize(spec: str) -> str:
        # "meshtastic>=2.7.10, <2.8" -> "meshtastic<2.8,>=2.7.10"
        m = re.match(r"^([A-Za-z0-9_.-]+)\s*(.*)$", spec.strip())
        if m is None:
            return spec.strip()
        name, rest = m.group(1), m.group(2)
        if not rest:
            return name
        parts = sorted(s.strip() for s in rest.split(",") if s.strip())
        return f"{name}," + ",".join(parts)

    def _pyproject_deps(self) -> set[str]:
        with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
            data = tomllib.load(fh)
        return {self._normalize(d) for d in data["project"]["dependencies"]}

    def _requirements_specs(self) -> set[str]:
        lines = [
            line.strip()
            for line in (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        return {self._normalize(line) for line in lines}

    def test_pyproject_and_requirements_declare_same_specifiers(self):
        self.assertEqual(self._pyproject_deps(), self._requirements_specs())


class TestModuleImportClaimsMatchDocs(unittest.TestCase):
    """Review finding #8: CLAUDE.md's "imports only …" claims must match the
    modules' actual AST import sets (a new import without a doc update fails)."""

    CLAIMS = {
        "inbound": {"telemetry_db", "chunking", "mesh_helpers"},
        "send_path": {"ack_state", "transport"},
    }

    def _repo_imports(self, stem: str) -> set[str]:
        tree = ast.parse((REPO_ROOT / f"{stem}.py").read_text(encoding="utf-8"))
        repo = {p.stem for p in _SOURCE_MODULES}
        found: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.level == 1:
                if node.module is None:
                    for alias in node.names:
                        if alias.name in repo and alias.name != stem:
                            found.add(alias.name)
                elif node.module in repo and node.module != stem:
                    found.add(node.module)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name in repo and alias.name != stem:
                        found.add(alias.name)
        return found

    def _claude_bullet(self, stem: str) -> str:
        text = (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
        match = re.search(rf"- \*\*`{stem}\.py`\*\*.*?(?=\n- \*\*|\Z)", text, re.S)
        self.assertIsNotNone(match, f"no CLAUDE.md bullet found for {stem}.py")
        return match.group(0)

    def test_import_claims_match_actual_imports(self):
        for stem, expected in self.CLAIMS.items():
            with self.subTest(module=stem):
                self.assertEqual(self._repo_imports(stem), expected)

    def test_documented_imports_appear_in_claude_bullet(self):
        for stem, imports in self.CLAIMS.items():
            bullet = self._claude_bullet(stem)
            for imported in sorted(imports):
                with self.subTest(module=stem, imported=imported):
                    # The bullet may name a module as `module` or `module.py`.
                    self.assertRegex(bullet, rf"`{re.escape(imported)}(\.py)?`")


if __name__ == "__main__":
    unittest.main()
