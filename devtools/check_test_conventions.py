#!/usr/bin/env python
"""Pre-commit hook: enforce test conventions from CONTRIBUTING.md.

Checks test files for:
  1. unittest imports (use pytest instead)
  2. unittest.mock usage (use pytest-mock's mocker fixture)
  3. TestCase subclasses (use plain functions)
  4. Class-based test grouping (use module-level functions)

Legacy files that predate these conventions are allowlisted.
Remove entries as files are migrated.
"""
import ast
import sys

LEGACY_ALLOWLIST = frozenset(
    {
        "test/unit/test_secrets_decorator.py",
        "test/unit/test_s3_storage.py",
        "test/unit/test_system_context.py",
        "test/unit/inheritance/test_inheritance.py",
        "test/unit/mutators/test_add_decorator_returns.py",
        "test/unit/mutators/test_dual_inheritance.py",
        "test/unit/mutators/test_flow_mutator_addition.py",
        "test/unit/mutators/test_post_step_none_false.py",
        "test/unit/mutators/test_remove_decorator_guard.py",
        "test/unit/mutators/test_string_step_mutator.py",
        "test/cmd/diff/test_metaflow_diff.py",
        "test/cmd/develop/test_stub_generator.py",
    }
)


def check_file(path):
    violations = []

    with open(path) as f:
        source = f.read()

    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError:
        return violations

    for node in ast.walk(tree):
        # 1. import unittest / from unittest import ...
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "unittest" or alias.name.startswith("unittest."):
                    violations.append(
                        (node.lineno, f"import {alias.name}", "use pytest / pytest-mock")
                    )

        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "unittest" or node.module.startswith("unittest."):
                names = ", ".join(a.name for a in node.names)
                violations.append(
                    (
                        node.lineno,
                        f"from {node.module} import {names}",
                        "use pytest / pytest-mock (mocker fixture)",
                    )
                )

        # 2. class Test*(unittest.TestCase) — TestCase subclass
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                base_name = ast.dump(base)
                if "TestCase" in base_name:
                    violations.append(
                        (
                            node.lineno,
                            f"class {node.name}(…TestCase)",
                            "use module-level test functions, not TestCase",
                        )
                    )
                    break

        # 3. class Test* without TestCase — class-based grouping
        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            is_testcase = any("TestCase" in ast.dump(b) for b in node.bases)
            if not is_testcase:
                has_test_methods = any(
                    isinstance(item, ast.FunctionDef) and item.name.startswith("test_")
                    for item in node.body
                )
                if has_test_methods:
                    violations.append(
                        (
                            node.lineno,
                            f"class {node.name}",
                            "use module-level test functions, not test classes",
                        )
                    )

    return violations


def main():
    status = 0
    for path in sys.argv[1:]:
        # Normalize to forward-slash relative path for allowlist matching
        normalized = path.replace("\\", "/")
        if any(normalized.endswith(a) for a in LEGACY_ALLOWLIST):
            continue

        violations = check_file(path)
        for lineno, what, fix in violations:
            print(f"{path}:{lineno}: {what} -> {fix}")
            status = 1

    if status:
        print(
            "\nSee CONTRIBUTING.md § Test conventions. "
            "Legacy files are allowlisted in devtools/check_test_conventions.py."
        )

    return status


if __name__ == "__main__":
    sys.exit(main())
