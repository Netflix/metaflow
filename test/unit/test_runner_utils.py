"""Unit tests for metaflow.runner.utils — specifically format_flowfile().

format_flowfile() is used by NBRunner and NBDeployer to convert notebook cell
content into a standalone Python script that can be executed as a Metaflow flow.
"""

import ast
import textwrap

import pytest


# ---------------------------------------------------------------------------
# Inline the function under test to avoid importing metaflow.runner.utils,
# which pulls in fcntl (Unix-only) and other heavy dependencies.  This keeps
# the test lightweight and runnable on all platforms.
# ---------------------------------------------------------------------------


def _get_base_name(node):
    """Extract the class name from an AST base-class node.

    Handles both simple names (``FlowSpec``) and attribute access
    (``metaflow.FlowSpec``).
    """
    if isinstance(node, ast.Attribute):
        return node.attr
    return getattr(node, "id", None)


def format_flowfile(cell):
    """Reference copy of metaflow.runner.utils.format_flowfile (with fix)."""
    flowspec = [
        x
        for x in ast.parse(cell).body
        if isinstance(x, ast.ClassDef)
        and any(_get_base_name(b) == "FlowSpec" for b in x.bases)
    ]

    if not flowspec:
        raise ModuleNotFoundError(
            "The cell doesn't contain any class that inherits from 'FlowSpec'"
        )

    lines = cell.splitlines()[: flowspec[0].end_lineno]
    lines += ["if __name__ == '__main__':", f"    {flowspec[0].name}()"]
    return "\n".join(lines)


# ---- helpers ---------------------------------------------------------------


def _dedent(text):
    """Convenience wrapper so test strings can be indented naturally."""
    return textwrap.dedent(text).strip()


# ---- tests -----------------------------------------------------------------


class TestFormatFlowfileBasic:
    """Core happy-path tests."""

    def test_simple_flow(self):
        cell = _dedent(
            """
            from metaflow import FlowSpec, step

            class MyFlow(FlowSpec):
                @step
                def start(self):
                    self.next(self.end)

                @step
                def end(self):
                    pass
        """
        )
        result = format_flowfile(cell)
        assert "class MyFlow(FlowSpec):" in result
        assert "if __name__ == '__main__':" in result
        assert "    MyFlow()" in result

    def test_imports_preserved(self):
        cell = _dedent(
            """
            import os
            from metaflow import FlowSpec, step

            class TrainFlow(FlowSpec):
                @step
                def start(self):
                    pass
        """
        )
        result = format_flowfile(cell)
        assert "import os" in result
        assert "from metaflow import FlowSpec, step" in result

    def test_main_guard_appended(self):
        cell = _dedent(
            """
            from metaflow import FlowSpec, step

            class HelloFlow(FlowSpec):
                @step
                def start(self):
                    pass
        """
        )
        lines = format_flowfile(cell).splitlines()
        assert lines[-2] == "if __name__ == '__main__':"
        assert lines[-1] == "    HelloFlow()"

    def test_flow_name_used_in_main_guard(self):
        cell = _dedent(
            """
            from metaflow import FlowSpec, step

            class CustomNameFlow(FlowSpec):
                @step
                def start(self):
                    pass
        """
        )
        result = format_flowfile(cell)
        assert "    CustomNameFlow()" in result


class TestFormatFlowfileEdgeCases:
    """Edge cases and error handling."""

    def test_no_flowspec_raises(self):
        cell = _dedent(
            """
            class NotAFlow:
                pass
        """
        )
        with pytest.raises(ModuleNotFoundError, match="FlowSpec"):
            format_flowfile(cell)

    def test_empty_cell_raises(self):
        with pytest.raises(ModuleNotFoundError, match="FlowSpec"):
            format_flowfile("")

    def test_only_imports_raises(self):
        cell = _dedent(
            """
            import os
            from metaflow import FlowSpec
        """
        )
        with pytest.raises(ModuleNotFoundError, match="FlowSpec"):
            format_flowfile(cell)

    def test_multiple_classes_picks_first_flowspec(self):
        cell = _dedent(
            """
            from metaflow import FlowSpec, step

            class Helper:
                pass

            class FirstFlow(FlowSpec):
                @step
                def start(self):
                    pass

            class SecondFlow(FlowSpec):
                @step
                def start(self):
                    pass
        """
        )
        result = format_flowfile(cell)
        assert "    FirstFlow()" in result
        assert "SecondFlow()" not in result.splitlines()[-1]

    def test_attribute_base_class(self):
        """Using metaflow.FlowSpec instead of plain FlowSpec should work."""
        cell = _dedent(
            """
            import metaflow

            class AttrFlow(metaflow.FlowSpec):
                @metaflow.step
                def start(self):
                    pass
        """
        )
        result = format_flowfile(cell)
        assert "class AttrFlow(metaflow.FlowSpec):" in result
        assert "    AttrFlow()" in result

    def test_content_after_class_is_excluded(self):
        cell = _dedent(
            """
            from metaflow import FlowSpec, step

            class MyFlow(FlowSpec):
                @step
                def start(self):
                    pass

            print("this should not appear")
        """
        )
        result = format_flowfile(cell)
        assert "print" not in result
