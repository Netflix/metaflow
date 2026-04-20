"""
Unit tests for conda/pypi parsers (requirements.txt, pyproject.toml, environment.yml).

Pure logic tests — no infrastructure needed.
"""

import pytest

from metaflow.plugins.pypi.parsers import (
    ParserValueError,
    conda_environment_yml_parser,
    requirements_txt_parser,
)


# ---------------------------------------------------------------------------
# requirements_txt_parser
# ---------------------------------------------------------------------------


class TestRequirementsTxtParser:
    def test_simple_package(self):
        result = requirements_txt_parser("requests==2.28.0\n")
        assert result["packages"] == {"requests": "2.28.0"}
        assert result["python"] is None

    def test_multiple_packages(self):
        content = "requests>=2.0\nnumpy==1.21.0\npandas\n"
        result = requirements_txt_parser(content)
        assert result["packages"]["requests"] == ">=2.0"
        assert result["packages"]["numpy"] == "1.21.0"
        assert result["packages"]["pandas"] == ""

    def test_python_version(self):
        content = "python==3.9\nrequests\n"
        result = requirements_txt_parser(content)
        assert result["python"] == "3.9"
        assert "python" not in result["packages"]
        assert result["packages"]["requests"] == ""

    def test_comments_and_blank_lines(self):
        content = "# this is a comment\n\nrequests==2.0\n  # another comment\nnumpy\n"
        result = requirements_txt_parser(content)
        assert len(result["packages"]) == 2

    def test_inline_comments(self):
        content = "requests==2.0  # HTTP library\n"
        result = requirements_txt_parser(content)
        assert result["packages"]["requests"] == "2.0"

    def test_extras(self):
        content = "requests[security]==2.28.0\n"
        result = requirements_txt_parser(content)
        assert "requests[security]" in result["packages"]

    def test_direct_reference(self):
        content = "mylib @ git+https://github.com/user/repo.git\n"
        result = requirements_txt_parser(content)
        assert any("mylib" in k for k in result["packages"])

    def test_environment_markers_rejected(self):
        content = 'requests==2.0; python_version>="3.6"\n'
        with pytest.raises(ParserValueError, match="Environment markers"):
            requirements_txt_parser(content)

    def test_invalid_requirement(self):
        content = "not a valid requirement!!!\n"
        with pytest.raises(ParserValueError, match="Not a valid PEP 508"):
            requirements_txt_parser(content)

    def test_multiple_python_specs_rejected(self):
        content = "python==3.9\npython==3.10\n"
        with pytest.raises(ParserValueError, match="Multiple Python version"):
            requirements_txt_parser(content)

    def test_empty_content(self):
        result = requirements_txt_parser("")
        assert result["packages"] == {}
        assert result["python"] is None

    def test_rye_lockfile_skip(self):
        """Rye lockfiles contain '-e file:.' which should be silently skipped."""
        content = "-e file:.\nrequests==2.0\n"
        result = requirements_txt_parser(content)
        assert result["packages"]["requests"] == "2.0"


# ---------------------------------------------------------------------------
# conda_environment_yml_parser
# ---------------------------------------------------------------------------


class TestCondaEnvironmentYmlParser:
    def test_simple_deps(self):
        content = "dependencies:\n  - numpy=1.21.2\n  - pandas=1.3.0\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"]["numpy"] == "1.21.2"
        assert result["packages"]["pandas"] == "1.3.0"

    def test_python_version(self):
        content = "dependencies:\n  - python=3.9\n  - numpy\n"
        result = conda_environment_yml_parser(content)
        assert result["python"] == "3.9"
        assert "python" not in result["packages"]
        assert result["packages"]["numpy"] == ""

    def test_no_version(self):
        content = "dependencies:\n  - numpy\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"]["numpy"] == ""

    def test_comments_skipped(self):
        content = "# env file\ndependencies:\n  # a comment\n  - numpy=1.0\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"]["numpy"] == "1.0"

    def test_subsection_rejected(self):
        content = "dependencies:\n  - pip:\n    - requests\n"
        with pytest.raises(ParserValueError, match="Unsupported subsection"):
            conda_environment_yml_parser(content)

    def test_inline_comments(self):
        content = "dependencies:\n  - numpy=1.0  # math lib\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"]["numpy"] == "1.0"

    def test_empty_deps(self):
        content = "name: test\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"] == {}
        assert result["python"] is None

    def test_double_equals(self):
        content = "dependencies:\n  - numpy==1.21.2\n"
        result = conda_environment_yml_parser(content)
        assert result["packages"]["numpy"] == "1.21.2"
