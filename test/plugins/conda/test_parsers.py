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


def test_requirements_parser_simple_package():
    result = requirements_txt_parser("requests==2.28.0\n")
    assert result["packages"] == {"requests": "2.28.0"}
    assert result["python"] is None


def test_requirements_parser_multiple_packages():
    content = "requests>=2.0\nnumpy==1.21.0\npandas\n"
    result = requirements_txt_parser(content)
    assert result["packages"]["requests"] == ">=2.0"
    assert result["packages"]["numpy"] == "1.21.0"
    assert result["packages"]["pandas"] == ""


def test_requirements_parser_python_version():
    content = "python==3.9\nrequests\n"
    result = requirements_txt_parser(content)
    assert result["python"] == "3.9"
    assert "python" not in result["packages"]
    assert result["packages"]["requests"] == ""


def test_requirements_parser_comments_and_blank_lines():
    content = "# this is a comment\n\nrequests==2.0\n  # another comment\nnumpy\n"
    result = requirements_txt_parser(content)
    assert len(result["packages"]) == 2


def test_requirements_parser_inline_comments():
    content = "requests==2.0  # HTTP library\n"
    result = requirements_txt_parser(content)
    assert result["packages"]["requests"] == "2.0"


def test_requirements_parser_extras():
    content = "requests[security]==2.28.0\n"
    result = requirements_txt_parser(content)
    assert "requests[security]" in result["packages"]


def test_requirements_parser_direct_reference():
    content = "mylib @ git+https://github.com/user/repo.git\n"
    result = requirements_txt_parser(content)
    assert any("mylib" in k for k in result["packages"])


@pytest.mark.parametrize(
    "content, match",
    [
        ('requests==2.0; python_version>="3.6"\n', "Environment markers"),
        ("not a valid requirement!!!\n", "Not a valid PEP 508"),
        ("python==3.9\npython==3.10\n", "Multiple Python version"),
    ],
    ids=["env_markers", "invalid_pep508", "multi_python"],
)
def test_requirements_parser_invalid_inputs(content, match):
    with pytest.raises(ParserValueError, match=match):
        requirements_txt_parser(content)


def test_requirements_parser_empty_content():
    result = requirements_txt_parser("")
    assert result["packages"] == {}
    assert result["python"] is None


def test_requirements_parser_rye_lockfile_skip():
    """Rye lockfiles contain '-e file:.' which should be silently skipped."""
    content = "-e file:.\nrequests==2.0\n"
    result = requirements_txt_parser(content)
    assert result["packages"]["requests"] == "2.0"


# ---------------------------------------------------------------------------
# conda_environment_yml_parser
# ---------------------------------------------------------------------------


def test_conda_parser_simple_deps():
    content = "dependencies:\n  - numpy=1.21.2\n  - pandas=1.3.0\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"]["numpy"] == "1.21.2"
    assert result["packages"]["pandas"] == "1.3.0"


def test_conda_parser_python_version():
    content = "dependencies:\n  - python=3.9\n  - numpy\n"
    result = conda_environment_yml_parser(content)
    assert result["python"] == "3.9"
    assert "python" not in result["packages"]
    assert result["packages"]["numpy"] == ""


def test_conda_parser_no_version():
    content = "dependencies:\n  - numpy\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"]["numpy"] == ""


def test_conda_parser_comments_skipped():
    content = "# env file\ndependencies:\n  # a comment\n  - numpy=1.0\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"]["numpy"] == "1.0"


def test_conda_parser_subsection_rejected():
    content = "dependencies:\n  - pip:\n    - requests\n"
    with pytest.raises(ParserValueError, match="Unsupported subsection"):
        conda_environment_yml_parser(content)


def test_conda_parser_inline_comments():
    content = "dependencies:\n  - numpy=1.0  # math lib\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"]["numpy"] == "1.0"


def test_conda_parser_empty_deps():
    content = "name: test\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"] == {}
    assert result["python"] is None


def test_conda_parser_double_equals():
    content = "dependencies:\n  - numpy==1.21.2\n"
    result = conda_environment_yml_parser(content)
    assert result["packages"]["numpy"] == "1.21.2"
