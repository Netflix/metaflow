from unittest.mock import patch

from metaflow.plugins.pypi.pip import Pip


def _make_pip():
    pip = object.__new__(Pip)
    return pip


def test_multiple_extra_index_urls_literal_newline():
    """Regression test: pip config list separates multiple URLs with literal \\n."""
    pip = _make_pip()
    config_output = (
        "global.index-url='https://pypi.org/simple'\n"
        r"global.extra-index-url='https://extra1.example.com/simple'\n'https://extra2.example.com/simple'"
    )
    with patch.object(pip, "_call", return_value=config_output):
        index, extras = pip.indices("dummy")
    assert index == "https://pypi.org/simple"
    assert extras == [
        "https://extra1.example.com/simple",
        "https://extra2.example.com/simple",
    ]
