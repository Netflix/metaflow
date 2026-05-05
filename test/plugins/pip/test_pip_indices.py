from unittest.mock import MagicMock

from metaflow.plugins.pypi.pip import Pip


def _make_pip():
    """Create a bare Pip instance for testing."""
    pip = object.__new__(Pip)
    return pip


def test_multiple_extra_index_urls_literal_newline(monkeypatch):
    """Regression test: pip config list separates multiple URLs with literal \\n."""
    pip = _make_pip()
    config_output = (
        "global.index-url='https://pypi.org/simple'\n"
        r"global.extra-index-url='https://extra1.example.com/simple'\n'https://extra2.example.com/simple'"
    )

    # Use monkeypatch instead of unittest.mock.patch
    mock_call = MagicMock(return_value=config_output)
    monkeypatch.setattr(pip, "_call", mock_call)

    # Execute
    index, extras = pip.indices("dummy")

    # Assert
    assert index == "https://pypi.org/simple"
    assert extras == [
        "https://extra1.example.com/simple",
        "https://extra2.example.com/simple",
    ]
