from metaflow.plugins.pypi.pip import Pip


def _make_pip():
    """Create a bare Pip instance for testing."""
    pip = object.__new__(Pip)
    return pip


def test_multiple_extra_index_urls_literal_newline(mocker):
    """Regression test: pip config list separates multiple URLs with literal \\n."""
    pip = _make_pip()
    config_output = (
        "global.index-url='https://pypi.org/simple'\n"
        r"global.extra-index-url='https://extra1.example.com/simple'\n'https://extra2.example.com/simple'"
    )

    mocker.patch.object(pip, "_call", return_value=config_output)

    index, extras = pip.indices("dummy")

    assert index == "https://pypi.org/simple"
    assert extras == [
        "https://extra1.example.com/simple",
        "https://extra2.example.com/simple",
    ]


def test_more_than_nine_extra_index_urls(mocker):
    """Every URL is split out, not just the first nine."""
    pip = _make_pip()
    urls = ["https://extra%d.example.com/simple" % i for i in range(12)]
    config_output = (
        "global.index-url='https://pypi.org/simple'\n"
        "global.extra-index-url=" + r"\n".join("'%s'" % u for u in urls)
    )

    mocker.patch.object(pip, "_call", return_value=config_output)

    index, extras = pip.indices("dummy")

    assert index == "https://pypi.org/simple"
    assert extras == urls
