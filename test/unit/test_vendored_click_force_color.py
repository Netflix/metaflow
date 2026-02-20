"""
Vendored Click FORCE_COLOR patch
=================================
The vendored Click 7.1.2 (_vendor/click/_compat.py) has a custom patch to
its isatty() function that respects the FORCE_COLOR env var.  Upstream Click
does not implement this — see https://github.com/pallets/click/issues/3022.

If Click is ever re-vendored or upgraded, this patch MUST be re-applied.
These tests will fail as a reminder.
"""

import pytest
from io import StringIO

from metaflow._vendor.click._compat import isatty, should_strip_ansi


@pytest.fixture
def pipe():
    return StringIO()


@pytest.mark.parametrize(
    "force_color, expected_isatty",
    [
        ("1", True),  # FORCE_COLOR=1 forces color
        ("true", True),  # any truthy non-"0" value forces color
        ("0", False),  # FORCE_COLOR=0 does not force
        ("", False),  # FORCE_COLOR="" treated as unset
    ],
)
def test_isatty_force_color(monkeypatch, pipe, force_color, expected_isatty):
    monkeypatch.setenv("FORCE_COLOR", force_color)
    assert isatty(pipe) is expected_isatty


def test_isatty_default_no_force_color(monkeypatch, pipe):
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    assert isatty(pipe) is False


def test_should_strip_ansi_force_color(monkeypatch, pipe):
    monkeypatch.setenv("FORCE_COLOR", "1")
    assert should_strip_ansi(pipe) is False


def test_should_strip_ansi_default(monkeypatch, pipe):
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    assert should_strip_ansi(pipe) is True
