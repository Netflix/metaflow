"""Shared pytest configuration for plugins tests.

Mirrors the ``--use-latest`` registration in ``test/unit/conftest.py`` so
spin-style conftests that call ``request.config.getoption("--use-latest")``
work when run under ``test/plugins/`` (e.g. when downstream consumers copy
``test/unit/spin/`` into ``test/plugins/spin/spin/``).
"""

import pytest


def pytest_addoption(parser):
    """Add custom command line options shared across plugins test suites."""
    parser.addoption(
        "--use-latest",
        action="store_true",
        default=False,
        help="Use latest run of each flow instead of running new ones",
    )
