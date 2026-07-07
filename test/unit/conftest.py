"""Shared pytest configuration for unit tests."""

import pytest


def pytest_addoption(parser):
    """Add custom command line options shared across unit test suites."""
    parser.addoption(
        "--use-latest",
        action="store_true",
        default=False,
        help="Use latest run of each flow instead of running new ones",
    )
