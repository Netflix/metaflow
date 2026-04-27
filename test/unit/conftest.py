"""Shared pytest configuration for unit tests."""

import unittest

import pytest

_LEGACY_TESTCASE_FILES = frozenset(
    {
        "test_secrets_decorator.py",
        "test_s3_storage.py",
        "test_system_context.py",
    }
)


def pytest_collection_modifyitems(items):
    for item in items:
        if not isinstance(item, pytest.Function):
            continue
        if item.cls and issubclass(item.cls, unittest.TestCase):
            filename = item.fspath.basename
            if filename not in _LEGACY_TESTCASE_FILES:
                item.add_marker(
                    pytest.mark.xfail(
                        reason=(
                            "unittest.TestCase is not allowed in new tests. "
                            "See CONTRIBUTING.md Test conventions."
                        ),
                        strict=True,
                    )
                )


def pytest_addoption(parser):
    """Add custom command line options shared across unit test suites."""
    parser.addoption(
        "--use-latest",
        action="store_true",
        default=False,
        help="Use latest run of each flow instead of running new ones",
    )
