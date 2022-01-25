"""Astroid hooks for unittest module"""
from metaflow._vendor.astroid.brain.helpers import register_module_extender
from metaflow._vendor.astroid.builder import parse
from metaflow._vendor.astroid.const import PY38_PLUS
from metaflow._vendor.astroid.manager import AstroidManager


def IsolatedAsyncioTestCaseImport():
    """
    In the unittest package, the IsolatedAsyncioTestCase class is imported lazily, i.e only
    when the __getattr__ method of the unittest module is called with 'IsolatedAsyncioTestCase' as
    argument. Thus the IsolatedAsyncioTestCase is not imported statically (during import time).
    This function mocks a classical static import of the IsolatedAsyncioTestCase.

    (see https://github.com/PyCQA/pylint/issues/4060)
    """
    return parse(
        """
    from .async_case import IsolatedAsyncioTestCase
    """
    )


if PY38_PLUS:
    register_module_extender(
        AstroidManager(), "unittest", IsolatedAsyncioTestCaseImport
    )
