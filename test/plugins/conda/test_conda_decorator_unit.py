"""
Unit tests for CondaStepDecorator and CondaFlowDecorator.

Pure logic tests -- no conda/micromamba installation needed.
"""

import pytest

from metaflow.plugins.pypi.conda_decorator import CondaFlowDecorator, CondaStepDecorator

DECORATOR_CLASSES = (CondaStepDecorator, CondaFlowDecorator)


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
def test_default_attributes(decorator_cls):
    deco = decorator_cls()
    deco.init()
    assert deco.attributes["packages"] == {}
    assert deco.attributes["python"] is None
    assert not deco.attributes["disabled"]


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
def test_user_defined_python_is_tracked(decorator_cls):
    deco = decorator_cls(attributes={"python": "3.9"})
    deco.init()
    assert deco.is_attribute_user_defined("python")
    assert not deco.is_attribute_user_defined("packages")
    assert deco.attributes["python"] == "3.9"


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
def test_user_defined_packages_are_tracked(decorator_cls):
    deco = decorator_cls(attributes={"packages": {"numpy": "1.21"}})
    deco.init()
    assert deco.is_attribute_user_defined("packages")
    assert not deco.is_attribute_user_defined("python")
    assert deco.attributes["packages"] == {"numpy": "1.21"}


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
def test_libraries_are_merged_into_packages_and_tracked(decorator_cls):
    deco = decorator_cls(attributes={"libraries": {"scipy": "1.7"}})
    deco.init()
    assert deco.is_attribute_user_defined("packages")
    assert deco.is_attribute_user_defined("libraries")
    assert deco.attributes["packages"] == {"scipy": "1.7"}
    assert deco.attributes["libraries"] == {}


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
def test_disabled_flag_is_preserved(decorator_cls):
    deco = decorator_cls(attributes={"disabled": True})
    deco.init()
    assert deco.attributes["disabled"] is True


@pytest.mark.parametrize("decorator_cls", DECORATOR_CLASSES)
@pytest.mark.parametrize(
    "attributes, expected_packages",
    [
        (
            {"packages": {"numpy": "1.21"}, "libraries": {"scipy": "1.7"}},
            {"numpy": "1.21", "scipy": "1.7"},
        ),
        (
            {"packages": {"numpy": "2.4.3"}, "libraries": {"numpy": "2.4.1"}},
            {"numpy": "2.4.3"},
        ),
    ],
)
def test_packages_and_libraries_merge_with_packages_precedence(
    decorator_cls, attributes, expected_packages
):
    deco = decorator_cls(attributes=attributes)
    deco.init()
    assert deco.attributes["packages"] == expected_packages
    assert deco.attributes["libraries"] == {}
