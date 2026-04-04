"""
Unit tests for CondaStepDecorator and CondaFlowDecorator.

Pure logic tests — no conda/micromamba installation needed.
"""

from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator


class TestCondaStepDecorator:
    def test_default_attributes(self):
        deco = CondaStepDecorator()
        deco.init()
        assert deco.attributes["packages"] == {}
        assert deco.attributes["python"] is None
        assert not deco.attributes["disabled"]

    def test_user_defined_python(self):
        deco = CondaStepDecorator(attributes={"python": "3.9"})
        deco.init()
        assert deco.is_attribute_user_defined("python")
        assert not deco.is_attribute_user_defined("packages")

    def test_user_defined_packages(self):
        deco = CondaStepDecorator(attributes={"packages": {"numpy": "1.21"}})
        deco.init()
        assert deco.is_attribute_user_defined("packages")
        assert not deco.is_attribute_user_defined("python")

    def test_libraries_backward_compat(self):
        """Legacy 'libraries' attribute should be merged into 'packages'."""
        deco = CondaStepDecorator(attributes={"libraries": {"scipy": "1.7"}})
        deco.init()
        assert deco.is_attribute_user_defined("packages")
        assert deco.is_attribute_user_defined("libraries")
        assert deco.attributes["packages"] == {"scipy": "1.7"}

    def test_disabled_flag(self):
        deco = CondaStepDecorator(attributes={"disabled": True})
        deco.init()
        assert deco.attributes["disabled"] is True

    def test_packages_and_libraries_merged(self):
        """When both packages and libraries are set, they should merge."""
        deco = CondaStepDecorator(
            attributes={"packages": {"numpy": "1.21"}, "libraries": {"scipy": "1.7"}}
        )
        deco.init()
        # libraries should be merged into packages
        assert "numpy" in deco.attributes["packages"]
        assert "scipy" in deco.attributes["packages"]
