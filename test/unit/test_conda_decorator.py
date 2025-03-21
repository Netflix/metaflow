from metaflow.plugins.pypi.conda_decorator import CondaStepDecorator


def test_decorator_custom_attributes():
    deco = CondaStepDecorator(attributes={"python": "3.9"})
    deco.init()
    assert deco.is_attribute_user_defined(
        "python"
    ), "python is supposed to be an user-defined attribute"
    assert not deco.is_attribute_user_defined(
        "packages"
    ), "packages is supposed to be default"
    assert not deco.is_attribute_user_defined(
        "libraries"
    ), "libraries is supposed to be default"


def test_decorator_custom_attributes_with_backward_compatibility():
    deco = CondaStepDecorator(attributes={"libraries": {"a": "test"}})
    deco.init()
    assert not deco.is_attribute_user_defined(
        "python"
    ), "python is supposed to be default"
    assert deco.is_attribute_user_defined(
        "packages"
    ), "packages is supposed to be user-defined"
    assert deco.is_attribute_user_defined(
        "libraries"
    ), "libraries is supposed to be user-defined"
