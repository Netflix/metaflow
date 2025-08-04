from metaflow.plugins.pypi.pypi_decorator import PyPIStepDecorator


def test_decorator_custom_attributes():
    deco = PyPIStepDecorator(attributes={"python": "3.9"})
    deco.init()
    assert deco.is_attribute_user_defined(
        "python"
    ), "python is supposed to be an user-defined attribute"
    assert not deco.is_attribute_user_defined(
        "packages"
    ), "packages is supposed to be default"
