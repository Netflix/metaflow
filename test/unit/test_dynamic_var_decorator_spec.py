from metaflow import var
from metaflow.decorators import Decorator
from metaflow.dynamic_var import _NO_DEFAULT
from metaflow.plugins.catch_decorator import CatchDecorator
from metaflow.plugins.environment_decorator import EnvironmentDecorator


def _parse_decorator_attrs(decorator):
    spec = decorator.make_decorator_spec()
    _, attr_spec = spec.split(":", 1)
    _, attrs = Decorator.extract_args_kwargs_from_decorator_spec(attr_spec)
    return attrs


def _assert_dynamic_var_spec(value, name, default):
    assert value.var_name == name
    assert value.pertask is True
    assert value.default is not _NO_DEFAULT
    assert value.default == default


def test_legacy_dynamic_var_spec_still_parses():
    _, attrs = Decorator.extract_args_kwargs_from_decorator_spec(
        "var=__dynvar__:catch_name"
    )

    assert attrs["var"].var_name == "catch_name"
    assert attrs["var"].pertask is False
    assert attrs["var"].default is _NO_DEFAULT


def test_top_level_dynamic_var_spec_preserves_pertask_and_default():
    attrs = _parse_decorator_attrs(
        CatchDecorator(
            attributes={
                "var": var("catch_names", pertask=True, default="caught_default"),
                "print_exception": False,
            },
            statically_defined=True,
        )
    )

    _assert_dynamic_var_spec(attrs["var"], "catch_names", "caught_default")


def test_nested_dynamic_var_spec_preserves_pertask_and_default():
    attrs = _parse_decorator_attrs(
        EnvironmentDecorator(
            attributes={
                "vars": {
                    "DYNAMIC_ENV_VALUE": var(
                        "env_values", pertask=True, default="fallback"
                    )
                }
            },
            statically_defined=True,
        )
    )

    _assert_dynamic_var_spec(
        attrs["vars"]["DYNAMIC_ENV_VALUE"], "env_values", "fallback"
    )
