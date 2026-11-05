"""Tests for DeployTimeField._check_type.

@trigger / @trigger_on_finish wrap a callable attribute in a DeployTimeField
whose parameter_type is a *list* of acceptable types, e.g. [str, dict] for
`event=` and [list, dict, tuple] for `event={"parameters": ...}`. When the
user's function returns something outside that set, the mismatch message has
to be buildable -- otherwise the user never sees why their function was
rejected.
"""

import pytest

import metaflow.parameters as parameters
from metaflow.exception import ParameterFieldTypeMismatch
from metaflow.parameters import DeployTimeField, ParameterContext


@pytest.fixture(autouse=True)
def parameter_context(monkeypatch):
    """DeployTimeField.__call__ curries the module-level context onto the
    user function; the CLI normally installs it via set_parameter_context()."""
    monkeypatch.setattr(
        parameters,
        "context_proto",
        ParameterContext(
            flow_name="TriggerFlow",
            user_name="tester",
            parameter_name=None,
            logger=lambda *a, **kw: None,
            ds_type="local",
            configs=None,
        ),
    )


def _field(parameter_type, value):
    return DeployTimeField(
        "event", parameter_type, None, lambda ctx, deploy_time: value, False
    )


@pytest.mark.parametrize(
    "parameter_type, expected_names",
    [
        ([str, dict], ["str", "dict"]),
        ([list, dict, tuple], ["list", "dict", "tuple"]),
    ],
    ids=["trigger_event", "trigger_event_parameters"],
)
def test_list_of_types_reports_the_mismatch(parameter_type, expected_names):
    field = _field(parameter_type, 42)
    with pytest.raises(ParameterFieldTypeMismatch) as excinfo:
        field(deploy_time=True)
    message = str(excinfo.value)
    for name in expected_names:
        assert name in message


@pytest.mark.parametrize(
    "parameter_type, value",
    [
        ([str, dict], "an-event-name"),
        ([str, dict], {"name": "an-event-name"}),
        ([list, dict, tuple], ["a", "b"]),
        ([list, dict, tuple], ("a", "b")),
    ],
)
def test_list_of_types_accepts_any_listed_type(parameter_type, value):
    assert _field(parameter_type, value)(deploy_time=True) == value


def test_single_type_mismatch_still_reports(parameter_type=int):
    field = _field(parameter_type, "not-an-int")
    with pytest.raises(ParameterFieldTypeMismatch) as excinfo:
        field(deploy_time=True)
    assert "Expected a int." in str(excinfo.value)


def test_single_type_match_still_passes():
    assert _field(int, 7)(deploy_time=True) == 7
