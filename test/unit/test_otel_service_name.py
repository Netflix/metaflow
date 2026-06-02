import importlib

import pytest

# Env vars that influence how OTEL_SERVICE_NAME is resolved. Cleared before each
# case so tests stay independent of the surrounding environment.
SERVICE_NAME_ENV_VARS = (
    "OTEL_SERVICE_NAME",
    "METAFLOW_OTEL_SERVICE_NAME",
    "METAFLOW_OTEL_INHERIT_SERVICE_NAME",
)


def _resolve_service_name():
    """Reload metaflow_config and return the freshly resolved OTEL_SERVICE_NAME.

    The value is computed at import time from the environment, so each case
    reloads the module after monkeypatching the relevant env vars.
    """
    import metaflow.metaflow_config as metaflow_config

    importlib.reload(metaflow_config)
    return metaflow_config.OTEL_SERVICE_NAME


@pytest.fixture(autouse=True)
def clear_service_name_env(monkeypatch):
    for var in SERVICE_NAME_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(
    "env, expected",
    [
        # Nothing set: historical default, unchanged.
        ({}, "metaflow"),
        # A surrounding OTEL_SERVICE_NAME is IGNORED unless inheritance is opted
        # in, so upgrades never silently relabel existing users' spans.
        ({"OTEL_SERVICE_NAME": "from-otel"}, "metaflow"),
        # Inheritance opted in: the standard OTEL_SERVICE_NAME is honored.
        (
            {
                "METAFLOW_OTEL_INHERIT_SERVICE_NAME": "1",
                "OTEL_SERVICE_NAME": "from-otel",
            },
            "from-otel",
        ),
        # Inheritance opted in but OTEL_SERVICE_NAME unset: falls back to default.
        ({"METAFLOW_OTEL_INHERIT_SERVICE_NAME": "1"}, "metaflow"),
        # Explicit METAFLOW_OTEL_SERVICE_NAME always wins, even over an inherited
        # standard var. Distinct values so the precedence is genuinely exercised.
        (
            {
                "METAFLOW_OTEL_INHERIT_SERVICE_NAME": "1",
                "OTEL_SERVICE_NAME": "from-otel",
                "METAFLOW_OTEL_SERVICE_NAME": "from-metaflow",
            },
            "from-metaflow",
        ),
        # Explicit override works with inheritance off too.
        ({"METAFLOW_OTEL_SERVICE_NAME": "pinned"}, "pinned"),
    ],
)
def test_otel_service_name_resolution(monkeypatch, env, expected):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert _resolve_service_name() == expected
