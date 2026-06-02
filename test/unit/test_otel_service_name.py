import importlib

import pytest

# Env vars that influence how OTEL_SERVICE_NAME is resolved. Cleared before each
# case so tests stay independent of the surrounding environment.
SERVICE_NAME_ENV_VARS = ("OTEL_SERVICE_NAME", "METAFLOW_OTEL_SERVICE_NAME")


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
        # Neither var set: backward-compatible default.
        ({}, "metaflow"),
        # Standard OTel env var is inherited.
        ({"OTEL_SERVICE_NAME": "my-app"}, "my-app"),
        # Metaflow-specific override takes precedence over the standard var.
        (
            {"OTEL_SERVICE_NAME": "my-app", "METAFLOW_OTEL_SERVICE_NAME": "metaflow"},
            "metaflow",
        ),
        # Metaflow-specific override works on its own.
        ({"METAFLOW_OTEL_SERVICE_NAME": "pinned"}, "pinned"),
    ],
)
def test_otel_service_name_resolution(monkeypatch, env, expected):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert _resolve_service_name() == expected
