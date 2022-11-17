import json
import os

from metaflow.exception import MetaflowException


def init_config():
    # Read configuration from $METAFLOW_HOME/config_<profile>.json.
    home = os.environ.get("METAFLOW_HOME", "~/.metaflowconfig")
    profile = os.environ.get("METAFLOW_PROFILE")
    path_to_config = os.path.join(home, "config.json")
    if profile:
        path_to_config = os.path.join(home, "config_%s.json" % profile)
    path_to_config = os.path.expanduser(path_to_config)
    config = {}
    if os.path.exists(path_to_config):
        with open(path_to_config, encoding="utf-8") as f:
            return json.load(f)
    elif profile:
        raise MetaflowException(
            "Unable to locate METAFLOW_PROFILE '%s' in '%s')" % (profile, home)
        )
    return config


# Initialize defaults required to setup environment variables.
METAFLOW_CONFIG = init_config()

_all_configs = {}


def config_values():
    for name, (value, conv_func) in _all_configs.items():
        if value is not None:
            yield name, conv_func(value)


def from_conf(
    name, default=None, validate_fn=None, ignore_config=False, propagate=True
):
    """
    First try to pull value from environment, then from metaflow config JSON.

    Prior to a value being returned, we will validate using validate_fn (if provided).
    Only non-None values are validated.

    validate_fn should accept (name, value).
    If the value validates, return None, else raise an MetaflowException.
    """
    env_name = "METAFLOW_%s" % name
    value = os.environ.get(
        env_name, default if ignore_config else METAFLOW_CONFIG.get(env_name, default)
    )
    if validate_fn and value is not None:
        validate_fn(env_name, value)
    if default is not None:
        if default == "{}":
            try:
                value = json.loads(value)
                if propagate:
                    _all_configs[env_name] = (value, json.dumps)
                return value
            except json.JSONDecodeError:
                raise ValueError(
                    "Expected a valid JSON for %s, got: %s" % (env_name, value)
                )
        else:
            try:
                value = type(default)(value)
            except ValueError:
                raise ValueError(
                    "Expected a %s for %s, got: %s" % (type(default), env_name, value)
                )
    if propagate:
        _all_configs[env_name] = (value, str)
    return value


def override_value(name, value):
    env_name = "METAFLOW_%s" % name
    # If we override a value, we don't actually need to propagate it as the override
    # will naturally apply there too (same code runs remotely).
    if env_name in _all_configs:
        del _all_configs[env_name]
    return value


def get_validate_choice_fn(choices):
    """Returns a validate_fn for use with from_conf().
    The validate_fn will check a value against a list of allowed choices.
    """

    def _validate_choice(name, value):
        if value not in choices:
            raise MetaflowException(
                "%s must be set to one of %s. Got '%s'." % (name, choices, value)
            )

    return _validate_choice
