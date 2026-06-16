import os
import re
from metaflow.exception import MetaflowException

DISALLOWED_SECRETS_ENV_VAR_PREFIXES = ["METAFLOW_"]


def get_default_secrets_backend_type():
    from metaflow.metaflow_config import DEFAULT_SECRETS_BACKEND_TYPE

    if DEFAULT_SECRETS_BACKEND_TYPE is None:
        raise MetaflowException(
            "No default secrets backend type configured, but needed by @secrets. "
            "Set METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE."
        )
    return DEFAULT_SECRETS_BACKEND_TYPE


def validate_env_vars_across_secrets(all_secrets_env_vars):
    vars_injected_by = {}
    for secret_spec, env_vars in all_secrets_env_vars:
        for k in env_vars:
            if k in vars_injected_by:
                raise MetaflowException(
                    "Secret '%s' will inject '%s' as env var, and it is also added by '%s'"
                    % (secret_spec, k, vars_injected_by[k])
                )
            vars_injected_by[k] = secret_spec


def validate_env_vars_vs_existing_env(all_secrets_env_vars):
    for secret_spec, env_vars in all_secrets_env_vars:
        for k in env_vars:
            if k in os.environ:
                raise MetaflowException(
                    "Secret '%s' will inject '%s' as env var, but it already exists in env"
                    % (secret_spec, k)
                )


def validate_env_vars(env_vars):
    for k, v in env_vars.items():
        if not isinstance(k, str):
            raise MetaflowException("Found non string key %s (%s)" % (str(k), type(k)))
        if not isinstance(v, str):
            raise MetaflowException(
                "Found non string value %s (%s)" % (str(v), type(v))
            )
        if not re.fullmatch("[a-zA-Z_][a-zA-Z0-9_]*", k):
            raise MetaflowException("Found invalid env var name '%s'." % k)
        for disallowed_prefix in DISALLOWED_SECRETS_ENV_VAR_PREFIXES:
            if k.startswith(disallowed_prefix):
                raise MetaflowException(
                    "Found disallowed env var name '%s' (starts with '%s')."
                    % (k, disallowed_prefix)
                )


def get_secrets_backend_provider(secrets_backend_type):
    from metaflow.plugins import SECRETS_PROVIDERS

    try:
        provider_cls = [
            pc for pc in SECRETS_PROVIDERS if pc.TYPE == secrets_backend_type
        ][0]
        return provider_cls()
    except IndexError:
        raise MetaflowException(
            "Unknown secrets backend type %s (available types: %s)"
            % (
                secrets_backend_type,
                ", ".join(pc.TYPE for pc in SECRETS_PROVIDERS if pc.TYPE != "inline"),
            )
        )
