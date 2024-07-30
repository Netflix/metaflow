import os
import re

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DEFAULT_SECRETS_ROLE
from metaflow.unbounded_foreach import UBF_TASK

from typing import Any, Dict, List, Union

DISALLOWED_SECRETS_ENV_VAR_PREFIXES = ["METAFLOW_"]


def get_default_secrets_backend_type():
    from metaflow.metaflow_config import DEFAULT_SECRETS_BACKEND_TYPE

    if DEFAULT_SECRETS_BACKEND_TYPE is None:
        raise MetaflowException(
            "No default secrets backend type configured, but needed by @secrets. "
            "Set METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE."
        )
    return DEFAULT_SECRETS_BACKEND_TYPE


class SecretSpec:
    def __init__(self, secrets_backend_type, secret_id, options={}, role=None):
        self._secrets_backend_type = secrets_backend_type
        self._secret_id = secret_id
        self._options = options
        self._role = role

    @property
    def secrets_backend_type(self):
        return self._secrets_backend_type

    @property
    def secret_id(self):
        return self._secret_id

    @property
    def options(self):
        return self._options

    @property
    def role(self):
        return self._role

    def to_json(self):
        """Mainly used for testing... not the same as the input dict in secret_spec_from_dict()!"""
        return {
            "secrets_backend_type": self.secrets_backend_type,
            "secret_id": self.secret_id,
            "options": self.options,
            "role": self.role,
        }

    def __str__(self):
        return "%s (%s)" % (self._secret_id, self._secrets_backend_type)

    @staticmethod
    def secret_spec_from_str(secret_spec_str, role):
        # "." may be used in secret_id one day (provider specific). HOWEVER, it provides the best UX for
        # non-conflicting cases (i.e. for secret ids that don't contain "."). This is true for all AWS
        # Secrets Manager secrets.
        #
        # So we skew heavily optimize for best upfront UX for the present (1/2023).
        #
        # If/when a certain secret backend supports "." secret names, we can figure out a solution at that time.
        # At a minimum, dictionary style secret spec may be used with no code changes (see secret_spec_from_dict()).
        # Other options could be:
        #  - accept and document that "." secret_ids don't work in Metaflow (across all possible providers)
        #  - add a Metaflow config variable that specifies the separator (default ".")
        #  - smarter spec parsing, that errors on secrets that look ambiguous. "aws-secrets-manager.XYZ" could mean:
        #    + secret_id "XYZ" in aws-secrets-manager backend, OR
        #    + secret_id "aws-secrets-manager.XYZ" default backend (if it is defined).
        #    + in this case, user can simply set "azure-key-vault.aws-secrets-manager.XYZ" instead!
        parts = secret_spec_str.split(".", maxsplit=1)
        if len(parts) == 1:
            secrets_backend_type = get_default_secrets_backend_type()
            secret_id = parts[0]
        else:
            secrets_backend_type = parts[0]
            secret_id = parts[1]
        return SecretSpec(
            secrets_backend_type, secret_id=secret_id, options={}, role=role
        )

    @staticmethod
    def secret_spec_from_dict(secret_spec_dict, role):
        if "type" not in secret_spec_dict:
            secrets_backend_type = get_default_secrets_backend_type()
        else:
            secrets_backend_type = secret_spec_dict["type"]
            if not isinstance(secrets_backend_type, str):
                raise MetaflowException(
                    "Bad @secrets specification - 'type' must be a string - found %s"
                    % type(secrets_backend_type)
                )
        secret_id = secret_spec_dict.get("id")
        if not isinstance(secret_id, str):
            raise MetaflowException(
                "Bad @secrets specification - 'id' must be a string - found %s"
                % type(secret_id)
            )
        options = secret_spec_dict.get("options", {})
        if not isinstance(options, dict):
            raise MetaflowException(
                "Bad @secrets specification - 'option' must be a dict - found %s"
                % type(options)
            )
        role_for_source = secret_spec_dict.get("role", None)
        if role_for_source is not None:
            if not isinstance(role_for_source, str):
                raise MetaflowException(
                    "Bad @secrets specification - 'role' must be a str - found %s"
                    % type(role_for_source)
                )
            role = role_for_source
        return SecretSpec(
            secrets_backend_type, secret_id=secret_id, options=options, role=role
        )


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


class SecretsDecorator(StepDecorator):
    """
    Specifies secrets to be retrieved and injected as environment variables prior to
    the execution of a step.

    Parameters
    ----------
    sources : List[Union[str, Dict[str, Any]]], default: []
        List of secret specs, defining how the secrets are to be retrieved
    """

    name = "secrets"
    defaults = {
        "sources": [],
        "role": None,
    }

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        if (
            ubf_context
            and ubf_context == UBF_TASK
            and os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local"
        ):
            # We will skip the secret injection for "locally" launched UBF_TASK (worker) tasks
            # When we "locally" run @parallel tasks, the control task will create the worker tasks and the environment variables
            # of the control task are inherited by the worker tasks. If we don't skip setting secrets in the worker task then the
            # worker tasks will try to set the environment variables again which will cause a clash with the control task's env vars,
            # causing the @secrets' `task_pre_step` to fail. In remote settings, (e.g. AWS Batch/Kubernetes), the worker task and
            # control task are independently created so there is no chances of an env var clash.
            return
        # List of pairs (secret_spec, env_vars_from_this_spec)
        all_secrets_env_vars = []
        secret_specs = []

        # Role (in terms of RBAC) to use when retrieving secrets.
        # This is a general concept applicable to multiple backends
        # E.g in AWS, this would be an IAM Role ARN.
        #
        # Config precedence (decreasing):
        # - Source level: @secrets(source=[{"role": ...}])
        # - Decorator level: @secrets(role=...)
        # - Metaflow config key DEFAULT_SECRETS_ROLE
        role = self.attributes["role"]
        if role is None:
            role = DEFAULT_SECRETS_ROLE

        for secret_spec_str_or_dict in self.attributes["sources"]:
            if isinstance(secret_spec_str_or_dict, str):
                secret_specs.append(
                    SecretSpec.secret_spec_from_str(secret_spec_str_or_dict, role=role)
                )
            elif isinstance(secret_spec_str_or_dict, dict):
                secret_specs.append(
                    SecretSpec.secret_spec_from_dict(secret_spec_str_or_dict, role=role)
                )
            else:
                raise MetaflowException(
                    "@secrets sources items must be either a string or a dict"
                )

        for secret_spec in secret_specs:
            secrets_backend_provider = get_secrets_backend_provider(
                secret_spec.secrets_backend_type
            )
            try:
                env_vars_for_secret = secrets_backend_provider.get_secret_as_dict(
                    secret_spec.secret_id,
                    options=secret_spec.options,
                    role=secret_spec.role,
                )
            except Exception as e:
                raise MetaflowException(
                    "Failed to retrieve secret '%s': %s" % (secret_spec.secret_id, e)
                )
            try:
                validate_env_vars(env_vars_for_secret)
            except ValueError as e:
                raise MetaflowException(
                    "Invalid env vars from secret %s: %s"
                    % (secret_spec.secret_id, str(e))
                )
            all_secrets_env_vars.append((secret_spec, env_vars_for_secret))

        validate_env_vars_across_secrets(all_secrets_env_vars)
        validate_env_vars_vs_existing_env(all_secrets_env_vars)

        # By this point
        # all_secrets_env_vars contains a list of dictionaries... env maps.
        # - env maps must be disjoint from each other
        # - env maps must be disjoint from existing current process os.environ

        for secrets_env_vars in all_secrets_env_vars:
            os.environ.update(secrets_env_vars[1].items())
