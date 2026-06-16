import os

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DEFAULT_SECRETS_ROLE
from metaflow.plugins.secrets.secrets_spec import SecretSpec
from metaflow.plugins.secrets.utils import (
    get_secrets_backend_provider,
    validate_env_vars,
    validate_env_vars_across_secrets,
    validate_env_vars_vs_existing_env,
)
from metaflow.unbounded_foreach import UBF_TASK


class SecretsDecorator(StepDecorator):
    """
    Specifies secrets to be retrieved and injected as environment variables prior to
    the execution of a step.

    Parameters
    ----------
    sources : List[Union[str, Dict[str, Any]]], default: []
        List of secret specs, defining how the secrets are to be retrieved
    role : str, optional, default: None
        Role to use for fetching secrets
    allow_override : bool, optional, default: False
        Toggle whether secrets can replace existing environment variables.
    """

    name = "secrets"
    defaults = {"sources": [], "role": None, "allow_override": False}

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
        if not self.attributes["allow_override"]:
            validate_env_vars_vs_existing_env(all_secrets_env_vars)

        # By this point
        # all_secrets_env_vars contains a list of dictionaries... env maps.
        # - env maps must be disjoint from each other
        # - env maps must be disjoint from existing current process os.environ

        for secrets_env_vars in all_secrets_env_vars:
            os.environ.update(secrets_env_vars[1].items())
