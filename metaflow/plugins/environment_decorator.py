import os

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator


class EnvironmentDecorator(StepDecorator):
    """
    Step decorator to add or update environment variables prior to the execution of your step.

    The environment variables set with this decorator will be present during the execution of the
    step.

    To use, annotate your step as follows:
    ```
    @environment(vars={'MY_ENV': 'value'})
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    vars : Dict
        Dictionary of environment variables to add/update prior to executing your step.
    kubernetes_vars: Dict
        Only used in KFP plugin.
        see https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information

        pod_name_var = V1EnvVar(
            name="MY_POD_NAME",
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(field_path="metadata.name")
            ),
        )
        @environment(kubernetes_vars=[pod_name_var])
        @step
        def my_step(self):
            ...
    """
    name = 'environment'
    defaults = {'vars': {}, 'kubernetes_vars': None}

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        cli_args.env.update(self.attributes['vars'].items())