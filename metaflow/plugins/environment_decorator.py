import os

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


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
    """
    name = 'environment'
    defaults = {'vars': {}}

    def vars_dict(self):
        env_vars = self.attributes["vars"]
        if isinstance(vars, str):  # env specified using --with will be a string that we must parse
            try:
                vars_dict = {}
                pairs = env_vars.split(",")
                for pair in pairs:
                    k, v = pair.split(":", 1) # split on the first colon only
                    vars_dict[k] = v

            except Exception as e:
                raise ValueError(
                    "Encountered a problem parsing stringified @environment vars attribute as a dict: {}".format(e)
                )
            return vars_dict
        else:
            return env_vars

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        cli_args.env.update(self.vars_dict().items())
