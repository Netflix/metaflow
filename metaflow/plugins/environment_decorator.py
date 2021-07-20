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
    """
    name = 'environment'
    defaults = {'vars': {}}

    def step_init(self, flow, graph, step_name, decorators, environment, datastore, logger):
        self.attributes["vars"] = self.vars_dict().items()

    def vars_dict(self):
        env_vars = self.attributes["vars"]
        env_vars = env_vars.lstrip("{").rstrip("}") # strip brackets
        if isinstance(vars, str):  # env specified using --with will be a string that we must parse
            try:
                vars_dict = {}
                pairs = env_vars.split(",")
                for pair in pairs:
                    k, v = pair.split(":", 1) # split on the first colon only
                    vars_dict[k.strip()] = v.strip()

            except Exception as e:
                raise ValueError(
                    "Encountered a problem parsing stringified @environment vars attribute as a dict: {}".format(e)
                )
            return vars_dict
        else:
            return env_vars

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        cli_args.env.update(self.vars_dict().items())
