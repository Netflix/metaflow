from metaflow.decorators import StepDecorator


class EnvironmentDecorator(StepDecorator):
    """
    Specifies environment variables to be set prior to the execution of a step.

    Parameters
    ----------
    vars : Dict[str, str], default {}
        Dictionary of environment variables to set.
    """

    name = "environment"
    defaults = {"vars": {}}
    allow_multiple = True

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self.logger = logger

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        cli_args.env.update(
            {key: str(value) for key, value in self.attributes["vars"].items()}
        )

    @classmethod
    def merge_vars(cls, environment_decorators):
        """Merge variables from a list of environment decorators and return a new dictionary."""
        dest = {}
        for deco in environment_decorators:
            for key, value in deco.attributes["vars"].items():
                if key in dest and value != dest[key]:
                    deco.logger(
                        "Overwriting value {} for environment variable {} with new value {}".format(
                            dest[key], key, value
                        )
                    )
                dest[key] = value
            dest.update(deco.attributes["vars"])
        return dest
