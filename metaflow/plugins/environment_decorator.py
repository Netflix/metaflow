from metaflow.decorators import StepDecorator


class EnvironmentDecorator(StepDecorator):
    """
    Specifies environment variables to be set prior to the execution of a step.

    Parameters
    ----------
    vars : Dict[str, str], default: {}
        Dictionary of environment variables to set.
    """

    name = "environment"
    defaults = {"vars": {}}

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        cli_args.env.update(
            {key: str(value) for key, value in self.attributes["vars"].items()}
        )
