from metaflow.decorators import StepDecorator
from metaflow.unbounded_foreach import UBF_CONTROL

class ParallelDecorator(StepDecorator):
    name = "parallel"
    defaults = {}

    def __init__(self, attributes=None, statically_defined=False):
        super(ParallelDecorator, self).__init__(attributes, statically_defined)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):

        if ubf_context == UBF_CONTROL:
            num_parallel = cli_args.task.ubf_iter.num_parallel
            cli_args.command_options["num-parallel"] = str(num_parallel)
