from metaflow.decorators import StepDecorator
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.exception import MetaflowException
import os
import sys


class ParallelDecorator(StepDecorator):
    name = "parallel"
    defaults = {}
    IS_PARALLEL = True

    def __init__(self, attributes=None, statically_defined=False):
        super(ParallelDecorator, self).__init__(attributes, statically_defined)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):

        if ubf_context == UBF_CONTROL:
            num_parallel = cli_args.task.ubf_iter.num_parallel
            cli_args.command_options["num-parallel"] = str(num_parallel)

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self.environment = environment

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        def _step_func_with_setup():
            self.setup_distributed_env(flow)
            step_func()

        if (
            ubf_context == UBF_CONTROL
            and os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local"
        ):
            from functools import partial

            env_to_use = getattr(self.environment, "base_env", self.environment)

            return partial(
                _local_multinode_control_task_step_func,
                flow,
                env_to_use,
                _step_func_with_setup,
                retry_count,
            )
        else:
            return _step_func_with_setup

    def setup_distributed_env(self, flow):
        # Overridden by subclasses to set up particular framework's environment.
        pass


def _local_multinode_control_task_step_func(flow, env_to_use, step_func, retry_count):
    """
    Used as multinode UBF control task when run in local mode.
    """
    from metaflow import current
    from metaflow.cli_args import cli_args
    from metaflow.unbounded_foreach import UBF_TASK
    import subprocess

    assert flow._unbounded_foreach
    foreach_iter = flow._parallel_ubf_iter
    if foreach_iter.__class__.__name__ != "ParallelUBF":
        raise MetaflowException(
            "Expected ParallelUBFIter iterator object, got:"
            + foreach_iter.__class__.__name__
        )

    num_parallel = foreach_iter.num_parallel
    os.environ["MF_PARALLEL_NUM_NODES"] = str(num_parallel)
    os.environ["MF_PARALLEL_MAIN_IP"] = "127.0.0.1"

    run_id = current.run_id
    step_name = current.step_name
    control_task_id = current.task_id

    (_, split_step_name, split_task_id) = control_task_id.split("-")[1:]
    # UBF handling for multinode case
    top_task_id = control_task_id.replace("control-", "")  # chop "-0"
    mapper_task_ids = [control_task_id]
    # If we are running inside Conda, we use the base executable FIRST;
    # the conda environment will then be used when runtime_step_cli is
    # called. This is so that it can properly set up all the metaflow
    # aliases needed.
    executable = env_to_use.executable(step_name)
    script = sys.argv[0]

    # start workers
    subprocesses = []
    for node_index in range(1, num_parallel):
        task_id = "%s_node_%d" % (top_task_id, node_index)
        mapper_task_ids.append(task_id)
        os.environ["MF_PARALLEL_NODE_INDEX"] = str(node_index)
        input_paths = "%s/%s/%s" % (run_id, split_step_name, split_task_id)
        # Override specific `step` kwargs.
        kwargs = cli_args.step_kwargs
        kwargs["split_index"] = str(node_index)
        kwargs["run_id"] = run_id
        kwargs["task_id"] = task_id
        kwargs["input_paths"] = input_paths
        kwargs["ubf_context"] = UBF_TASK
        kwargs["retry_count"] = str(retry_count)

        cmd = cli_args.step_command(executable, script, step_name, step_kwargs=kwargs)
        p = subprocess.Popen(cmd)
        subprocesses.append(p)

    flow._control_mapper_tasks = [
        "%s/%s/%s" % (run_id, step_name, mapper_task_id)
        for mapper_task_id in mapper_task_ids
    ]
    flow._control_task_is_mapper_zero = True

    # run the step function ourselves
    os.environ["MF_PARALLEL_NODE_INDEX"] = "0"
    step_func()

    # join the subprocesses
    for p in subprocesses:
        p.wait()
        if p.returncode:
            raise Exception("Subprocess failed, return code {}".format(p.returncode))
