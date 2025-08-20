from collections import namedtuple
from metaflow.decorators import StepDecorator
from metaflow.unbounded_foreach import UBF_CONTROL, CONTROL_TASK_TAG
from metaflow.exception import MetaflowException
from metaflow.metadata_provider import MetaDatum
from metaflow.metaflow_current import current, Parallel
import os
import sys


class ParallelDecorator(StepDecorator):
    """
    MF Add To Current
    -----------------
    parallel -> metaflow.metaflow_current.Parallel
        Returns a namedtuple with relevant information about the parallel task.

        @@ Returns
        -------
        Parallel
            `namedtuple` with the following fields:
                - main_ip (`str`)
                    The IP address of the control task.
                - num_nodes (`int`)
                    The total number of tasks created by @parallel
                - node_index (`int`)
                    The index of the current task in all the @parallel tasks.
                - control_task_id (`Optional[str]`)
                    The task ID of the control task. Available to all tasks.

    is_parallel -> bool
        True if the current step is a @parallel step.
    """

    name = "parallel"
    defaults = {}
    IS_PARALLEL = True

    def __init__(self, attributes=None, statically_defined=False, inserted_by=None):
        super(ParallelDecorator, self).__init__(
            attributes, statically_defined, inserted_by
        )

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        if ubf_context == UBF_CONTROL:
            num_parallel = cli_args.task.ubf_iter.num_parallel
            cli_args.command_options["num-parallel"] = str(num_parallel)
            if os.environ.get("METAFLOW_RUNTIME_ENVIRONMENT", "local") == "local":
                cli_args.command_options["split_index"] = "0"

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        # TODO: This can be supported in the future, but for the time being we disable the transition as it leads to
        # a UBF exception during runtime when the actual parallel-join step is conditional (switching between different join implementations from the @parallel step).
        if graph[step_name].type == "split-switch":
            raise MetaflowException(
                "A @parallel step can not be a conditional switch step. Please add a join step after *%s*"
                % step_name
            )
        self.environment = environment
        # Previously, the `parallel` property was a hardcoded, static property within `current`.
        # Whenever `current.parallel` was called, it returned a named tuple with values coming from
        # environment variables, loaded dynamically at runtime.
        # Now, many of these environment variables are set by compute-related decorators in `task_pre_step`.
        # This necessitates ensuring the correct ordering of the `parallel` and compute decorators if we want to
        # statically set the namedtuple via `current._update_env` in `task_pre_step`. Hence we avoid using
        # `current._update_env` since:
        # - it will set a static named tuple, resolving environment variables only once (at the time of calling `current._update_env`).
        # - we cannot guarantee the order of calling the decorator's `task_pre_step` (calling `current._update_env` may not set
        #   the named tuple with the correct values).
        # Therefore, we explicitly set the property in `step_init` to ensure the property can resolve the appropriate values in the named tuple
        # when accessed at runtime.
        setattr(
            current.__class__,
            "parallel",
            property(
                fget=lambda _: Parallel(
                    main_ip=os.environ.get("MF_PARALLEL_MAIN_IP", "127.0.0.1"),
                    num_nodes=int(os.environ.get("MF_PARALLEL_NUM_NODES", "1")),
                    node_index=int(os.environ.get("MF_PARALLEL_NODE_INDEX", "0")),
                    control_task_id=os.environ.get("MF_PARALLEL_CONTROL_TASK_ID", None),
                )
            ),
        )

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
        from metaflow import current

        # Set `is_parallel` to `True` in `current` just like we
        # with `is_production` in the project decorator.
        current._update_env(
            {
                "is_parallel": True,
            }
        )

        self.input_paths = [obj.pathspec for obj in inputs]
        task_metadata_list = [
            MetaDatum(
                field="parallel-world-size",
                value=flow._parallel_ubf_iter.num_parallel,
                type="parallel-world-size",
                tags=["attempt_id:{0}".format(0)],
            )
        ]
        if ubf_context == UBF_CONTROL:
            # A Task's tags are now those of its ancestral Run, so we are not able
            # to rely on a task's tags to indicate the presence of a control task
            # so, on top of adding the tags above, we also add a task metadata
            # entry indicating that this is a "control task".
            #
            # Here we will also add a task metadata entry to indicate "control
            # task". Within the metaflow repo, the only dependency of such a
            # "control task" indicator is in the integration test suite (see
            # Step.control_tasks() in client API).
            task_metadata_list += [
                MetaDatum(
                    field="internal_task_type",
                    value=CONTROL_TASK_TAG,
                    type="internal_task_type",
                    tags=["attempt_id:{0}".format(0)],
                )
            ]
            flow._control_task_is_mapper_zero = True

        metadata.register_metadata(run_id, step_name, task_id, task_metadata_list)

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
                ",".join(self.input_paths),
            )
        else:
            return _step_func_with_setup

    def setup_distributed_env(self, flow):
        # Overridden by subclasses to set up particular framework's environment.
        pass


def _local_multinode_control_task_step_func(
    flow, env_to_use, step_func, retry_count, input_paths
):
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
    os.environ["MF_PARALLEL_CONTROL_TASK_ID"] = str(current.task_id)

    run_id = current.run_id
    step_name = current.step_name
    control_task_id = current.task_id
    # UBF handling for multinode case
    mapper_task_ids = [control_task_id]
    # If we are running inside Conda, we use the base executable FIRST;
    # the conda environment will then be used when runtime_step_cli is
    # called. This is so that it can properly set up all the metaflow
    # aliases needed.
    executable = env_to_use.executable(step_name)
    script = sys.argv[0]

    # start workers
    # TODO: Logs for worker processes are assigned to control process as of today, which
    #       should be fixed at some point
    subprocesses = []
    for node_index in range(1, num_parallel):
        task_id = "%s_node_%d" % (control_task_id, node_index)
        mapper_task_ids.append(task_id)
        os.environ["MF_PARALLEL_NODE_INDEX"] = str(node_index)
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

    # run the step function ourselves
    os.environ["MF_PARALLEL_NODE_INDEX"] = "0"
    step_func()

    # join the subprocesses
    for p in subprocesses:
        p.wait()
        if p.returncode:
            raise Exception("Subprocess failed, return code {}".format(p.returncode))
