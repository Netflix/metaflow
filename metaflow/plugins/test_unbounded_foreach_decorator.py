# NOTE: This file is an example of an unbounded-foreach implementation and is
# used to test the functionality. This is an example only and is not part of the
# core unbounded-foreach implementation.

import os
import subprocess
import sys
from metaflow.cli_args import cli_args
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.unbounded_foreach import (
    UnboundedForeachInput,
    UBF_CONTROL,
    UBF_TASK,
    CONTROL_TASK_TAG,
)
from metaflow.util import to_unicode
from metaflow.metadata import MetaDatum


class InternalTestUnboundedForeachInput(UnboundedForeachInput):
    """
    Test class that wraps around values (any iterator) and simulates an
    unbounded-foreach instead of a bounded foreach.
    """

    NAME = "InternalTestUnboundedForeachInput"

    def __init__(self, iterable):
        self.iterable = iterable
        super(InternalTestUnboundedForeachInput, self).__init__()

    def __iter__(self):
        return iter(self.iterable)

    def __next__(self):
        return next(self.iter)

    def __getitem__(self, key):
        # Add this for the sake of control task.
        if key is None:
            return self
        return self.iterable[key]

    def __len__(self):
        return len(self.iterable)

    def __str__(self):
        return str(self.iterable)

    def __repr__(self):
        return "%s(%s)" % (self.NAME, self.iterable)


class InternalTestUnboundedForeachDecorator(StepDecorator):
    name = "unbounded_test_foreach_internal"
    results_dict = {}

    def __init__(self, attributes=None, statically_defined=False):
        super(InternalTestUnboundedForeachDecorator, self).__init__(
            attributes, statically_defined
        )

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        self.environment = environment

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
        if ubf_context == UBF_CONTROL:
            metadata.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="internal_task_type",
                        value=CONTROL_TASK_TAG,
                        type="internal_task_type",
                        tags=["attempt_id:{0}".format(0)],
                    )
                ],
            )
            self.input_paths = [obj.pathspec for obj in inputs]

    def control_task_step_func(self, flow, graph, retry_count):
        from metaflow import current

        run_id = current.run_id
        step_name = current.step_name
        control_task_id = current.task_id
        # If we are running inside Conda, we use the base executable FIRST;
        # the conda environment will then be used when runtime_step_cli is
        # called. This is so that it can properly set up all the metaflow
        # aliases needed.
        env_to_use = getattr(self.environment, "base_env", self.environment)
        executable = env_to_use.executable(step_name)
        script = sys.argv[0]

        # Access the `unbounded_foreach` param using `flow` (as datastore).
        assert flow._unbounded_foreach
        foreach_iter = flow.input
        if not isinstance(foreach_iter, InternalTestUnboundedForeachInput):
            raise MetaflowException(
                "Expected type to be "
                "InternalTestUnboundedForeachInput. Found %s" % (type(foreach_iter))
            )
        foreach_num_splits = sum(1 for _ in foreach_iter)

        print(
            "Simulating UnboundedForeach over value:",
            foreach_iter,
            "num_splits:",
            foreach_num_splits,
        )
        mapper_tasks = []

        for i in range(foreach_num_splits):
            task_id = "%s-%d" % (control_task_id, i)
            pathspec = "%s/%s/%s" % (run_id, step_name, task_id)
            mapper_tasks.append(to_unicode(pathspec))
            input_paths = ",".join(self.input_paths)

            # Override specific `step` kwargs.
            kwargs = cli_args.step_kwargs
            kwargs["split_index"] = str(i)
            kwargs["run_id"] = run_id
            kwargs["task_id"] = task_id
            kwargs["input_paths"] = input_paths
            kwargs["ubf_context"] = UBF_TASK
            kwargs["retry_count"] = 0

            cmd = cli_args.step_command(
                executable, script, step_name, step_kwargs=kwargs
            )
            step_cli = " ".join(cmd)
            # Print cmdline for execution. Doesn't work without the temporary
            # unicode object while using `print`.
            print(
                "[${cwd}] Starting split#{split} with cmd:{cmd}".format(
                    cwd=os.getcwd(), split=i, cmd=step_cli
                )
            )
            output_bytes = subprocess.check_output(cmd)
            output = to_unicode(output_bytes)
            for line in output.splitlines():
                print("[Split#%d] %s" % (i, line))
        # Save the list of (child) mapper task pathspec(s) into a designated
        # artifact `_control_mapper_tasks`.
        flow._control_mapper_tasks = mapper_tasks

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        if ubf_context == UBF_CONTROL:
            from functools import partial

            return partial(self.control_task_step_func, flow, graph, retry_count)
        else:
            return step_func

    def step_task_retry_count(self):
        # UBF plugins don't want retry for the control task. We signal this
        # intent to the runtime by returning (None, None).
        return None, None
