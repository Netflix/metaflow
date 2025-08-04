"""
Local backend

Execute the flow with a native runtime
using local / remote processes
"""

from __future__ import print_function
import json
import os
import sys
import fcntl
import re
import tempfile
import time
import subprocess
from datetime import datetime
from io import BytesIO
from itertools import chain
from functools import partial
from concurrent import futures

from metaflow.datastore.exceptions import DataException
from contextlib import contextmanager

from . import get_namespace
from .metadata_provider import MetaDatum
from .metaflow_config import FEAT_ALWAYS_UPLOAD_CODE_PACKAGE, MAX_ATTEMPTS, UI_URL
from .exception import (
    MetaflowException,
    MetaflowInternalError,
    METAFLOW_EXIT_DISALLOW_RETRY,
)
from . import procpoll
from .datastore import TaskDataStoreSet
from .debug import debug
from .decorators import flow_decorators
from .flowspec import _FlowState
from .mflog import mflog, RUNTIME_LOG_SOURCE
from .util import to_unicode, compress_list, unicode_type
from .clone_util import clone_task_helper
from .unbounded_foreach import (
    CONTROL_TASK_TAG,
    UBF_CONTROL,
    UBF_TASK,
)

from .user_configs.config_options import ConfigInput
from .user_configs.config_parameters import dump_config_values

import metaflow.tracing as tracing

MAX_WORKERS = 16
MAX_NUM_SPLITS = 100
MAX_LOG_SIZE = 1024 * 1024
POLL_TIMEOUT = 1000  # ms
PROGRESS_INTERVAL = 300  # s
# The following is a list of the (data) artifacts used by the runtime while
# executing a flow. These are prefetched during the resume operation by
# leveraging the TaskDataStoreSet.
PREFETCH_DATA_ARTIFACTS = [
    "_foreach_stack",
    "_task_ok",
    "_transition",
    "_control_mapper_tasks",
    "_control_task_is_mapper_zero",
]
RESUME_POLL_SECONDS = 60

# Runtime must use logsource=RUNTIME_LOG_SOURCE for all loglines that it
# formats according to mflog. See a comment in mflog.__init__
mflog_msg = partial(mflog.decorate, RUNTIME_LOG_SOURCE)

# TODO option: output dot graph periodically about execution


class NativeRuntime(object):
    def __init__(
        self,
        flow,
        graph,
        flow_datastore,
        metadata,
        environment,
        package,
        logger,
        entrypoint,
        event_logger,
        monitor,
        run_id=None,
        clone_run_id=None,
        clone_only=False,
        reentrant=False,
        steps_to_rerun=None,
        max_workers=MAX_WORKERS,
        max_num_splits=MAX_NUM_SPLITS,
        max_log_size=MAX_LOG_SIZE,
        resume_identifier=None,
        skip_decorator_hooks=False,
    ):
        if run_id is None:
            self._run_id = metadata.new_run_id()
        else:
            self._run_id = run_id
            metadata.register_run_id(run_id)

        self._flow = flow
        self._graph = graph
        self._flow_datastore = flow_datastore
        self._metadata = metadata
        self._environment = environment
        self._package = package
        self._logger = logger
        self._max_workers = max_workers
        self._active_tasks = dict()  # Key: step name;
        # value: [number of running tasks, number of done tasks]
        # Special key 0 is total number of running tasks
        self._active_tasks[0] = 0
        self._unprocessed_steps = set([n.name for n in self._graph])
        self._max_num_splits = max_num_splits
        self._max_log_size = max_log_size
        self._params_task = None
        self._entrypoint = entrypoint
        self.event_logger = event_logger
        self._monitor = monitor
        self._resume_identifier = resume_identifier

        self._clone_run_id = clone_run_id
        self._clone_only = clone_only
        self._cloned_tasks = []
        self._ran_or_scheduled_task_index = set()
        self._reentrant = reentrant
        self._run_url = None
        self._skip_decorator_hooks = skip_decorator_hooks

        # If steps_to_rerun is specified, we will not clone them in resume mode.
        self._steps_to_rerun = steps_to_rerun or {}
        # sorted_nodes are in topological order already, so we only need to
        # iterate through the nodes once to get a stable set of rerun steps.
        for step_name in self._graph.sorted_nodes:
            if step_name in self._steps_to_rerun:
                out_funcs = self._graph[step_name].out_funcs or []
                for next_step in out_funcs:
                    self._steps_to_rerun.add(next_step)

        self._origin_ds_set = None
        if clone_run_id:
            # resume logic
            # 0. If clone_run_id is specified, attempt to clone all the
            # successful tasks from the flow with `clone_run_id`. And run the
            # unsuccessful or not-run steps in the regular fashion.
            # 1. With _find_origin_task, for every task in the current run, we
            # find the equivalent task in `clone_run_id` using
            # pathspec_index=run/step:[index] and verify if this task can be
            # cloned.
            # 2. If yes, we fire off a clone-only task which copies the
            # metadata from the `clone_origin` to pathspec=run/step/task to
            # mimmick that the resumed run looks like an actual run.
            # 3. All steps that couldn't be cloned (either unsuccessful or not
            # run) are run as regular tasks.
            # Lastly, to improve the performance of the cloning process, we
            # leverage the TaskDataStoreSet abstraction to prefetch the
            # entire DAG of `clone_run_id` and relevant data artifacts
            # (see PREFETCH_DATA_ARTIFACTS) so that the entire runtime can
            # access the relevant data from cache (instead of going to the datastore
            # after the first prefetch).
            logger(
                "Gathering required information to resume run (this may take a bit of time)..."
            )
            self._origin_ds_set = TaskDataStoreSet(
                flow_datastore,
                clone_run_id,
                prefetch_data_artifacts=PREFETCH_DATA_ARTIFACTS,
            )
        self._run_queue = []
        self._poll = procpoll.make_poll()
        self._workers = {}  # fd -> subprocess mapping
        self._finished = {}
        self._is_cloned = {}
        # NOTE: In case of unbounded foreach, we need the following to schedule
        # the (sibling) mapper tasks  of the control task (in case of resume);
        # and ensure that the join tasks runs only if all dependent tasks have
        # finished.
        self._control_num_splits = {}  # control_task -> num_splits mapping

        if not self._skip_decorator_hooks:
            for step in flow:
                for deco in step.decorators:
                    deco.runtime_init(flow, graph, package, self._run_id)

    def _new_task(self, step, input_paths=None, **kwargs):
        if input_paths is None:
            may_clone = True
        else:
            may_clone = all(self._is_cloned[path] for path in input_paths)

        if step in self._steps_to_rerun:
            may_clone = False

        if step == "_parameters" or self._skip_decorator_hooks:
            decos = []
        else:
            decos = getattr(self._flow, step).decorators

        return Task(
            self._flow_datastore,
            self._flow,
            step,
            self._run_id,
            self._metadata,
            self._environment,
            self._entrypoint,
            self.event_logger,
            self._monitor,
            input_paths=input_paths,
            may_clone=may_clone,
            clone_run_id=self._clone_run_id,
            clone_only=self._clone_only,
            reentrant=self._reentrant,
            origin_ds_set=self._origin_ds_set,
            decos=decos,
            logger=self._logger,
            resume_identifier=self._resume_identifier,
            **kwargs,
        )

    @property
    def run_id(self):
        return self._run_id

    def persist_constants(self, task_id=None):
        self._params_task = self._new_task("_parameters", task_id=task_id)
        if not self._params_task.is_cloned:
            self._params_task.persist(self._flow)

        self._is_cloned[self._params_task.path] = self._params_task.is_cloned

    def should_skip_clone_only_execution(self):
        (
            should_skip_clone_only_execution,
            skip_reason,
        ) = self._should_skip_clone_only_execution()
        if should_skip_clone_only_execution:
            self._logger(skip_reason, system_msg=True)
            return True
        return False

    @contextmanager
    def run_heartbeat(self):
        self._metadata.start_run_heartbeat(self._flow.name, self._run_id)
        yield
        self._metadata.stop_heartbeat()

    def print_workflow_info(self):
        self._run_url = (
            "%s/%s/%s" % (UI_URL.rstrip("/"), self._flow.name, self._run_id)
            if UI_URL
            else None
        )

        if self._run_url:
            self._logger(
                "Workflow starting (run-id %s), see it in the UI at %s"
                % (
                    self._run_id,
                    self._run_url,
                ),
                system_msg=True,
            )
        else:
            self._logger(
                "Workflow starting (run-id %s):" % self._run_id, system_msg=True
            )

    def _should_skip_clone_only_execution(self):
        if self._clone_only and self._params_task:
            if self._params_task.resume_done():
                return True, "Resume already complete. Skip clone-only execution."
            if not self._params_task.is_resume_leader():
                return (
                    True,
                    "Not resume leader under resume execution. Skip clone-only execution.",
                )
        return False, None

    def clone_task(
        self,
        step_name,
        task_id,
        pathspec_index,
        cloned_task_pathspec_index,
        finished_tuple,
        ubf_context,
        generate_task_obj,
        verbose=False,
    ):
        try:
            new_task_id = task_id
            if generate_task_obj:
                task = self._new_task(step_name, pathspec_index=pathspec_index)
                if ubf_context:
                    task.ubf_context = ubf_context
                new_task_id = task.task_id
                self._cloned_tasks.append(task)
                self._ran_or_scheduled_task_index.add(cloned_task_pathspec_index)
                task_pathspec = "{}/{}/{}".format(self._run_id, step_name, new_task_id)
            else:
                task_pathspec = "{}/{}/{}".format(self._run_id, step_name, new_task_id)
                Task.clone_pathspec_mapping[task_pathspec] = "{}/{}/{}".format(
                    self._clone_run_id, step_name, task_id
                )
            if verbose:
                self._logger(
                    "Cloning task from {}/{}/{}/{} to {}/{}/{}/{}".format(
                        self._flow.name,
                        self._clone_run_id,
                        step_name,
                        task_id,
                        self._flow.name,
                        self._run_id,
                        step_name,
                        new_task_id,
                    ),
                    system_msg=True,
                )
            clone_task_helper(
                self._flow.name,
                self._clone_run_id,
                self._run_id,
                step_name,
                task_id,  # origin_task_id
                new_task_id,
                self._flow_datastore,
                self._metadata,
                origin_ds_set=self._origin_ds_set,
            )
            self._finished[(step_name, finished_tuple)] = task_pathspec
            self._is_cloned[task_pathspec] = True
        except Exception as e:
            self._logger(
                "Cloning {}/{}/{}/{} failed with error: {}".format(
                    self._flow.name, self._clone_run_id, step_name, task_id, str(e)
                )
            )

    def clone_original_run(self, generate_task_obj=False, verbose=True):
        self._logger(
            "Cloning {}/{}".format(self._flow.name, self._clone_run_id),
            system_msg=True,
        )

        inputs = []

        ubf_mapper_tasks_to_clone = set()
        ubf_control_tasks = set()
        # We only clone ubf mapper tasks if the control task is complete.
        # Here we need to check which control tasks are complete, and then get the corresponding
        # mapper tasks.
        for task_ds in self._origin_ds_set:
            _, step_name, task_id = task_ds.pathspec.split("/")
            pathspec_index = task_ds.pathspec_index
            if task_ds["_task_ok"] and step_name != "_parameters":
                # Control task contains "_control_mapper_tasks" but, in the case of
                # @parallel decorator, the control task is also a mapper task so we
                # need to distinguish this using _control_task_is_mapper_zero
                control_mapper_tasks = (
                    []
                    if "_control_mapper_tasks" not in task_ds
                    else task_ds["_control_mapper_tasks"]
                )
                if control_mapper_tasks:
                    if task_ds.get("_control_task_is_mapper_zero", False):
                        # Strip out the control task of list of mapper tasks
                        ubf_control_tasks.add(control_mapper_tasks[0])
                        ubf_mapper_tasks_to_clone.update(control_mapper_tasks[1:])
                    else:
                        ubf_mapper_tasks_to_clone.update(control_mapper_tasks)
                        # Since we only add mapper tasks here, if we are not in the list
                        # we are a control task
                        if task_ds.pathspec not in ubf_mapper_tasks_to_clone:
                            ubf_control_tasks.add(task_ds.pathspec)

        for task_ds in self._origin_ds_set:
            _, step_name, task_id = task_ds.pathspec.split("/")
            pathspec_index = task_ds.pathspec_index

            if (
                task_ds["_task_ok"]
                and step_name != "_parameters"
                and (step_name not in self._steps_to_rerun)
            ):
                # "_unbounded_foreach" is a special flag to indicate that the transition
                # is an unbounded foreach.
                # Both parent and splitted children tasks will have this flag set.
                # The splitted control/mapper tasks
                # are not foreach types because UBF is always followed by a join step.
                is_ubf_task = (
                    "_unbounded_foreach" in task_ds and task_ds["_unbounded_foreach"]
                ) and (self._graph[step_name].type != "foreach")

                is_ubf_control_task = task_ds.pathspec in ubf_control_tasks

                is_ubf_mapper_task = is_ubf_task and (not is_ubf_control_task)

                if is_ubf_mapper_task and (
                    task_ds.pathspec not in ubf_mapper_tasks_to_clone
                ):
                    # Skip copying UBF mapper tasks if control task is incomplete.
                    continue

                ubf_context = None
                if is_ubf_task:
                    ubf_context = "ubf_test" if is_ubf_mapper_task else "ubf_control"

                finished_tuple = tuple(
                    [s._replace(value=0) for s in task_ds.get("_foreach_stack", ())]
                )
                cloned_task_pathspec_index = pathspec_index.split("/")[1]
                if task_ds.get("_control_task_is_mapper_zero", False):
                    # Replace None with index 0 for control task as it is part of the
                    # UBF (as a mapper as well)
                    finished_tuple = finished_tuple[:-1] + (
                        finished_tuple[-1]._replace(index=0),
                    )
                    # We need this reverse override though because when we check
                    # if a task has been cloned in _queue_push, the index will be None
                    # because the _control_task_is_mapper_zero is set in the control
                    # task *itself* and *not* in the one that is launching the UBF nest.
                    # This means that _translate_index will use None.
                    cloned_task_pathspec_index = re.sub(
                        r"(\[(?:\d+, ?)*)0\]",
                        lambda m: (m.group(1) or "[") + "None]",
                        cloned_task_pathspec_index,
                    )

                inputs.append(
                    (
                        step_name,
                        task_id,
                        pathspec_index,
                        cloned_task_pathspec_index,
                        finished_tuple,
                        is_ubf_mapper_task,
                        ubf_context,
                    )
                )

        with futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            all_tasks = [
                executor.submit(
                    self.clone_task,
                    step_name,
                    task_id,
                    pathspec_index,
                    cloned_task_pathspec_index,
                    finished_tuple,
                    ubf_context=ubf_context,
                    generate_task_obj=generate_task_obj and (not is_ubf_mapper_task),
                    verbose=verbose,
                )
                for (
                    step_name,
                    task_id,
                    pathspec_index,
                    cloned_task_pathspec_index,
                    finished_tuple,
                    is_ubf_mapper_task,
                    ubf_context,
                ) in inputs
            ]
            _, _ = futures.wait(all_tasks)
        self._logger(
            "{}/{} cloned!".format(self._flow.name, self._clone_run_id), system_msg=True
        )
        self._params_task.mark_resume_done()

    def execute(self):
        if len(self._cloned_tasks) > 0:
            # mutable list storing the cloned tasks.
            self._run_queue = []
            self._active_tasks[0] = 0
        else:
            if self._params_task:
                self._queue_push("start", {"input_paths": [self._params_task.path]})
            else:
                self._queue_push("start", {})
        progress_tstamp = time.time()
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as config_file:
            # Configurations are passed through a file to avoid overloading the
            # command-line. We only need to create this file once and it can be reused
            # for any task launch
            config_value = dump_config_values(self._flow)
            if config_value:
                json.dump(config_value, config_file)
                config_file.flush()
                self._config_file_name = config_file.name
            else:
                self._config_file_name = None
            try:
                # main scheduling loop
                exception = None
                while (
                    self._run_queue or self._active_tasks[0] > 0 or self._cloned_tasks
                ):
                    # 1. are any of the current workers finished?
                    if self._cloned_tasks:
                        finished_tasks = self._cloned_tasks
                        # reset the list of cloned tasks and let poll_workers handle
                        # the remaining transition
                        self._cloned_tasks = []
                    else:
                        finished_tasks = list(self._poll_workers())
                    # 2. push new tasks triggered by the finished tasks to the queue
                    self._queue_tasks(finished_tasks)
                    # 3. if there are available worker slots, pop and start tasks
                    #    from the queue.
                    self._launch_workers()

                    if time.time() - progress_tstamp > PROGRESS_INTERVAL:
                        progress_tstamp = time.time()
                        tasks_print = ", ".join(
                            [
                                "%s (%d running; %d done)" % (k, v[0], v[1])
                                for k, v in self._active_tasks.items()
                                if k != 0 and v[0] > 0
                            ]
                        )
                        if self._active_tasks[0] == 0:
                            msg = "No tasks are running."
                        else:
                            if self._active_tasks[0] == 1:
                                msg = "1 task is running: "
                            else:
                                msg = "%d tasks are running: " % self._active_tasks[0]
                            msg += "%s." % tasks_print

                        self._logger(msg, system_msg=True)

                        if len(self._run_queue) == 0:
                            msg = "No tasks are waiting in the queue."
                        else:
                            if len(self._run_queue) == 1:
                                msg = "1 task is waiting in the queue: "
                            else:
                                msg = "%d tasks are waiting in the queue." % len(
                                    self._run_queue
                                )

                        self._logger(msg, system_msg=True)
                        if len(self._unprocessed_steps) > 0:
                            if len(self._unprocessed_steps) == 1:
                                msg = "%s step has not started" % (
                                    next(iter(self._unprocessed_steps)),
                                )
                            else:
                                msg = "%d steps have not started: " % len(
                                    self._unprocessed_steps
                                )
                                msg += "%s." % ", ".join(self._unprocessed_steps)
                            self._logger(msg, system_msg=True)

            except KeyboardInterrupt as ex:
                self._logger("Workflow interrupted.", system_msg=True, bad=True)
                self._killall()
                exception = ex
                raise
            except Exception as ex:
                self._logger("Workflow failed.", system_msg=True, bad=True)
                self._killall()
                exception = ex
                raise
            finally:
                # on finish clean tasks
                if not self._skip_decorator_hooks:
                    for step in self._flow:
                        for deco in step.decorators:
                            deco.runtime_finished(exception)
                self._run_exit_hooks()

        # assert that end was executed and it was successful
        if ("end", ()) in self._finished:
            if self._run_url:
                self._logger(
                    "Done! See the run in the UI at %s" % self._run_url,
                    system_msg=True,
                )
            else:
                self._logger("Done!", system_msg=True)
        elif self._clone_only:
            self._logger(
                "Clone-only resume complete -- only previously successful steps were "
                "cloned; no new tasks executed!",
                system_msg=True,
            )
            self._params_task.mark_resume_done()
        else:
            raise MetaflowInternalError(
                "The *end* step was not successful by the end of flow."
            )

    def _run_exit_hooks(self):
        try:
            exit_hook_decos = self._flow._flow_decorators.get("exit_hook", [])
            if not exit_hook_decos:
                return

            successful = ("end", ()) in self._finished or self._clone_only
            pathspec = f"{self._graph.name}/{self._run_id}"
            flow_file = self._environment.get_environment_info()["script"]

            def _call(fn_name):
                try:
                    result = (
                        subprocess.check_output(
                            args=[
                                sys.executable,
                                "-m",
                                "metaflow.plugins.exit_hook.exit_hook_script",
                                flow_file,
                                fn_name,
                                pathspec,
                            ],
                            env=os.environ,
                        )
                        .decode()
                        .strip()
                    )
                    print(result)
                except subprocess.CalledProcessError as e:
                    print(f"[exit_hook] Hook '{fn_name}' failed with error: {e}")
                except Exception as e:
                    print(f"[exit_hook] Unexpected error in hook '{fn_name}': {e}")

            # Call all exit hook functions regardless of individual failures
            for fn_name in [
                name
                for deco in exit_hook_decos
                for name in (deco.success_hooks if successful else deco.error_hooks)
            ]:
                _call(fn_name)

        except Exception as ex:
            pass  # do not fail due to exit hooks for whatever reason.

    def _killall(self):
        # If we are here, all children have received a signal and are shutting down.
        # We want to give them an opportunity to do so and then kill
        live_workers = set(self._workers.values())
        now = int(time.time())
        self._logger(
            "Terminating %d active tasks..." % len(live_workers),
            system_msg=True,
            bad=True,
        )
        while live_workers and int(time.time()) - now < 5:
            # While not all workers are dead and we have waited less than 5 seconds
            live_workers = [worker for worker in live_workers if not worker.clean()]
        if live_workers:
            self._logger(
                "Killing %d remaining tasks after having waited for %d seconds -- "
                "some tasks may not exit cleanly"
                % (len(live_workers), int(time.time()) - now),
                system_msg=True,
                bad=True,
            )
            for worker in live_workers:
                worker.kill()
        self._logger("Flushing logs...", system_msg=True, bad=True)
        # give killed workers a chance to flush their logs to datastore
        for _ in range(3):
            list(self._poll_workers())

    # Given the current task information (task_index), the type of transition,
    # and the split index, return the new task index.
    def _translate_index(self, task, next_step, type, split_index=None):
        match = re.match(r"^(.+)\[(.*)\]$", task.task_index)
        if match:
            _, foreach_index = match.groups()
            # Convert foreach_index to a list of integers
            if len(foreach_index) > 0:
                foreach_index = foreach_index.split(",")
            else:
                foreach_index = []
        else:
            raise ValueError(
                "Index not in the format of {run_id}/{step_name}[{foreach_index}]"
            )
        if type == "linear":
            return "%s[%s]" % (next_step, ",".join(foreach_index))
        elif type == "join":
            indices = []
            if len(foreach_index) > 0:
                indices = foreach_index[:-1]
            return "%s[%s]" % (next_step, ",".join(indices))
        elif type == "split":
            foreach_index.append(str(split_index))
            return "%s[%s]" % (next_step, ",".join(foreach_index))

    # Store the parameters needed for task creation, so that pushing on items
    # onto the run_queue is an inexpensive operation.
    def _queue_push(self, step, task_kwargs, index=None):
        # In the case of cloning, we set all the cloned tasks as the
        # finished tasks when pushing tasks using _queue_tasks. This means that we
        # could potentially try to push the same task multiple times (for example
        # if multiple parents of a join are cloned). We therefore keep track of what
        # has executed (been cloned) or what has been scheduled and avoid scheduling
        # it again.
        if index:
            if index in self._ran_or_scheduled_task_index:
                # It has already run or been scheduled
                return
            # Note that we are scheduling this to run
            self._ran_or_scheduled_task_index.add(index)
        self._run_queue.insert(0, (step, task_kwargs))
        # For foreaches, this will happen multiple time but is ok, becomes a no-op
        self._unprocessed_steps.discard(step)

    def _queue_pop(self):
        return self._run_queue.pop() if self._run_queue else (None, {})

    def _queue_task_join(self, task, next_steps):
        # if the next step is a join, we need to check that
        # all input tasks for the join have finished before queuing it.

        # CHECK: this condition should be enforced by the linter but
        # let's assert that the assumption holds
        if len(next_steps) > 1:
            msg = (
                "Step *{step}* transitions to a join and another "
                "step. The join must be the only transition."
            )
            raise MetaflowInternalError(task, msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        unbounded_foreach = not task.results.is_none("_unbounded_foreach")

        if unbounded_foreach:
            # Before we queue the join, do some post-processing of runtime state
            # (_finished, _is_cloned) for the (sibling) mapper tasks.
            # Update state of (sibling) mapper tasks for control task.
            if task.ubf_context == UBF_CONTROL:
                mapper_tasks = task.results.get("_control_mapper_tasks")
                if not mapper_tasks:
                    msg = (
                        "Step *{step}* has a control task which didn't "
                        "specify the artifact *_control_mapper_tasks* for "
                        "the subsequent *{join}* step."
                    )
                    raise MetaflowInternalError(
                        msg.format(step=task.step, join=next_steps[0])
                    )
                elif not (
                    isinstance(mapper_tasks, list)
                    and isinstance(mapper_tasks[0], unicode_type)
                ):
                    msg = (
                        "Step *{step}* has a control task which didn't "
                        "specify the artifact *_control_mapper_tasks* as a "
                        "list of strings but instead specified it as {typ} "
                        "with elements of {elem_typ}."
                    )
                    raise MetaflowInternalError(
                        msg.format(
                            step=task.step,
                            typ=type(mapper_tasks),
                            elem_type=type(mapper_tasks[0]),
                        )
                    )
                num_splits = len(mapper_tasks)
                self._control_num_splits[task.path] = num_splits

                # If the control task is cloned, all mapper tasks should have been cloned
                # as well, so we no longer need to handle cloning of mapper tasks in runtime.

                # Update _finished if we are not cloned. If we were cloned, we already
                # updated _finished with the new tasks. Note that the *value* of mapper
                # tasks is incorrect and contains the pathspec of the *cloned* run
                # but we don't use it for anything. We could look to clean it up though
                if not task.is_cloned:
                    _, foreach_stack = task.finished_id
                    top = foreach_stack[-1]
                    bottom = list(foreach_stack[:-1])
                    for i in range(num_splits):
                        s = tuple(bottom + [top._replace(index=i)])
                        self._finished[(task.step, s)] = mapper_tasks[i]
                        self._is_cloned[mapper_tasks[i]] = False

            # Find and check status of control task and retrieve its pathspec
            # for retrieving unbounded foreach cardinality.
            _, foreach_stack = task.finished_id
            top = foreach_stack[-1]
            bottom = list(foreach_stack[:-1])
            s = tuple(bottom + [top._replace(index=None)])

            # UBF control can also be the first task of the list. Then
            # it will have index=0 instead of index=None.
            if task.results.get("_control_task_is_mapper_zero", False):
                s = tuple(bottom + [top._replace(index=0)])
            control_path = self._finished.get((task.step, s))
            if control_path:
                # Control task was successful.
                # Additionally check the state of (sibling) mapper tasks as well
                # (for the sake of resume) before queueing join task.
                num_splits = self._control_num_splits[control_path]
                required_tasks = []
                for i in range(num_splits):
                    s = tuple(bottom + [top._replace(index=i)])
                    required_tasks.append(self._finished.get((task.step, s)))

                if all(required_tasks):
                    index = self._translate_index(task, next_step, "join")
                    # all tasks to be joined are ready. Schedule the next join step.
                    self._queue_push(
                        next_step,
                        {"input_paths": required_tasks, "join_type": "foreach"},
                        index,
                    )
        else:
            # matching_split is the split-parent of the finished task
            matching_split = self._graph[self._graph[next_step].split_parents[-1]]
            _, foreach_stack = task.finished_id
            index = ""
            if matching_split.type == "foreach":
                # next step is a foreach join

                def siblings(foreach_stack):
                    top = foreach_stack[-1]
                    bottom = list(foreach_stack[:-1])
                    for index in range(top.num_splits):
                        yield tuple(bottom + [top._replace(index=index)])

                # required tasks are all split-siblings of the finished task
                required_tasks = [
                    self._finished.get((task.step, s)) for s in siblings(foreach_stack)
                ]
                join_type = "foreach"
                index = self._translate_index(task, next_step, "join")
            else:
                # next step is a split
                # required tasks are all branches joined by the next step
                required_tasks = [
                    self._finished.get((step, foreach_stack))
                    for step in self._graph[next_step].in_funcs
                ]
                join_type = "linear"
                index = self._translate_index(task, next_step, "linear")

            if all(required_tasks):
                # all tasks to be joined are ready. Schedule the next join step.
                self._queue_push(
                    next_step,
                    {"input_paths": required_tasks, "join_type": join_type},
                    index,
                )

    def _queue_task_foreach(self, task, next_steps):
        # CHECK: this condition should be enforced by the linter but
        # let's assert that the assumption holds
        if len(next_steps) > 1:
            msg = (
                "Step *{step}* makes a foreach split but it defines "
                "multiple transitions. Specify only one transition "
                "for foreach."
            )
            raise MetaflowInternalError(msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        unbounded_foreach = not task.results.is_none("_unbounded_foreach")
        if unbounded_foreach:
            # Need to push control process related task.
            ubf_iter_name = task.results.get("_foreach_var")
            ubf_iter = task.results.get(ubf_iter_name)
            # UBF control task has no split index, hence "None" as place holder.

            if task.results.get("_control_task_is_mapper_zero", False):
                index = self._translate_index(task, next_step, "split", 0)
            else:
                index = self._translate_index(task, next_step, "split", None)
            self._queue_push(
                next_step,
                {
                    "input_paths": [task.path],
                    "ubf_context": UBF_CONTROL,
                    "ubf_iter": ubf_iter,
                },
                index,
            )
        else:
            num_splits = task.results["_foreach_num_splits"]
            if num_splits > self._max_num_splits:
                msg = (
                    "Foreach in step *{step}* yielded {num} child steps "
                    "which is more than the current maximum of {max} "
                    "children. You can raise the maximum with the "
                    "--max-num-splits option. "
                )
                raise TaskFailed(
                    task,
                    msg.format(
                        step=task.step, num=num_splits, max=self._max_num_splits
                    ),
                )

            # schedule all splits
            for i in range(num_splits):
                index = self._translate_index(task, next_step, "split", i)
                self._queue_push(
                    next_step,
                    {"split_index": str(i), "input_paths": [task.path]},
                    index,
                )

    def _queue_tasks(self, finished_tasks):
        # finished tasks include only successful tasks
        for task in finished_tasks:
            self._finished[task.finished_id] = task.path
            self._is_cloned[task.path] = task.is_cloned

            # CHECK: ensure that runtime transitions match with
            # statically inferred transitions. Make an exception for control
            # tasks, where we just rely on static analysis since we don't
            # execute user code.
            trans = task.results.get("_transition")
            if trans:
                next_steps = trans[0]
                foreach = trans[1]
            else:
                next_steps = []
                foreach = None
            expected = self._graph[task.step].out_funcs
            if next_steps != expected:
                msg = (
                    "Based on static analysis of the code, step *{step}* "
                    "was expected to transition to step(s) *{expected}*. "
                    "However, when the code was executed, self.next() was "
                    "called with *{actual}*. Make sure there is only one "
                    "unconditional self.next() call in the end of your "
                    "step. "
                )
                raise MetaflowInternalError(
                    msg.format(
                        step=task.step,
                        expected=", ".join(expected),
                        actual=", ".join(next_steps),
                    )
                )

            # Different transition types require different treatment
            if any(self._graph[f].type == "join" for f in next_steps):
                # Next step is a join
                self._queue_task_join(task, next_steps)
            elif foreach:
                # Next step is a foreach child
                self._queue_task_foreach(task, next_steps)
            else:
                # Next steps are normal linear steps
                for step in next_steps:
                    index = self._translate_index(task, step, "linear")
                    self._queue_push(step, {"input_paths": [task.path]}, index)

    def _poll_workers(self):
        if self._workers:
            for event in self._poll.poll(POLL_TIMEOUT):
                worker = self._workers.get(event.fd)
                if worker:
                    if event.can_read:
                        worker.read_logline(event.fd)
                    if event.is_terminated:
                        returncode = worker.terminate()

                        for fd in worker.fds():
                            self._poll.remove(fd)
                            del self._workers[fd]
                        step_counts = self._active_tasks[worker.task.step]
                        step_counts[0] -= 1  # One less task for this step is running
                        step_counts[1] += 1  # ... and one more completed.
                        # We never remove from self._active_tasks because it is possible
                        # for all currently running task for a step to complete but
                        # for others to still be queued up.
                        self._active_tasks[0] -= 1

                        task = worker.task
                        if returncode:
                            # worker did not finish successfully
                            if (
                                worker.cleaned
                                or returncode == METAFLOW_EXIT_DISALLOW_RETRY
                            ):
                                self._logger(
                                    "This failed task will not be retried.",
                                    system_msg=True,
                                )
                            else:
                                if (
                                    task.retries
                                    < task.user_code_retries + task.error_retries
                                ):
                                    self._retry_worker(worker)
                                else:
                                    raise TaskFailed(task)
                        else:
                            # worker finished successfully
                            yield task

    def _launch_workers(self):
        while self._run_queue and self._active_tasks[0] < self._max_workers:
            step, task_kwargs = self._queue_pop()
            # Initialize the task (which can be expensive using remote datastores)
            # before launching the worker so that cost is amortized over time, instead
            # of doing it during _queue_push.
            if (
                FEAT_ALWAYS_UPLOAD_CODE_PACKAGE
                and "METAFLOW_CODE_SHA" not in os.environ
            ):
                # We check if the code package is uploaded and, if so, we set the
                # environment variables that will cause the metadata service to
                # register the code package with the task created in _new_task below
                code_sha = self._package.package_sha(timeout=0.01)
                if code_sha:
                    os.environ["METAFLOW_CODE_SHA"] = code_sha
                    os.environ["METAFLOW_CODE_URL"] = self._package.package_url()
                    os.environ["METAFLOW_CODE_DS"] = self._flow_datastore.TYPE
                    os.environ["METAFLOW_CODE_METADATA"] = (
                        self._package.package_metadata
                    )

            task = self._new_task(step, **task_kwargs)
            self._launch_worker(task)

    def _retry_worker(self, worker):
        worker.task.retries += 1
        if worker.task.retries >= MAX_ATTEMPTS:
            # any results with an attempt ID >= MAX_ATTEMPTS will be ignored
            # by datastore, so running a task with such a retry_could would
            # be pointless and dangerous
            raise MetaflowInternalError(
                "Too many task attempts (%d)! "
                "MAX_ATTEMPTS exceeded." % worker.task.retries
            )

        worker.task.new_attempt()
        self._launch_worker(worker.task)

    def _launch_worker(self, task):
        if self._clone_only and not task.is_cloned:
            # We don't launch a worker here
            self._logger(
                "Not executing non-cloned task for step '%s' in clone-only resume"
                % "/".join([task.flow_name, task.run_id, task.step]),
                system_msg=True,
            )
            return

        worker = Worker(task, self._max_log_size, self._config_file_name)
        for fd in worker.fds():
            self._workers[fd] = worker
            self._poll.add(fd)
        active_step_counts = self._active_tasks.setdefault(task.step, [0, 0])

        # We have an additional task for this step running
        active_step_counts[0] += 1

        # One more task actively running
        self._active_tasks[0] += 1


class Task(object):
    clone_pathspec_mapping = {}

    def __init__(
        self,
        flow_datastore,
        flow,
        step,
        run_id,
        metadata,
        environment,
        entrypoint,
        event_logger,
        monitor,
        input_paths=None,
        may_clone=False,
        clone_run_id=None,
        clone_only=False,
        reentrant=False,
        origin_ds_set=None,
        decos=None,
        logger=None,
        # Anything below this is passed as part of kwargs
        split_index=None,
        ubf_context=None,
        ubf_iter=None,
        join_type=None,
        task_id=None,
        resume_identifier=None,
        pathspec_index=None,
    ):
        self.step = step
        self.flow = flow
        self.flow_name = flow.name
        self.run_id = run_id
        self.task_id = None
        self._path = None
        self.input_paths = input_paths
        self.split_index = split_index
        self.ubf_context = ubf_context
        self.ubf_iter = ubf_iter
        self.decos = [] if decos is None else decos
        self.entrypoint = entrypoint
        self.environment = environment
        self.environment_type = self.environment.TYPE
        self.clone_run_id = clone_run_id
        self.clone_origin = None
        self.origin_ds_set = origin_ds_set
        self.metadata = metadata
        self.event_logger = event_logger
        self.monitor = monitor

        self._logger = logger

        self.retries = 0
        self.user_code_retries = 0
        self.error_retries = 0

        self.tags = metadata.sticky_tags
        self.event_logger_type = self.event_logger.TYPE
        self.monitor_type = monitor.TYPE

        self.metadata_type = metadata.TYPE
        self.datastore_type = flow_datastore.TYPE
        self._flow_datastore = flow_datastore
        self.datastore_sysroot = flow_datastore.datastore_root
        self._results_ds = None

        # Only used in clone-only resume.
        self._is_resume_leader = None
        self._resume_done = None
        self._resume_identifier = resume_identifier
        origin = None
        if clone_run_id and may_clone:
            origin = self._find_origin_task(clone_run_id, join_type, pathspec_index)
        if origin and origin["_task_ok"]:
            # At this point, we know we are going to clone
            self._is_cloned = True

            task_id_exists_already = False
            task_completed = False
            if reentrant:
                # A re-entrant clone basically allows multiple concurrent processes
                # to perform the clone at the same time to the same new run id. Let's
                # assume two processes A and B both simultaneously calling
                # `resume --reentrant --run-id XX`.
                # We want to guarantee that:
                #   - All incomplete tasks are cloned exactly once.
                # To achieve this, we will select a resume leader and let it clone the
                # entire execution graph. This ensures that we only write once to the
                # datastore and metadata.
                #
                # We use the cloned _parameter task's task-id as the "key" to synchronize
                # on. We try to "register" this new task-id (or rather the full pathspec
                # <run>/<step>/<taskid>) with the metadata service which will indicate
                # if we actually registered it or if it existed already. If we did manage
                # to register it (_parameter task), we are the "elected resume leader"
                # in essence and proceed to clone. If we didn't, we just wait to make
                # sure the entire clone execution is fully done (ie: the clone is finished).
                if task_id is not None:
                    # Sanity check -- this should never happen. We cannot allow
                    # for explicit task-ids because in the reentrant case, we use the
                    # cloned task's id as the new task's id.
                    raise MetaflowInternalError(
                        "Reentrant clone-only resume does not allow for explicit task-id"
                    )

                if resume_identifier:
                    self.log(
                        "Resume identifier is %s." % resume_identifier,
                        system_msg=True,
                    )
                else:
                    raise MetaflowInternalError(
                        "Reentrant clone-only resume needs a resume identifier."
                    )
                # We will use the same task_id as the original task
                # to use it effectively as a synchronization key
                clone_task_id = origin.task_id
                # Make sure the task-id is a non-integer to not clash with task ids
                # assigned by the metadata provider. If this is an integer, we
                # add some string to it. It doesn't matter what we add as long as it is
                # consistent.
                try:
                    clone_task_int = int(clone_task_id)
                    clone_task_id = "resume-%d" % clone_task_int
                except ValueError:
                    pass

                # If _get_task_id returns True it means the task already existed, so
                # we wait for it.
                task_id_exists_already = self._get_task_id(clone_task_id)

                # We may not have access to task datastore on first resume attempt, but
                # on later resume attempt, we should check if the resume task is complete
                # or not. This is to fix the issue where the resume leader was killed
                # unexpectedly during cloning and never mark task complete.
                try:
                    task_completed = self.results["_task_ok"]
                except DataException as e:
                    pass
            else:
                self._get_task_id(task_id)

            # Store the mapping from current_pathspec -> origin_pathspec which
            # will be useful for looking up origin_ds_set in find_origin_task.
            self.clone_pathspec_mapping[self._path] = origin.pathspec
            if self.step == "_parameters":
                # In the _parameters task, we need to resolve who is the resume leader.
                self._is_resume_leader = False
                resume_leader = None

                if task_id_exists_already:
                    # If the task id already exists, we need to check if current task is the resume leader in previous attempt.
                    ds = self._flow_datastore.get_task_datastore(
                        self.run_id, self.step, self.task_id
                    )
                    if not ds["_task_ok"]:
                        raise MetaflowInternalError(
                            "Externally cloned _parameters task did not succeed"
                        )

                    # Check if we should be the resume leader (maybe from previous attempt).
                    # To avoid the edge case where the resume leader is selected but has not
                    # yet written the _resume_leader metadata, we will wait for a few seconds.
                    # We will wait for resume leader for at most 3 times.
                    for _ in range(3):
                        if ds.has_metadata("_resume_leader", add_attempt=False):
                            resume_leader = ds.load_metadata(
                                ["_resume_leader"], add_attempt=False
                            )["_resume_leader"]
                            self._is_resume_leader = resume_leader == resume_identifier
                        else:
                            self.log(
                                "Waiting for resume leader to be selected. Sleeping ...",
                                system_msg=True,
                            )
                            time.sleep(3)
                else:
                    # If the task id does not exist, current task is the resume leader.
                    resume_leader = resume_identifier
                    self._is_resume_leader = True

                if reentrant:
                    if resume_leader:
                        self.log(
                            "Resume leader is %s." % resume_leader,
                            system_msg=True,
                        )
                    else:
                        raise MetaflowInternalError(
                            "Can not determine the resume leader in distributed resume mode."
                        )

                if self._is_resume_leader:
                    if reentrant:
                        self.log(
                            "Selected as the reentrant clone leader.",
                            system_msg=True,
                        )
                    # Clone in place without relying on run_queue.
                    self.new_attempt()
                    self._ds.clone(origin)
                    # Set the resume leader be the task that calls the resume (first task to clone _parameters task).
                    # We will always set resume leader regardless whether we are in distributed resume case or not.
                    if resume_identifier:
                        self._ds.save_metadata(
                            {"_resume_leader": resume_identifier}, add_attempt=False
                        )

                    self._ds.done()
                else:
                    # Wait for the resume leader to complete
                    while True:
                        ds = self._flow_datastore.get_task_datastore(
                            self.run_id, self.step, self.task_id
                        )

                        # Check if resume is complete. Resume leader will write the done file.
                        self._resume_done = ds.has_metadata(
                            "_resume_done", add_attempt=False
                        )

                        if self._resume_done:
                            break

                        self.log(
                            "Waiting for resume leader to complete. Sleeping for %ds..."
                            % RESUME_POLL_SECONDS,
                            system_msg=True,
                        )
                        time.sleep(RESUME_POLL_SECONDS)
                    self.log(
                        "_parameters clone completed by resume leader", system_msg=True
                    )
            else:
                # Only leader can reach non-parameter steps in resume.

                # Store the origin pathspec in clone_origin so this can be run
                # as a task by the runtime.
                self.clone_origin = origin.pathspec
                # Save a call to creating the results_ds since its same as origin.
                self._results_ds = origin

                # If the task is already completed in new run, we don't need to clone it.
                self._should_skip_cloning = task_completed
                if self._should_skip_cloning:
                    self.log(
                        "Skipping cloning of previously run task %s"
                        % self.clone_origin,
                        system_msg=True,
                    )
                else:
                    self.log(
                        "Cloning previously run task %s" % self.clone_origin,
                        system_msg=True,
                    )
        else:
            self._is_cloned = False
            if clone_only:
                # We are done -- we don't proceed to create new task-ids
                return
            self._get_task_id(task_id)

        # Open the output datastore only if the task is not being cloned.
        if not self._is_cloned:
            self.new_attempt()
            for deco in decos:
                deco.runtime_task_created(
                    self._ds,
                    task_id,
                    split_index,
                    input_paths,
                    self._is_cloned,
                    ubf_context,
                )

                # determine the number of retries of this task
                user_code_retries, error_retries = deco.step_task_retry_count()
                if user_code_retries is None and error_retries is None:
                    # This signals the runtime that the task doesn't want any
                    # retries indifferent to other decorator opinions.
                    # NOTE: This is needed since we don't statically disallow
                    # specifying `@retry` in combination with decorators which
                    # implement `unbounded_foreach` semantics. This allows for
                    # ergonomic user invocation of `--with retry`; instead
                    # choosing to specially handle this way in the runtime.
                    self.user_code_retries = None
                    self.error_retries = None
                if (
                    self.user_code_retries is not None
                    and self.error_retries is not None
                ):
                    self.user_code_retries = max(
                        self.user_code_retries, user_code_retries
                    )
                    self.error_retries = max(self.error_retries, error_retries)
            if self.user_code_retries is None and self.error_retries is None:
                self.user_code_retries = 0
                self.error_retries = 0

    def new_attempt(self):
        self._ds = self._flow_datastore.get_task_datastore(
            self.run_id, self.step, self.task_id, attempt=self.retries, mode="w"
        )
        self._ds.init_task()

    def log(self, msg, system_msg=False, pid=None, timestamp=True):
        if pid:
            prefix = "[%s (pid %s)] " % (self._path, pid)
        else:
            prefix = "[%s] " % self._path

        self._logger(msg, head=prefix, system_msg=system_msg, timestamp=timestamp)
        sys.stdout.flush()

    def is_resume_leader(self):
        assert (
            self.step == "_parameters"
        ), "Only _parameters step can check resume leader."
        return self._is_resume_leader

    def resume_done(self):
        assert (
            self.step == "_parameters"
        ), "Only _parameters step can check wheather resume is complete."
        return self._resume_done

    def mark_resume_done(self):
        assert (
            self.step == "_parameters"
        ), "Only _parameters step can mark resume as done."
        assert self.is_resume_leader(), "Only resume leader can mark resume as done."

        # Mark the resume as done. This is called at the end of the resume flow and after
        # the _parameters step was successfully cloned, so we need to 'dangerously' save
        # this done file, but the risk should be minimal.
        self._ds._dangerous_save_metadata_post_done(
            {"_resume_done": True}, add_attempt=False
        )

    def _get_task_id(self, task_id):
        already_existed = True
        tags = []
        if self.ubf_context == UBF_CONTROL:
            tags = [CONTROL_TASK_TAG]
        # Register Metaflow tasks.
        if task_id is None:
            task_id = str(
                self.metadata.new_task_id(self.run_id, self.step, sys_tags=tags)
            )
            already_existed = False
        else:
            # task_id is preset only by persist_constants().
            already_existed = not self.metadata.register_task_id(
                self.run_id,
                self.step,
                task_id,
                0,
                sys_tags=tags,
            )

        self.task_id = task_id
        self._path = "%s/%s/%s" % (self.run_id, self.step, self.task_id)
        return already_existed

    def _find_origin_task(self, clone_run_id, join_type, pathspec_index=None):
        if pathspec_index:
            origin = self.origin_ds_set.get_with_pathspec_index(pathspec_index)
            return origin
        elif self.step == "_parameters":
            pathspec = "%s/_parameters[]" % clone_run_id
            origin = self.origin_ds_set.get_with_pathspec_index(pathspec)

            if origin is None:
                # This is just for usability: We could rerun the whole flow
                # if an unknown clone_run_id is provided but probably this is
                # not what the user intended, so raise a warning
                raise MetaflowException(
                    "Resume could not find run id *%s*" % clone_run_id
                )
            else:
                return origin
        else:
            # all inputs must have the same foreach stack, so we can safely
            # pick the first one
            parent_pathspec = self.input_paths[0]
            origin_parent_pathspec = self.clone_pathspec_mapping[parent_pathspec]
            parent = self.origin_ds_set.get_with_pathspec(origin_parent_pathspec)
            # Parent should be non-None since only clone the child if the parent
            # was successfully cloned.
            foreach_stack = parent["_foreach_stack"]
            if join_type == "foreach":
                # foreach-join pops the topmost index
                index = ",".join(str(s.index) for s in foreach_stack[:-1])
            elif self.split_index or self.ubf_context == UBF_CONTROL:
                # foreach-split pushes a new index
                index = ",".join(
                    [str(s.index) for s in foreach_stack] + [str(self.split_index)]
                )
            else:
                # all other transitions keep the parent's foreach stack intact
                index = ",".join(str(s.index) for s in foreach_stack)
            pathspec = "%s/%s[%s]" % (clone_run_id, self.step, index)
            return self.origin_ds_set.get_with_pathspec_index(pathspec)

    @property
    def path(self):
        return self._path

    @property
    def results(self):
        if self._results_ds:
            return self._results_ds
        else:
            self._results_ds = self._flow_datastore.get_task_datastore(
                self.run_id, self.step, self.task_id
            )
            return self._results_ds

    @property
    def task_index(self):
        _, task_index = self.results.pathspec_index.split("/")
        return task_index

    @property
    def finished_id(self):
        # note: id is not available before the task has finished.
        # Index already identifies the task within the foreach,
        # we will remove foreach value so that it is easier to
        # identify siblings within a foreach.
        foreach_stack_tuple = tuple(
            [s._replace(value=0) for s in self.results["_foreach_stack"]]
        )
        return (self.step, foreach_stack_tuple)

    @property
    def is_cloned(self):
        return self._is_cloned

    @property
    def should_skip_cloning(self):
        return self._should_skip_cloning

    def persist(self, flow):
        # this is used to persist parameters before the start step
        flow._task_ok = flow._success = True
        flow._foreach_stack = []
        self._ds.persist(flow)
        self._ds.done()

    def save_logs(self, logtype_to_logs):
        self._ds.save_logs(RUNTIME_LOG_SOURCE, logtype_to_logs)

    def save_metadata(self, name, metadata):
        self._ds.save_metadata({name: metadata})

    def __str__(self):
        return " ".join(self._args)


class TaskFailed(MetaflowException):
    headline = "Step failure"

    def __init__(self, task, msg=""):
        body = "Step *%s* (task-id %s) failed" % (task.step, task.task_id)
        if msg:
            body = "%s: %s" % (body, msg)
        else:
            body += "."

        super(TaskFailed, self).__init__(body)


class TruncatedBuffer(object):
    def __init__(self, name, maxsize):
        self.name = name
        self._maxsize = maxsize
        self._buffer = BytesIO()
        self._size = 0
        self._eof = False

    def write(self, bytedata, system_msg=False):
        if system_msg:
            self._buffer.write(bytedata)
        elif not self._eof:
            if self._size + len(bytedata) < self._maxsize:
                self._buffer.write(bytedata)
                self._size += len(bytedata)
            else:
                msg = b"[TRUNCATED - MAXIMUM LOG FILE SIZE REACHED]\n"
                self._buffer.write(mflog_msg(msg))
                self._eof = True

    def get_bytes(self):
        return self._buffer.getvalue()

    def get_buffer(self):
        self._buffer.seek(0)
        return self._buffer


class CLIArgs(object):
    """
    Container to allow decorators modify the command line parameters
    for step execution in StepDecorator.runtime_step_cli().
    """

    def __init__(self, task):
        self.task = task
        self.entrypoint = list(task.entrypoint)
        step_obj = getattr(self.task.flow, self.task.step)
        self.top_level_options = {
            "quiet": True,
            "metadata": self.task.metadata_type,
            "environment": self.task.environment_type,
            "datastore": self.task.datastore_type,
            "pylint": False,
            "event-logger": self.task.event_logger_type,
            "monitor": self.task.monitor_type,
            "datastore-root": self.task.datastore_sysroot,
            "with": [
                deco.make_decorator_spec()
                for deco in chain(
                    self.task.decos,
                    step_obj.wrappers,
                    step_obj.config_decorators,
                )
                if not deco.statically_defined and deco.inserted_by is None
            ],
        }

        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook.
        for deco in flow_decorators(self.task.flow):
            self.top_level_options.update(deco.get_top_level_options())

        # We also pass configuration options using the kv.<name> syntax which will cause
        # the configuration options to be loaded from the CONFIG file (or local-config-file
        # in the case of the local runtime)
        configs = self.task.flow._flow_state.get(_FlowState.CONFIGS)
        if configs:
            self.top_level_options["config-value"] = [
                (k, ConfigInput.make_key_name(k)) for k in configs
            ]

        self.commands = ["step"]
        self.command_args = [self.task.step]
        self.command_options = {
            "run-id": task.run_id,
            "task-id": task.task_id,
            "input-paths": compress_list(task.input_paths),
            "split-index": task.split_index,
            "retry-count": task.retries,
            "max-user-code-retries": task.user_code_retries,
            "tag": task.tags,
            "namespace": get_namespace() or "",
            "ubf-context": task.ubf_context,
        }
        self.env = {}

    def get_args(self):
        # TODO: Make one with dict_to_cli_options; see cli_args.py for more detail
        def _options(mapping):
            for k, v in mapping.items():
                # None or False arguments are ignored
                # v needs to be explicitly False, not falsy, e.g. 0 is an acceptable value
                if v is None or v is False:
                    continue

                # we need special handling for 'with' since it is a reserved
                # keyword in Python, so we call it 'decospecs' in click args
                if k == "decospecs":
                    k = "with"
                k = k.replace("_", "-")
                v = v if isinstance(v, (list, tuple, set)) else [v]
                for value in v:
                    yield "--%s" % k
                    if not isinstance(value, bool):
                        value = value if isinstance(value, tuple) else (value,)
                        for vv in value:
                            yield to_unicode(vv)

        args = list(self.entrypoint)
        args.extend(_options(self.top_level_options))
        args.extend(self.commands)
        args.extend(self.command_args)

        args.extend(_options(self.command_options))
        return args

    def get_env(self):
        return self.env

    def __str__(self):
        return " ".join(self.get_args())


class Worker(object):
    def __init__(self, task, max_logs_size, config_file_name):
        self.task = task
        self._config_file_name = config_file_name
        self._proc = self._launch()

        if task.retries > task.user_code_retries:
            self.task.log(
                "Task fallback is starting to handle the failure.",
                system_msg=True,
                pid=self._proc.pid,
            )
        elif not task.is_cloned:
            suffix = " (retry)." if task.retries else "."
            self.task.log(
                "Task is starting" + suffix, system_msg=True, pid=self._proc.pid
            )

        self._stdout = TruncatedBuffer("stdout", max_logs_size)
        self._stderr = TruncatedBuffer("stderr", max_logs_size)

        self._logs = {
            self._proc.stderr.fileno(): (self._proc.stderr, self._stderr),
            self._proc.stdout.fileno(): (self._proc.stdout, self._stdout),
        }

        self._encoding = sys.stdout.encoding or "UTF-8"
        self.killed = False  # Killed indicates that the task was forcibly killed
        # with SIGKILL by the master process.
        # A killed task is always considered cleaned
        self.cleaned = False  # A cleaned task is one that is shutting down and has been
        # noticed by the runtime and queried for its state (whether or
        # not it is properly shut down)

    def _launch(self):
        args = CLIArgs(self.task)
        env = dict(os.environ)

        if self.task.clone_run_id:
            args.command_options["clone-run-id"] = self.task.clone_run_id

        if self.task.is_cloned and self.task.clone_origin:
            args.command_options["clone-only"] = self.task.clone_origin
            # disabling sidecars for cloned tasks due to perf reasons
            args.top_level_options["event-logger"] = "nullSidecarLogger"
            args.top_level_options["monitor"] = "nullSidecarMonitor"
        else:
            # decorators may modify the CLIArgs object in-place
            for deco in self.task.decos:
                deco.runtime_step_cli(
                    args,
                    self.task.retries,
                    self.task.user_code_retries,
                    self.task.ubf_context,
                )

        # Add user configurations using a file to avoid using up too much space on the
        # command line
        if self._config_file_name:
            args.top_level_options["local-config-file"] = self._config_file_name
        # Pass configuration options
        env.update(args.get_env())
        env["PYTHONUNBUFFERED"] = "x"
        tracing.inject_tracing_vars(env)
        # NOTE bufsize=1 below enables line buffering which is required
        # by read_logline() below that relies on readline() not blocking
        # print('running', args)
        cmdline = args.get_args()
        debug.subcommand_exec(cmdline)
        return subprocess.Popen(
            cmdline,
            env=env,
            bufsize=1,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    def emit_log(self, msg, buf, system_msg=False):
        if mflog.is_structured(msg):
            res = mflog.parse(msg)
            if res:
                # parsing successful
                plain = res.msg
                timestamp = res.utc_tstamp
                if res.should_persist:
                    # in special circumstances we may receive structured
                    # loglines that haven't been properly persisted upstream.
                    # This is the case if, for example, a task crashes in an external
                    # system and we retrieve the remaining logs after the crash.
                    # Those lines are marked with a special tag, should_persist,
                    # which we process here
                    buf.write(mflog.unset_should_persist(msg))
            else:
                # parsing failed, corrupted logline. Print it as-is
                timestamp = datetime.utcnow()
                plain = msg
        else:
            # If the line isn't formatted with mflog already, we format it here.
            plain = msg
            timestamp = datetime.utcnow()
            # store unformatted loglines in the buffer that will be
            # persisted, assuming that all previously formatted loglines have
            # been already persisted at the source.
            buf.write(mflog_msg(msg, now=timestamp), system_msg=system_msg)
        text = plain.strip().decode(self._encoding, errors="replace")
        self.task.log(
            text,
            pid=self._proc.pid,
            timestamp=mflog.utc_to_local(timestamp),
            system_msg=system_msg,
        )

    def read_logline(self, fd):
        fileobj, buf = self._logs[fd]
        # readline() below should never block thanks to polling and
        # line buffering. If it does, things will deadlock
        line = fileobj.readline()
        if line:
            self.emit_log(line, buf)
            return True
        else:
            return False

    def fds(self):
        return (self._proc.stderr.fileno(), self._proc.stdout.fileno())

    def clean(self):
        if self.killed:
            return True
        if not self.cleaned:
            for fileobj, buf in self._logs.values():
                msg = b"[KILLED BY ORCHESTRATOR]\n"
                self.emit_log(msg, buf, system_msg=True)
            self.cleaned = True
        return self._proc.poll() is not None

    def kill(self):
        if not self.killed:
            try:
                self._proc.kill()
            except:
                pass
            self.cleaned = True
            self.killed = True

    def terminate(self):
        # this shouldn't block, since terminate() is called only
        # after the poller has decided that the worker is dead
        returncode = self._proc.wait()

        # consume all remaining loglines
        # we set the file descriptor to be non-blocking, since
        # the pipe may stay active due to subprocesses launched by
        # the worker, e.g. sidecars, so we can't rely on EOF. We try to
        # read just what's available in the pipe buffer
        for fileobj, buf in self._logs.values():
            fileno = fileobj.fileno()
            fcntl.fcntl(fileno, fcntl.F_SETFL, os.O_NONBLOCK)
            try:
                while self.read_logline(fileno):
                    pass
            except:
                # ignore "resource temporarily unavailable" etc. errors
                # caused due to non-blocking. Draining is done on a
                # best-effort basis.
                pass

        # Return early if the task is cloned since we don't want to
        # perform any log collection.
        if not self.task.is_cloned:
            self.task.save_metadata(
                "runtime",
                {
                    "return_code": returncode,
                    "killed": self.killed,
                    "success": returncode == 0,
                },
            )
            if returncode:
                if not self.killed:
                    if returncode == -11:
                        self.emit_log(
                            b"Task failed with a segmentation fault.",
                            self._stderr,
                            system_msg=True,
                        )
                    else:
                        self.emit_log(b"Task failed.", self._stderr, system_msg=True)
            else:
                num = self.task.results["_foreach_num_splits"]
                if num:
                    self.task.log(
                        "Foreach yields %d child steps." % num,
                        system_msg=True,
                        pid=self._proc.pid,
                    )
                self.task.log(
                    "Task finished successfully.", system_msg=True, pid=self._proc.pid
                )
            self.task.save_logs(
                {
                    "stdout": self._stdout.get_buffer(),
                    "stderr": self._stderr.get_buffer(),
                }
            )

        return returncode

    def __str__(self):
        return "Worker[%d]: %s" % (self._proc.pid, self.task.path)
