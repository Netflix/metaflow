"""
Local backend

Execute the flow with a native runtime
using local / remote processes
"""
from __future__ import print_function
import os
import sys
import fcntl
import time
import subprocess
from datetime import datetime
from io import BytesIO
from functools import partial

from . import get_namespace
from .metadata import MetaDatum
from .metaflow_config import MAX_ATTEMPTS
from .exception import (
    MetaflowException,
    MetaflowInternalError,
    METAFLOW_EXIT_DISALLOW_RETRY,
)
from . import procpoll
from .datastore import TaskDataStoreSet
from .debug import debug
from .decorators import flow_decorators
from .mflog import mflog, RUNTIME_LOG_SOURCE
from .util import to_unicode, compress_list, unicode_type
from .unbounded_foreach import (
    CONTROL_TASK_TAG,
    UBF_CONTROL,
    UBF_TASK,
)

MAX_WORKERS = 16
MAX_NUM_SPLITS = 100
MAX_LOG_SIZE = 1024 * 1024
PROGRESS_INTERVAL = 1000  # ms
# The following is a list of the (data) artifacts used by the runtime while
# executing a flow. These are prefetched during the resume operation by
# leveraging the TaskDataStoreSet.
PREFETCH_DATA_ARTIFACTS = ["_foreach_stack", "_task_ok", "_transition"]

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
        clone_steps=None,
        max_workers=MAX_WORKERS,
        max_num_splits=MAX_NUM_SPLITS,
        max_log_size=MAX_LOG_SIZE,
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
        self._logger = logger
        self._max_workers = max_workers
        self._num_active_workers = 0
        self._max_num_splits = max_num_splits
        self._max_log_size = max_log_size
        self._params_task = None
        self._entrypoint = entrypoint
        self.event_logger = event_logger
        self._monitor = monitor

        self._clone_run_id = clone_run_id
        self._clone_steps = {} if clone_steps is None else clone_steps

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

        for step in flow:
            for deco in step.decorators:
                deco.runtime_init(flow, graph, package, self._run_id)

    def _new_task(self, step, input_paths=None, **kwargs):

        if input_paths is None:
            may_clone = True
        else:
            may_clone = all(self._is_cloned[path] for path in input_paths)

        if step in self._clone_steps:
            may_clone = False

        if step == "_parameters":
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
            origin_ds_set=self._origin_ds_set,
            decos=decos,
            logger=self._logger,
            **kwargs
        )

    @property
    def run_id(self):
        return self._run_id

    def persist_constants(self, task_id=None):
        task = self._new_task("_parameters", task_id=task_id)
        if not task.is_cloned:
            task.persist(self._flow)
        self._params_task = task.path
        self._is_cloned[task.path] = task.is_cloned

    def execute(self):

        self._logger("Workflow starting (run-id %s):" % self._run_id, system_msg=True)

        self._metadata.start_run_heartbeat(self._flow.name, self._run_id)

        if self._params_task:
            self._queue_push("start", {"input_paths": [self._params_task]})
        else:
            self._queue_push("start", {})

        progress_tstamp = time.time()
        try:
            # main scheduling loop
            exception = None
            while self._run_queue or self._num_active_workers > 0:

                # 1. are any of the current workers finished?
                finished_tasks = list(self._poll_workers())
                # 2. push new tasks triggered by the finished tasks to the queue
                self._queue_tasks(finished_tasks)
                # 3. if there are available worker slots, pop and start tasks
                #    from the queue.
                self._launch_workers()

                if time.time() - progress_tstamp > PROGRESS_INTERVAL:
                    progress_tstamp = time.time()
                    msg = "%d tasks are running: %s." % (
                        self._num_active_workers,
                        "e.g. ...",
                    )  # TODO
                    self._logger(msg, system_msg=True)
                    msg = "%d tasks are waiting in the queue." % len(self._run_queue)
                    self._logger(msg, system_msg=True)
                    msg = "%d steps are pending: %s." % (0, "e.g. ...")  # TODO
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
            for step in self._flow:
                for deco in step.decorators:
                    deco.runtime_finished(exception)

            self._metadata.stop_heartbeat()

        # assert that end was executed and it was successful
        if ("end", ()) in self._finished:
            self._logger("Done!", system_msg=True)
        else:
            raise MetaflowInternalError(
                "The *end* step was not successful " "by the end of flow."
            )

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

    # Store the parameters needed for task creation, so that pushing on items
    # onto the run_queue is an inexpensive operation.
    def _queue_push(self, step, task_kwargs):
        self._run_queue.insert(0, (step, task_kwargs))

    def _queue_pop(self):
        return self._run_queue.pop() if self._run_queue else None

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
                if task.is_cloned:
                    # Add mapper tasks to be cloned.
                    for i in range(num_splits):
                        # NOTE: For improved robustness, introduce
                        # `clone_options` as an enum so that we can force that
                        # clone must occur for this task.
                        self._queue_push(
                            task.step,
                            {
                                "input_paths": task.input_paths,
                                "split_index": str(i),
                                "ubf_context": UBF_TASK,
                            },
                        )
                else:
                    # Update _finished since these tasks were successfully
                    # run elsewhere so that join will be unblocked.
                    step_name, foreach_stack = task.finished_id
                    top = foreach_stack[-1]
                    bottom = list(foreach_stack[:-1])
                    for i in range(num_splits):
                        s = tuple(bottom + [top._replace(index=i)])
                        self._finished[(task.step, s)] = mapper_tasks[i]
                        self._is_cloned[mapper_tasks[i]] = False

            # Find and check status of control task and retrieve its pathspec
            # for retrieving unbounded foreach cardinality.
            step_name, foreach_stack = task.finished_id
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
                    # all tasks to be joined are ready. Schedule the next join step.
                    self._queue_push(
                        next_step,
                        {"input_paths": required_tasks, "join_type": "foreach"},
                    )
        else:
            # matching_split is the split-parent of the finished task
            matching_split = self._graph[self._graph[next_step].split_parents[-1]]
            step_name, foreach_stack = task.finished_id

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
            else:
                # next step is a split
                # required tasks are all branches joined by the next step
                required_tasks = [
                    self._finished.get((step, foreach_stack))
                    for step in self._graph[next_step].in_funcs
                ]
                join_type = "linear"

            if all(required_tasks):
                # all tasks to be joined are ready. Schedule the next join step.
                self._queue_push(
                    next_step, {"input_paths": required_tasks, "join_type": join_type}
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
            self._queue_push(
                next_step,
                {
                    "input_paths": [task.path],
                    "ubf_context": UBF_CONTROL,
                    "ubf_iter": ubf_iter,
                },
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
                self._queue_push(
                    next_step, {"split_index": str(i), "input_paths": [task.path]}
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
                    self._queue_push(step, {"input_paths": [task.path]})

    def _poll_workers(self):
        if self._workers:
            for event in self._poll.poll(PROGRESS_INTERVAL):
                worker = self._workers.get(event.fd)
                if worker:
                    if event.can_read:
                        worker.read_logline(event.fd)
                    if event.is_terminated:
                        returncode = worker.terminate()

                        for fd in worker.fds():
                            self._poll.remove(fd)
                            del self._workers[fd]
                        self._num_active_workers -= 1

                        task = worker.task
                        if returncode:
                            # worker did not finish successfully
                            if (
                                worker.cleaned
                                or returncode == METAFLOW_EXIT_DISALLOW_RETRY
                            ):
                                self._logger(
                                    "This failed task will not be " "retried.",
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
        while self._run_queue and self._num_active_workers < self._max_workers:
            step, task_kwargs = self._queue_pop()
            # Initialize the task (which can be expensive using remote datastores)
            # before launching the worker so that cost is amortized over time, instead
            # of doing it during _queue_push.
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
        worker = Worker(task, self._max_log_size)
        for fd in worker.fds():
            self._workers[fd] = worker
            self._poll.add(fd)
        self._num_active_workers += 1


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
        split_index=None,
        ubf_context=None,
        ubf_iter=None,
        clone_run_id=None,
        origin_ds_set=None,
        may_clone=False,
        join_type=None,
        logger=None,
        task_id=None,
        decos=[],
    ):
        if ubf_context == UBF_CONTROL:
            [input_path] = input_paths
            run, input_step, input_task = input_path.split("/")
            # We associate the control task-id to be 1:1 with the split node
            # where the unbounded-foreach was defined.
            # We prefer encoding the corresponding split into the task_id of
            # the control node; so it has access to this information quite
            # easily. There is anyway a corresponding int id stored in the
            # metadata backend - so this should be fine.
            task_id = "control-%s-%s-%s" % (run, input_step, input_task)
        # Register only regular Metaflow (non control) tasks.
        if task_id is None:
            task_id = str(metadata.new_task_id(run_id, step))
        else:
            # task_id is preset only by persist_constants() or control tasks.
            if ubf_context == UBF_CONTROL:
                tags = [CONTROL_TASK_TAG]
                attempt_id = 0
                metadata.register_task_id(
                    run_id,
                    step,
                    task_id,
                    attempt_id,
                    sys_tags=tags,
                )
                # As part of tag mutation project, we will be overlaying Run's tags
                # (user and system) on top of Task tags.  I.e. a Task will no longer have
                # its own tags distinct from their ancestral Run.  It means we should not
                # use tags to indicate if a task is a control task in future.
                #
                # This is the main PR that implements the same "stashing to task metadata"
                # concept:
                #     https://github.com/Netflix/metaflow/pull/1039
                #
                # Here we will also add a task metadata entry to indicate "control task".
                # Within the metaflow repo, the only dependency of such a "control task"
                # indicator is in the integration test suite (see Step.control_tasks() in
                # client API).
                task_metadata_list = [
                    MetaDatum(
                        field="internal_task_type",
                        value=CONTROL_TASK_TAG,
                        type="internal_task_type",
                        tags=["attempt_id:{0}".format(attempt_id)],
                    )
                ]
                metadata.register_metadata(run_id, step, task_id, task_metadata_list)
            else:
                metadata.register_task_id(run_id, step, task_id, 0)

        self.step = step
        self.flow_name = flow.name
        self.run_id = run_id
        self.task_id = task_id
        self.input_paths = input_paths
        self.split_index = split_index
        self.ubf_context = ubf_context
        self.ubf_iter = ubf_iter
        self.decos = decos
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
        self._path = "%s/%s/%s" % (self.run_id, self.step, self.task_id)

        self.retries = 0
        self.user_code_retries = 0
        self.error_retries = 0

        self.tags = metadata.sticky_tags
        self.event_logger_type = self.event_logger.logger_type
        self.monitor_type = monitor.monitor_type

        self.metadata_type = metadata.TYPE
        self.datastore_type = flow_datastore.TYPE
        self._flow_datastore = flow_datastore
        self.datastore_sysroot = flow_datastore.datastore_root
        self._results_ds = None

        if clone_run_id and may_clone:
            self._is_cloned = self._attempt_clone(clone_run_id, join_type)
        else:
            self._is_cloned = False

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

    def _find_origin_task(self, clone_run_id, join_type):
        if self.step == "_parameters":
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

    def _attempt_clone(self, clone_run_id, join_type):
        origin = self._find_origin_task(clone_run_id, join_type)

        if origin and origin["_task_ok"]:
            # Store the mapping from current_pathspec -> origin_pathspec which
            # will be useful for looking up origin_ds_set in find_origin_task.
            self.clone_pathspec_mapping[self._path] = origin.pathspec
            if self.step == "_parameters":
                # Clone in place without relying on run_queue.
                self.new_attempt()
                self._ds.clone(origin)
                self._ds.done()
            else:
                self.log(
                    "Cloning results of a previously run task %s" % origin.pathspec,
                    system_msg=True,
                )
                # Store the origin pathspec in clone_origin so this can be run
                # as a task by the runtime.
                self.clone_origin = origin.pathspec
                # Save a call to creating the results_ds since its same as origin.
                self._results_ds = origin
            return True
        else:
            return False

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
    def finished_id(self):
        # note: id is not available before the task has finished
        return (self.step, tuple(self.results["_foreach_stack"]))

    @property
    def is_cloned(self):
        return self._is_cloned

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
        self.top_level_options = {
            "quiet": True,
            "metadata": self.task.metadata_type,
            "environment": self.task.environment_type,
            "datastore": self.task.datastore_type,
            "event-logger": self.task.event_logger_type,
            "monitor": self.task.monitor_type,
            "datastore-root": self.task.datastore_sysroot,
            "with": [
                deco.make_decorator_spec()
                for deco in self.task.decos
                if not deco.statically_defined
            ],
        }

        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook.
        for deco in flow_decorators():
            self.top_level_options.update(deco.get_top_level_options())

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
                # v needs to be explicitly False, not falsy, eg. 0 is an acceptable value
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
                        yield to_unicode(value)

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
    def __init__(self, task, max_logs_size):

        self.task = task
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
        # not is is properly shut down)

    def _launch(self):
        args = CLIArgs(self.task)
        env = dict(os.environ)

        if self.task.clone_run_id:
            args.command_options["clone-run-id"] = self.task.clone_run_id

        if self.task.is_cloned and self.task.clone_origin:
            args.command_options["clone-only"] = self.task.clone_origin
            # disabling atlas sidecar for cloned tasks due to perf reasons
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
        env.update(args.get_env())
        env["PYTHONUNBUFFERED"] = "x"
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
                    # This is the case e.g. if a task crashes in an external
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
