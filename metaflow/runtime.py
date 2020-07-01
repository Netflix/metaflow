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
import time
import select
import subprocess
from functools import partial

from . import get_namespace
from .metaflow_config import MAX_ATTEMPTS
from .exception import MetaflowException,\
    MetaflowInternalError,\
    METAFLOW_EXIT_DISALLOW_RETRY
from . import procpoll
from .datastore import DataException, MetaflowDatastoreSet
from .metadata import MetaDatum
from .debug import debug

from .util import to_unicode, compress_list
try:
    # python2
    import cStringIO
    BytesIO = cStringIO.StringIO
except:
    # python3
    import io
    BytesIO = io.BytesIO

MAX_WORKERS=16
MAX_NUM_SPLITS=100
MAX_LOG_SIZE=1024*1024
PROGRESS_INTERVAL = 1000 #ms
# The following is a list of the (data) artifacts used by the runtime while
# executing a flow. These are prefetched during the resume operation by
# leveraging the MetaflowDatastoreSet.
PREFETCH_DATA_ARTIFACTS = ['_foreach_stack', '_task_ok', '_transition']

# TODO option: output dot graph periodically about execution

class NativeRuntime(object):

    def __init__(self,
                 flow,
                 graph,
                 datastore,
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
                 max_log_size=MAX_LOG_SIZE):

        if run_id is None:
            self._run_id = metadata.new_run_id()
        else:
            self._run_id = run_id
            metadata.register_run_id(run_id)

        self._flow = flow
        self._graph = graph
        self._datastore = datastore
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
            # leverage the MetaflowDatastoreSet abstraction to prefetch the
            # entire DAG of `clone_run_id` and relevant data artifacts
            # (see PREFETCH_DATA_ARTIFACTS) so that the entire runtime can
            # access the relevant data from cache (instead of going to the datastore
            # after the first prefetch).
            logger(
                'Gathering required information to resume run (this may take a bit of time)...')
            self._origin_ds_set = \
                MetaflowDatastoreSet(
                    datastore,
                    flow.name,
                    clone_run_id,
                    metadata=metadata,
                    event_logger=event_logger,
                    monitor=monitor,
                    prefetch_data_artifacts=PREFETCH_DATA_ARTIFACTS)
        self._run_queue = []
        self._poll = procpoll.make_poll()
        self._workers = {}  # fd -> subprocess mapping
        self._finished = {}
        self._is_cloned = {}

        for step in flow:
            for deco in step.decorators:
                deco.runtime_init(flow,
                                  graph,
                                  package,
                                  self._run_id)

    def _new_task(self, step, input_paths=None, **kwargs):

        if input_paths is None:
            may_clone = True
        else:
            may_clone = all(self._is_cloned[path] for path in input_paths)

        if step in self._clone_steps:
            may_clone = False

        if step == '_parameters':
            decos = []
        else:
            decos = getattr(self._flow, step).decorators

        return Task(self._datastore,
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
                    **kwargs)

    @property
    def run_id(self):
        return self._run_id

    def persist_parameters(self, task_id=None):
        task = self._new_task('_parameters', task_id=task_id)
        if not task.is_cloned:
            task.persist(self._flow)
        self._params_task = task.path
        self._is_cloned[task.path] = task.is_cloned

    def execute(self):

        self._logger('Workflow starting (run-id %s):' % self._run_id,
                     system_msg=True)

        if self._params_task:
            self._queue_push('start', {'input_paths': [self._params_task]})
        else:
            self._queue_push('start', {})

        progress_tstamp = time.time()
        try:
            # main scheduling loop
            exception = None
            while self._run_queue or\
                    self._num_active_workers > 0:

                # 1. are any of the current workers finished?
                finished_tasks = list(self._poll_workers())
                # 2. push new tasks triggered by the finished tasks to the queue
                self._queue_tasks(finished_tasks)
                # 3. if there are available worker slots, pop and start tasks
                #    from the queue.
                self._launch_workers()

                if time.time() - progress_tstamp > PROGRESS_INTERVAL:
                    progress_tstamp = time.time()
                    msg = "%d tasks are running: %s." %\
                          (self._num_active_workers, 'e.g. ...')  # TODO
                    self._logger(msg, system_msg=True)
                    msg = "%d tasks are waiting in the queue." %\
                          len(self._run_queue)
                    self._logger(msg, system_msg=True)
                    msg = "%d steps are pending: %s." %\
                          (0, 'e.g. ...')  # TODO
                    self._logger(msg, system_msg=True)

        except KeyboardInterrupt as ex:
            self._logger('Workflow interrupted.', system_msg=True, bad=True)
            self._killall()
            exception = ex
            raise
        except Exception as ex:
            self._logger('Workflow failed.', system_msg=True, bad=True)
            self._killall()
            exception = ex
            raise
        finally:
            # on finish clean tasks
            for step in self._flow:
                for deco in step.decorators:
                    deco.runtime_finished(exception)

        # assert that end was executed and it was successful
        if ('end', ()) in self._finished:
            self._logger('Done!', system_msg=True)
        else:
            raise MetaflowInternalError('The *end* step was not successful '
                                        'by the end of flow.')

    def _killall(self):
        # If we are here, all children have received a signal and are shutting down.
        # We want to give them an opportunity to do so and then kill
        live_workers = set(self._workers.values())
        now = int(time.time())
        self._logger('Terminating %d active tasks...' % len(live_workers),
                     system_msg=True, bad=True)
        while live_workers and int(time.time()) - now < 5:
            # While not all workers are dead and we have waited less than 5 seconds
            live_workers = [worker for worker in live_workers if not worker.clean()]
        if live_workers:
            self._logger('Killing %d remaining tasks after having waited for %d seconds -- '
                         'some tasks may not exit cleanly' % (len(live_workers),
                                                              int(time.time()) - now),
                         system_msg=True, bad=True)
            for worker in live_workers:
                worker.kill()
        self._logger('Flushing logs...', system_msg=True, bad=True)
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
            msg = 'Step *{step}* transitions to a join and another '\
                  'step. The join must be the only transition.'
            raise MetaflowInternalError(task, msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        # matching_split is the split-parent of the finished task
        matching_split = self._graph[self._graph[next_step].split_parents[-1]]
        step_name, foreach_stack = task.finished_id

        if matching_split.type == 'foreach':
            # next step is a foreach join

            def siblings(foreach_stack):
                top = foreach_stack[-1]
                bottom = list(foreach_stack[:-1])
                for index in range(top.num_splits):
                    yield tuple(bottom + [top._replace(index=index)])

            # required tasks are all split-siblings of the finished task
            required_tasks = [self._finished.get((task.step, s))
                              for s in siblings(foreach_stack)]
            join_type = 'foreach'
        else:
            # next step is a split-and
            # required tasks are all branches joined by the next step
            required_tasks = [self._finished.get((step, foreach_stack))
                              for step in self._graph[next_step].in_funcs]
            join_type = 'linear'

        if all(required_tasks):
            # all tasks to be joined are ready. Schedule the next join step.
            self._queue_push(next_step,
                             {'input_paths': required_tasks,
                              'join_type': join_type})

    def _queue_task_foreach(self, task, next_steps):

        # CHECK: this condition should be enforced by the linter but
        # let's assert that the assumption holds
        if len(next_steps) > 1:
            msg = 'Step *{step}* makes a foreach split but it defines '\
                  'multiple transitions. Specify only one transition '\
                  'for foreach.'
            raise MetaflowInternalError(msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        num_splits = task.results['_foreach_num_splits']
        if num_splits > self._max_num_splits:
            msg = 'Foreach in step *{step}* yielded {num} child steps '\
                  'which is more than the current maximum of {max} '\
                  'children. You can raise the maximum with the '\
                  '--max-num-splits option. '
            raise TaskFailed(task, msg.format(step=task.step,
                                              num=num_splits,
                                              max=self._max_num_splits))

        # schedule all splits
        for i in range(num_splits):
            self._queue_push(next_step,
                             {'split_index': str(i),
                              'input_paths': [task.path]})

    def _queue_tasks(self, finished_tasks):
        # finished tasks include only successful tasks
        for task in finished_tasks:
            self._finished[task.finished_id] = task.path
            self._is_cloned[task.path] = task.is_cloned

            # CHECK: ensure that runtime transitions match with
            # statically inferred transitions
            trans = task.results.get('_transition')
            if trans:
                next_steps = trans[0]
                foreach = trans[1]
            else:
                next_steps = []
                foreach = None
            expected = self._graph[task.step].out_funcs
            if next_steps != expected:
                msg = 'Based on static analysis of the code, step *{step}* '\
                      'was expected to transition to step(s) *{expected}*. '\
                      'However, when the code was executed, self.next() was '\
                      'called with *{actual}*. Make sure there is only one '\
                      'unconditional self.next() call in the end of your '\
                      'step. '
                raise MetaflowInternalError(msg.format(step=task.step,
                                                       expected=', '.join(
                                                           expected),
                                                       actual=', '.join(next_steps)))

            # Different transition types require different treatment
            if any(self._graph[f].type == 'join' for f in next_steps):
                # Next step is a join
                self._queue_task_join(task, next_steps)
            elif foreach:
                # Next step is a foreach child
                self._queue_task_foreach(task, next_steps)
            else:
                # Next steps are normal linear steps
                for step in next_steps:
                    self._queue_push(step, {'input_paths': [task.path]})

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
                            if worker.cleaned or \
                               returncode == METAFLOW_EXIT_DISALLOW_RETRY:
                                self._logger("This failed task will not be "
                                             "retried.", system_msg=True)
                            else:
                                if task.retries < task.user_code_retries +\
                                        task.error_retries:
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
            raise MetaflowInternalError("Too many task attempts (%d)! "
                                        "MAX_ATTEMPTS exceeded."
                                        % worker.task.retries)

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

    def __init__(self,
                 datastore,
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
                 clone_run_id=None,
                 origin_ds_set=None,
                 may_clone=False,
                 join_type=None,
                 logger=None,
                 task_id=None,
                 decos=[]):
        if task_id is None:
            task_id = str(metadata.new_task_id(run_id, step))
        else:
            # task_id is preset only by persist_parameters()
            metadata.register_task_id(run_id, step, task_id)

        self.step = step
        self.flow_name = flow.name
        self.run_id = run_id
        self.task_id = task_id
        self.input_paths = input_paths
        self.split_index = split_index
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
        self._path = '%s/%s/%s' % (self.run_id, self.step, self.task_id)

        self.retries = 0
        self.user_code_retries = 0
        self.error_retries = 0

        self.tags = metadata.sticky_tags
        self.event_logger_type = self.event_logger.logger_type
        self.monitor_type = monitor.monitor_type

        self.metadata_type = metadata.TYPE
        self.datastore_type = datastore.TYPE
        self._datastore = datastore
        self.datastore_sysroot = datastore.datastore_root
        self._results_ds = None

        if clone_run_id and may_clone:
            self._is_cloned = self._attempt_clone(clone_run_id, join_type)
        else:
            self._is_cloned = False

        # Open the output datastore only if the task is not being cloned.
        if not self._is_cloned:
            self.new_attempt()

            for deco in decos:
                deco.runtime_task_created(self._ds,
                                          task_id,
                                          split_index,
                                          input_paths,
                                          self._is_cloned)

                # determine the number of retries of this task
                user_code_retries, error_retries = deco.step_task_retry_count()
                self.user_code_retries = max(self.user_code_retries,
                                             user_code_retries)
                self.error_retries = max(self.error_retries, error_retries)

    def new_attempt(self):
        self._ds = self._datastore(self.flow_name,
                                   run_id=self.run_id,
                                   step_name=self.step,
                                   task_id=self.task_id,
                                   mode='w',
                                   metadata=self.metadata,
                                   attempt=self.retries,
                                   event_logger=self.event_logger,
                                   monitor=self.monitor)


    def log(self, msg, system_msg=False, pid=None):
        if pid:
            prefix = '[%s (pid %s)] ' % (self._path, pid)
        else:
            prefix = '[%s] ' % self._path

        self._logger(msg, head=prefix, system_msg=system_msg)
        sys.stdout.flush()

    def _find_origin_task(self, clone_run_id, join_type):
        if self.step == '_parameters':
            pathspec = '%s/_parameters[]' % clone_run_id
            origin = self.origin_ds_set.get_with_pathspec_index(pathspec)

            if origin is None:
                # This is just for usability: We could rerun the whole flow
                # if an unknown clone_run_id is provided but probably this is
                # not what the user intended, so raise a warning
                raise MetaflowException("Resume could not find run id *%s*" %
                                        clone_run_id)
            else:
                return origin
        else:
            # all inputs must have the same foreach stack, so we can safely
            # pick the first one
            parent_pathspec = self.input_paths[0]
            origin_parent_pathspec = \
                self.clone_pathspec_mapping[parent_pathspec]
            parent = self.origin_ds_set.get_with_pathspec(origin_parent_pathspec)
            # Parent should be non-None since only clone the child if the parent
            # was successfully cloned.
            foreach_stack = parent['_foreach_stack']
            if join_type == 'foreach':
                # foreach-join pops the topmost index
                index = ','.join(str(s.index) for s in foreach_stack[:-1])
            elif self.split_index:
                # foreach-split pushes a new index
                index = ','.join([str(s.index) for s in foreach_stack] +
                                 [str(self.split_index)])
            else:
                # all other transitions keep the parent's foreach stack intact
                index = ','.join(str(s.index) for s in foreach_stack)
            pathspec = '%s/%s[%s]' % (clone_run_id, self.step, index)
            return self.origin_ds_set.get_with_pathspec_index(pathspec)

    def _attempt_clone(self, clone_run_id, join_type):
        origin = self._find_origin_task(clone_run_id, join_type)

        if origin and origin['_task_ok']:
            # Store the mapping from current_pathspec -> origin_pathspec which
            # will be useful for looking up origin_ds_set in find_origin_task.
            self.clone_pathspec_mapping[self._path] = origin.pathspec
            if self.step == '_parameters':
                # Clone in place without relying on run_queue.
                self.new_attempt()
                self._ds.clone(origin)
                self._ds.done()
            else:
                self.log("Cloning results of a previously run task %s"
                         % origin.pathspec, system_msg=True)
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
            self._results_ds = self._datastore(self.flow_name,
                                               run_id=self.run_id,
                                               step_name=self.step,
                                               task_id=self.task_id,
                                               mode='r',
                                               metadata=self.metadata,
                                               event_logger=self.event_logger,
                                               monitor=self.monitor)
            return self._results_ds

    @property
    def finished_id(self):
        # note: id is not available before the task has finished
        return (self.step, tuple(self.results['_foreach_stack']))

    @property
    def is_cloned(self):
        return self._is_cloned

    def persist(self, flow):
        # this is used to persist parameters before the start step
        flow._task_ok = flow._success = True
        flow._foreach_stack = []
        self._ds.persist(flow)
        self._ds.done()

    def save_logs(self, logtype, logs):
        location = self._ds.save_log(logtype, logs)
        datum = [MetaDatum(field='log_location_%s' % logtype,
                           value=json.dumps({
                               'ds_type': self._ds.TYPE,
                               'location': location,
                               'attempt': self.retries}),
                           type='log_path')]
        self.metadata.register_metadata(self.run_id,
                                        self.step,
                                        self.task_id,
                                        datum)
        return location

    def save_metadata(self, name, metadata):
        self._ds.save_metadata(name, metadata)

    def __str__(self):
        return ' '.join(self._args)


class TaskFailed(MetaflowException):
    headline = "Step failure"

    def __init__(self, task, msg=''):
        body = "Step *%s* (task-id %s) failed" % (task.step,
                                                  task.task_id)
        if msg:
            body = '%s: %s' % (body, msg)
        else:
            body += '.'

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
                msg = b'[TRUNCATED - MAXIMUM LOG FILE SIZE REACHED]\n'
                self._buffer.write(msg)
                self._eof = True

    def get_bytes(self):
        return self._buffer.getvalue()


class CLIArgs(object):
    """
    Container to allow decorators modify the command line parameters
    for step execution in StepDecorator.runtime_step_cli().
    """

    def __init__(self, task):
        self.task = task
        self.entrypoint = list(task.entrypoint)
        self.top_level_options = {
            'quiet': True,
            'coverage': 'coverage' in sys.modules,
            'metadata': self.task.metadata_type,
            'environment': self.task.environment_type,
            'datastore': self.task.datastore_type,
            'event-logger': self.task.event_logger_type,
            'monitor': self.task.monitor_type,
            'datastore-root': self.task.datastore_sysroot,
            'with': [deco.make_decorator_spec() for deco in self.task.decos
                     if not deco.statically_defined]
        }
        self.commands = ['step']
        self.command_args = [self.task.step]
        self.command_options = {
            'run-id': task.run_id,
            'task-id': task.task_id,
            'input-paths': compress_list(task.input_paths),
            'split-index': task.split_index,
            'retry-count': task.retries,
            'max-user-code-retries': task.user_code_retries,
            'tag': task.tags,
            'namespace': get_namespace() or ''
        }
        self.env = {}

    def get_args(self):
        def options(mapping):
            for k, v in mapping.items():
                values = v if isinstance(v, list) else [v]
                for value in values:
                    if value:
                        yield '--%s' % k
                        if not isinstance(value, bool):
                            yield to_unicode(value)

        args = list(self.entrypoint)
        args.extend(options(self.top_level_options))
        args.extend(self.commands)
        args.extend(self.command_args)
        args.extend(options(self.command_options))
        return args

    def get_env(self):
        return self.env

    def __str__(self):
        return ' '.join(self.get_args())


class Worker(object):

    def __init__(self, task, max_logs_size):

        self.task = task
        self._proc = self._launch()

        if task.retries > task.user_code_retries:
            self.task.log('Task fallback is starting to handle the failure.',
                          system_msg=True,
                          pid=self._proc.pid)
        elif not task.is_cloned:
            suffix = ' (retry).' if task.retries else '.'
            self.task.log('Task is starting' + suffix,
                          system_msg=True,
                          pid=self._proc.pid)

        self._stdout = TruncatedBuffer('stdout', max_logs_size)
        self._stderr = TruncatedBuffer('stderr', max_logs_size)

        self._logs = {self._proc.stderr.fileno(): (self._proc.stderr,
                                                   self._stderr),
                      self._proc.stdout.fileno(): (self._proc.stdout,
                                                   self._stdout)}

        self._encoding = sys.stdout.encoding or 'UTF-8'
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
            args.command_options['clone-run-id'] = self.task.clone_run_id

        if self.task.is_cloned and self.task.clone_origin:
            args.command_options['clone-only'] = self.task.clone_origin
            # disabling atlas sidecar for cloned tasks due to perf reasons
            args.top_level_options['monitor'] = 'nullSidecarMonitor'
        else:
            # decorators may modify the CLIArgs object in-place
            for deco in self.task.decos:
                deco.runtime_step_cli(args,
                                      self.task.retries,
                                      self.task.user_code_retries)
        env.update(args.get_env())
        # the env vars are needed by the test framework, nothing else
        env['_METAFLOW_ATTEMPT'] = str(self.task.retries)
        if self.task.clone_run_id:
            env['_METAFLOW_RESUMED_RUN'] = '1'
            env['_METAFLOW_RESUME_ORIGIN_RUN_ID'] = str(self.task.clone_run_id)
        # NOTE bufsize=1 below enables line buffering which is required
        # by read_logline() below that relies on readline() not blocking
        # print('running', args)
        cmdline = args.get_args()
        debug.subcommand_exec(cmdline)
        return subprocess.Popen(cmdline,
                                env=env,
                                bufsize=1,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)

    def write(self, msg, buf):
        buf.write(msg)
        text = msg.rstrip().decode(self._encoding, errors='replace')
        self.task.log(text, pid=self._proc.pid)

    def read_logline(self, fd):
        fileobj, buf = self._logs[fd]
        # readline() below should never block thanks to polling and
        # line buffering. If it does, things will deadlock
        line = fileobj.readline()
        if line:
            self.write(line, buf)
            return True
        else:
            return False

    def fds(self):
        return (self._proc.stderr.fileno(),
                self._proc.stdout.fileno())

    def clean(self):
        if self.killed:
            return True
        if not self.cleaned:
            for fileobj, buf in self._logs.values():
                buf.write(b'[KILLED BY ORCHESTRATOR]\n', system_msg=True)
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
            self.task.save_logs('stdout', self._stdout.get_bytes())
            self.task.save_logs('stderr', self._stderr.get_bytes())

            self.task.save_metadata('runtime', {'return_code': returncode,
                                                'killed': self.killed,
                                                'success': returncode == 0})
            if returncode:
                if not self.killed:
                    self.task.log('Task failed.',
                                  system_msg=True,
                                  pid=self._proc.pid)
            else:
                num = self.task.results['_foreach_num_splits']
                if num:
                    self.task.log('Foreach yields %d child steps.' % num,
                                  system_msg=True,
                                  pid=self._proc.pid)
                self.task.log('Task finished successfully.',
                              system_msg=True,
                              pid=self._proc.pid)

        return returncode

    def __str__(self):
        return 'Worker[%d]: %s' % (self._proc.pid, self.task.path)
