from __future__ import print_function
import sys
import os
import time

from .metaflow_config import MAX_ATTEMPTS
from .metadata import MetaDatum
from .datastore import Inputs, MetaflowDatastoreSet
from .exception import MetaflowInternalError,\
    MetaflowDataMissing,\
    MetaflowExceptionWrapper
from .util import all_equal,\
    get_username,\
    resolve_identity
from .current import current

from collections import namedtuple

ForeachFrame = namedtuple('ForeachFrame',
                          ['step', 'var', 'num_splits', 'index'])


class MetaflowTask(object):
    """
    MetaflowTask prepares a Flow instance for execution of a single step.
    """

    def __init__(self,
                 flow,
                 datastore,
                 metadata,
                 environment,
                 console_logger,
                 event_logger,
                 monitor):
        self.flow = flow
        self.datastore = datastore
        self.metadata = metadata
        self.environment = environment
        self.console_logger = console_logger
        self.event_logger = event_logger
        self.monitor = monitor

    def _exec_step_function(self, step_function, input_obj=None):
        self.environment.validate_environment(logger=self.console_logger)
        if input_obj is None:
            step_function()
        else:
            step_function(input_obj)

    def _init_parameters(self, parameter_ds):
        # overwrite Parameters in the flow object
        vars = []
        for var, param in self.flow._get_parameters():
            # make the parameter a read-only property
            # note x=x binds the current value of x to the closure
            def property_setter(
                    _, cls=self.flow.__class__, param=param, var=var, parameter_ds=parameter_ds):
                v = param.load_parameter(parameter_ds[var])
                setattr(cls, var, property(fget=lambda _, val=v: val))
                return v

            setattr(self.flow.__class__, var,
                    property(fget=property_setter))
            vars.append(var)
        self.flow._datastore.passdown_partial(parameter_ds, vars)
        return vars

    def _init_data(self, run_id, join_type, input_paths):
        # We prefer to use the parallelized version to initialize datastores
        # (via MetaflowDatastoreSet) only with more than 4 datastores, because
        # the baseline overhead of using the set is ~1.5s and each datastore
        # init takes ~200-300ms when run sequentially.
        if len(input_paths) > 4:
            prefetch_data_artifacts = None
            if join_type and join_type == 'foreach':
                # Prefetch 'foreach' related artifacts to improve time taken by
                # _init_foreach.
                prefetch_data_artifacts = \
                    ['_foreach_stack', '_foreach_num_splits', '_foreach_var']
            # Note: Specify `pathspecs` while creating the datastore set to
            # guarantee strong consistency and guard against missing input.
            datastore_set = \
                MetaflowDatastoreSet(self.datastore,
                                     self.flow.name,
                                     run_id,
                                     pathspecs=input_paths,
                                     metadata=self.metadata,
                                     event_logger=self.event_logger,
                                     monitor=self.monitor,
                                     prefetch_data_artifacts=prefetch_data_artifacts)
            ds_list = [ds for ds in datastore_set]
            if len(ds_list) != len(input_paths):
                raise MetaflowDataMissing("Some input datastores are missing. "
                                          "Expected: %d Actual: %d" %
                                          (len(input_paths), len(ds_list)))
        else:
            # initialize directly in the single input case.
            ds_list = []
            for input_path in input_paths:
                run_id, step_name, task_id = input_path.split('/')
                ds_list.append(
                    self.datastore(self.flow.name,
                                   run_id=run_id,
                                   step_name=step_name,
                                   task_id=task_id,
                                   metadata=self.metadata,
                                   event_logger=self.event_logger,
                                   monitor=self.monitor))
        if not ds_list:
            # this guards against errors in input paths
            raise MetaflowDataMissing("Input paths *%s* resolved to zero "
                                      "inputs" % ','.join(input_paths))
        return ds_list

    def _init_foreach(self, step_name, join_type, inputs, split_index):
        # these variables are only set by the split step in the output
        # data. They don't need to be accessible in the flow.
        self.flow._foreach_var = None
        self.flow._foreach_num_splits = None

        # There are three cases that can alter the foreach state:
        # 1) start - initialize an empty foreach stack
        # 2) join - pop the topmost frame from the stack
        # 3) step following a split - push a new frame in the stack

        # case 1) - reset the stack
        if step_name == 'start':
            self.flow._foreach_stack = []

        # case 2) - this is a join step
        elif join_type:
            # assert the lineage of incoming branches
            def lineage():
                for i in inputs:
                    if join_type == 'foreach':
                        top = i['_foreach_stack'][-1]
                        bottom = i['_foreach_stack'][:-1]
                        # the topmost indices in the stack are all
                        # different naturally, so ignore them in the
                        # assertion
                        yield bottom + [top._replace(index=0)]
                    else:
                        yield i['_foreach_stack']

            if not all_equal(lineage()):
                raise MetaflowInternalError("Step *%s* tried to join branches "
                                            "whose lineages don't match."
                                            % step_name)

            # assert that none of the inputs are splits - we don't
            # allow empty foreaches (joins immediately following splits)
            if any(not i.is_none('_foreach_var') for i in inputs):
                raise MetaflowInternalError("Step *%s* tries to join a foreach "
                                            "split with no intermediate steps."
                                            % step_name)

            inp = inputs[0]
            if join_type == 'foreach':
                # Make sure that the join got all splits as its inputs.
                # Datastore.resolve() leaves out all undone tasks, so if
                # something strange happened upstream, the inputs list
                # may not contain all inputs which should raise an exception
                stack = inp['_foreach_stack']
                if len(inputs) != stack[-1].num_splits:
                    raise MetaflowDataMissing("Foreach join *%s* expected %d "
                                              "splits but only %d inputs were "
                                              "found" % (step_name,
                                                         stack[-1].num_splits,
                                                         len(inputs)))
                # foreach-join pops the topmost frame from the stack
                self.flow._foreach_stack = stack[:-1]
            else:
                # a non-foreach join doesn't change the stack
                self.flow._foreach_stack = inp['_foreach_stack']

        # case 3) - our parent was a split. Initialize a new foreach frame.
        elif not inputs[0].is_none('_foreach_var'):
            if len(inputs) != 1:
                raise MetaflowInternalError("Step *%s* got multiple inputs "
                                            "although it follows a split step."
                                            % step_name)

            if split_index is None:
                raise MetaflowInternalError("Step *%s* follows a split step "
                                            "but no split_index is "
                                            "specified." % step_name)

            # push a new index after a split to the stack
            frame = ForeachFrame(step_name,
                                 inputs[0]['_foreach_var'],
                                 inputs[0]['_foreach_num_splits'],
                                 split_index)

            stack = inputs[0]['_foreach_stack']
            stack.append(frame)
            self.flow._foreach_stack = stack

    def _clone_flow(self, datastore):
        x = self.flow.__class__(use_cli=False)
        x._set_datastore(datastore)
        return x

    def clone_only(self, step_name, run_id, task_id, clone_origin_task):
        if not clone_origin_task:
            raise MetaflowInternalError("task.clone_only needs a valid "
                                        "clone_origin_task value.")
        # 1. initialize output datastore
        output = self.datastore(self.flow.name,
                                run_id=run_id,
                                step_name=step_name,
                                task_id=task_id,
                                mode='w',
                                metadata=self.metadata,
                                attempt=0,
                                event_logger=self.event_logger,
                                monitor=self.monitor)
        origin_run_id, origin_step_name, origin_task_id =\
            clone_origin_task.split('/')
        # 2. initialize origin datastore
        origin = self.datastore(self.flow.name,
                                run_id=origin_run_id,
                                step_name=origin_step_name,
                                task_id=origin_task_id,
                                metadata=self.metadata,
                                event_logger=self.event_logger,
                                monitor=self.monitor)
        output.clone(origin)
        output.done()

    def run_step(self,
                 step_name,
                 run_id,
                 task_id,
                 origin_run_id,
                 input_paths,
                 split_index,
                 retry_count,
                 max_user_code_retries):

        if run_id and task_id:
            self.metadata.register_run_id(run_id)
            self.metadata.register_task_id(run_id, step_name, task_id)
        else:
            raise MetaflowInternalError("task.run_step needs a valid run_id "
                                        "and task_id")

        if retry_count >= MAX_ATTEMPTS:
            # any results with an attempt ID >= MAX_ATTEMPTS will be ignored
            # by datastore, so running a task with such a retry_could would
            # be pointless and dangerous
            raise MetaflowInternalError("Too many task attempts (%d)! "
                                        "MAX_ATTEMPTS exceeded." % retry_count)

        self.metadata.register_metadata(run_id,
                                        step_name,
                                        task_id,
                                        [MetaDatum(field='attempt',
                                                   value=str(retry_count),
                                                   type='attempt',
                                                   tags=[]),
                                         MetaDatum(field='origin-run-id',
                                                   value=str(origin_run_id),
                                                   type='origin-run-id',
                                                   tags=[])])

        step_func = getattr(self.flow, step_name)
        node = self.flow._graph[step_name]
        join_type = None
        if node.type == 'join':
            join_type = self.flow._graph[node.split_parents[-1]].type

        # 1. initialize output datastore
        output = self.datastore(self.flow.name,
                                run_id=run_id,
                                step_name=step_name,
                                task_id=task_id,
                                mode='w',
                                metadata=self.metadata,
                                attempt=retry_count,
                                event_logger=self.event_logger,
                                monitor=self.monitor)

        if input_paths:
            # 2. initialize input datastores
            inputs = self._init_data(run_id, join_type, input_paths)

            # 3. initialize foreach state
            self._init_foreach(step_name, join_type, inputs, split_index)

        # 4. initialize the current singleton
        current._set_env(flow_name=self.flow.name,
                         run_id=run_id,
                         step_name=step_name,
                         task_id=task_id,
                         retry_count=retry_count,
                         origin_run_id=origin_run_id,
                         namespace=resolve_identity(),
                         username=get_username(),
                         is_running=True)

        # 5. run task
        output.save_metadata('task_begin', {
            'code_package_sha': os.environ.get('METAFLOW_CODE_SHA'),
            'code_package_ds': os.environ.get('METAFLOW_CODE_DS'),
            'code_package_url': os.environ.get('METAFLOW_CODE_URL'),
            'retry_count': retry_count
        })
        logger = self.event_logger
        start = time.time()
        try:
            # init side cars
            logger.start()

            msg = {
                "task_id": task_id,
                "msg": 'task starting',
                "step_name": step_name,
                "run_id": run_id,
                "flow_name": self.flow.name,
                "ts": round(time.time())
            }
            logger.log(msg)

            self.flow._current_step = step_name
            self.flow._success = False
            self.flow._task_ok = None
            self.flow._exception = None
            # Note: All internal flow attributes (ie: non-user artifacts)
            # should either be set prior to running the user code or listed in
            # FlowSpec._EPHEMERAL to allow for proper merging/importing of
            # user artifacts in the user's step code.
            decorators = step_func.decorators
            for deco in decorators:

                deco.task_pre_step(step_name,
                                   output,
                                   self.metadata,
                                   run_id,
                                   task_id,
                                   self.flow,
                                   self.flow._graph,
                                   retry_count,
                                   max_user_code_retries)

                # decorators can actually decorate the step function,
                # or they can replace it altogether. This functionality
                # is used e.g. by catch_decorator which switches to a
                # fallback code if the user code has failed too many
                # times.
                step_func = deco.task_decorate(step_func,
                                               self.flow,
                                               self.flow._graph,
                                               retry_count,
                                               max_user_code_retries)

            if join_type:
                # Join step:

                # Ensure that we have the right number of inputs. The
                # foreach case is checked above.
                if join_type != 'foreach' and\
                   len(inputs) != len(node.in_funcs):
                    raise MetaflowDataMissing("Join *%s* expected %d "
                                              "inputs but only %d inputs "
                                              "were found"
                                              % (step_name,
                                                 len(node.in_funcs),
                                                 len(inputs)))

                # Multiple input contexts are passed in as an argument
                # to the step function.
                input_obj = Inputs(self._clone_flow(inp) for inp in inputs)
                self.flow._set_datastore(output)
                # initialize parameters (if they exist)
                # We take Parameter values from the first input,
                # which is always safe since parameters are read-only
                current._update_env({'parameter_names': self._init_parameters(inputs[0])})
                self._exec_step_function(step_func, input_obj)
            else:
                # Linear step:
                # We are running with a single input context.
                # The context is embedded in the flow.
                if len(inputs) > 1:
                    # This should be captured by static checking but
                    # let's assert this again
                    raise MetaflowInternalError("Step *%s* is not a join "
                                                "step but it gets multiple "
                                                "inputs." % step_name)
                self.flow._set_datastore(inputs[0])
                if input_paths:
                    # initialize parameters (if they exist)
                    # We take Parameter values from the first input,
                    # which is always safe since parameters are read-only
                    current._update_env({'parameter_names': self._init_parameters(inputs[0])})
                self._exec_step_function(step_func)

            for deco in decorators:
                deco.task_post_step(step_name,
                                    self.flow,
                                    self.flow._graph,
                                    retry_count,
                                    max_user_code_retries)

            self.flow._task_ok = True
            self.flow._success = True

        except Exception as ex:

            tsk_msg = {
                "task_id": task_id,
                "exception_msg": str(ex),
                "msg": 'task failed with exception',
                "step_name": step_name,
                "run_id": run_id,
                "flow_name": self.flow.name
            }
            logger.log(tsk_msg)

            exception_handled = False
            for deco in decorators:
                res = deco.task_exception(ex,
                                          step_name,
                                          self.flow,
                                          self.flow._graph,
                                          retry_count,
                                          max_user_code_retries)
                exception_handled = bool(res) or exception_handled

            if exception_handled:
                self.flow._task_ok = True
            else:
                self.flow._task_ok = False
                self.flow._exception = MetaflowExceptionWrapper(ex)
                print('%s failed:' % self.flow, file=sys.stderr)
                raise

        finally:
            end = time.time() - start

            msg = {
                "task_id": task_id,
                "msg": 'task ending',
                "step_name": step_name,
                "run_id": run_id,
                "flow_name": self.flow.name,
                "ts": round(time.time()),
                "runtime": round(end)
            }
            logger.log(msg)

            attempt_ok = str(bool(int(self.flow._task_ok)))
            self.metadata.register_metadata(run_id,
                                            step_name,
                                            task_id,
                                            [MetaDatum(field='attempt_ok',
                                                       value=attempt_ok,
                                                       type='internal_attempt_status',
                                                       tags=["attempt_id:{0}".
                                                       format(str(retry_count))
                                                             ])
                                             ])

            output.save_metadata('task_end', {})
            output.persist(self.flow)

            # terminate side cars
            logger.terminate()

            # this writes a success marker indicating that the
            # "transaction" is done
            output.done()

            # final decorator hook: The task results are now
            # queryable through the client API / datastore
            for deco in decorators:
                deco.task_finished(step_name,
                                   self.flow,
                                   self.flow._graph,
                                   self.flow._task_ok,
                                   retry_count,
                                   max_user_code_retries)
