import traceback

from metaflow.exception import MetaflowException,\
    MetaflowExceptionWrapper
from metaflow.decorators import StepDecorator

NUM_FALLBACK_RETRIES = 3


class FailureHandledByCatch(MetaflowException):
    headline = 'Task execution failed but @catch handled it'

    def __init__(self, retry_count):
        msg = 'Task execution kept failing over %d attempts. '\
              'Your code did not raise an exception. Something '\
              'in the execution environment caused the failure.' % retry_count
        super(FailureHandledByCatch, self).__init__(msg)


class CatchDecorator(StepDecorator):
    """
    Step decorator to specify error handling for your step.

    This decorator indicates that exceptions in the step should be caught and not fail the entire
    flow.

    This can be used in conjunction with the @retry decorator. In that case, catch will only
    activate if all retries fail and will catch the last exception thrown by the last retry.

    To use, annotate your step as follows:
    ```
    @catch(var='foo')
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    var : string
        Name of the artifact in which to store the caught exception. If not specified,
        the exception is not stored
    print_exception : bool
        Determines whether or not the exception is printed to stdout when caught. Defaults
        to True
    """
    name = 'catch'
    defaults = {'var': None,
                'print_exception': True}

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        # handling _foreach_var and _foreach_num_splits requires some
        # deeper thinking, so let's not support that use case for now
        self.logger = logger
        if graph[step].type == 'foreach':
            raise MetaflowException('@catch is defined for the step *%s* '
                                    'but @catch is not supported in foreach '
                                    'split steps.' % step)

    def _print_exception(self, step, flow):
        self.logger(head='@catch caught an exception from %s' % flow,
                    timestamp=False)
        for line in traceback.format_exc().splitlines():
            self.logger('>  %s' % line, timestamp=False)

    def _set_var(self, flow, val):
        var = self.attributes.get('var')
        if var:
            setattr(flow, var, val)

    def task_exception(self,
                       exception,
                       step,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):

        if self.attributes['print_exception']:
            self._print_exception(step, flow)

        # pretend that self.next() was called as usual
        flow._transition = (graph[step].out_funcs, None, None)
        # store the exception
        picklable = MetaflowExceptionWrapper(exception)
        flow._catch_exception = picklable
        self._set_var(flow, picklable)
        return True

    def task_post_step(self,
                       step_name,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):
        # there was no exception, set the exception var (if any) to None
        self._set_var(flow, None)

    def step_task_retry_count(self):
        return 0, NUM_FALLBACK_RETRIES

    def task_decorate(self,
                      step_func,
                      func,
                      graph,
                      retry_count,
                      max_user_code_retries):

        # if the user code has failed max_user_code_retries times, @catch
        # runs a piece of fallback code instead. This way we can continue
        # running the flow downsteam, as we have a proper entry for this task.

        def fallback_step(inputs=None):
            raise FailureHandledByCatch(retry_count)

        if retry_count > max_user_code_retries:
            return fallback_step
        else:
            return step_func
