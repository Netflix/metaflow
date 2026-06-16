import traceback

from metaflow.exception import MetaflowException, MetaflowExceptionWrapper
from metaflow.decorators import StepDecorator
from metaflow import current

NUM_FALLBACK_RETRIES = 3


class FailureHandledByCatch(MetaflowException):
    headline = "Task execution failed but @catch handled it"

    def __init__(self, retry_count):
        msg = (
            "Task execution kept failing over %d attempts. "
            "Your code did not raise an exception. Something "
            "in the execution environment caused the failure." % retry_count
        )
        super(FailureHandledByCatch, self).__init__(msg)


class CatchDecorator(StepDecorator):
    """
    Specifies that the step will success under all circumstances.

    The decorator will create an optional artifact, specified by `var`, which
    contains the exception raised. You can use it to detect the presence
    of errors, indicating that all happy-path artifacts produced by the step
    are missing.

    Parameters
    ----------
    var : str, optional, default None
        Name of the artifact in which to store the caught exception.
        If not specified, the exception is not stored.
    print_exception : bool, default True
        Determines whether or not the exception is printed to
        stdout when caught.
    """

    name = "catch"
    defaults = {"var": None, "print_exception": True}

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # handling _foreach_var and _foreach_num_splits requires some
        # deeper thinking, so let's not support that use case for now
        self.logger = logger
        if graph[step].type == "foreach":
            raise MetaflowException(
                "@catch is defined for the step *%s* "
                "but @catch is not supported in foreach "
                "split steps." % step
            )

        # Do not support catch on switch steps for now.
        # When applying @catch to a switch step, we can not guarantee that the flow attribute used for the switching condition gets properly recorded.
        if graph[step].type == "split-switch":
            raise MetaflowException(
                "@catch is defined for the step *%s* "
                "but @catch is not supported in conditional "
                "switch steps." % step
            )

    def _print_exception(self, step, flow):
        self.logger(head="@catch caught an exception from %s" % flow, timestamp=False)
        for line in traceback.format_exc().splitlines():
            self.logger(">  %s" % line, timestamp=False)

    def _set_var(self, flow, val):
        var = self.attributes.get("var")
        if var:
            setattr(flow, var, val)

    def task_exception(
        self, exception, step, flow, graph, retry_count, max_user_code_retries
    ):
        # Only "catch" exceptions after all retries are exhausted
        if retry_count < max_user_code_retries:
            return False

        if self.attributes["print_exception"]:
            self._print_exception(step, flow)

        # pretend that self.next() was called as usual
        flow._transition = (graph[step].out_funcs, None)

        # If this task is a UBF control task, it will return itself as the singleton
        # list of tasks.
        if hasattr(flow, "_parallel_ubf_iter"):
            flow._control_mapper_tasks = [
                "/".join((current.run_id, current.step_name, current.task_id))
            ]
        # store the exception
        picklable = MetaflowExceptionWrapper(exception)
        flow._catch_exception = picklable
        self._set_var(flow, picklable)
        return True

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        # there was no exception, set the exception var (if any) to None
        self._set_var(flow, None)

    def step_task_retry_count(self):
        return 0, NUM_FALLBACK_RETRIES

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        # if the user code has failed max_user_code_retries times, @catch
        # runs a piece of fallback code instead. This way we can continue
        # running the flow downstream, as we have a proper entry for this task.

        def fallback_step(inputs=None):
            raise FailureHandledByCatch(retry_count)

        if retry_count > max_user_code_retries:
            return fallback_step
        else:
            return step_func
