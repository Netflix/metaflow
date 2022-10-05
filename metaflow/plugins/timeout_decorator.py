import signal
import traceback

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator
from metaflow.unbounded_foreach import UBF_CONTROL


class TimeoutException(MetaflowException):
    headline = "@timeout"


class TimeoutDecorator(StepDecorator):
    """
    Specifies a timeout for your step.

    This decorator is useful if this step may hang indefinitely.

    This can be used in conjunction with the `@retry` decorator as well as the `@catch` decorator.
    A timeout is considered to be an exception thrown by the step. It will cause the step to be
    retried if needed and the exception will be caught by the `@catch` decorator, if present.

    Note that all the values specified in parameters are added together so if you specify
    60 seconds and 1 hour, the decorator will have an effective timeout of 1 hour and 1 minute.

    Parameters
    ----------
    seconds : int, default: 0
        Number of seconds to wait prior to timing out.
    minutes : int, default: 0
        Number of minutes to wait prior to timing out.
    hours : int, default: 0
        Number of hours to wait prior to timing out.
    """

    name = "timeout"
    defaults = {"seconds": 0, "minutes": 0, "hours": 0}

    def __init__(self, *args, **kwargs):
        super(TimeoutDecorator, self).__init__(*args, **kwargs)
        # Initialize secs in __init__ so other decorators could safely use this
        # value without worrying about decorator order.
        # Convert values in attributes to type:int since they can be type:str
        # when passed using the CLI option --with.
        self.secs = (
            int(self.attributes["hours"]) * 3600
            + int(self.attributes["minutes"]) * 60
            + int(self.attributes["seconds"])
        )

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        self.logger = logger
        if not self.secs:
            raise MetaflowException("Specify a duration for @timeout.")

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
        if ubf_context != UBF_CONTROL and retry_count <= max_user_code_retries:
            # enable timeout only when executing user code
            self.step_name = step_name
            signal.signal(signal.SIGALRM, self._sigalrm_handler)
            signal.alarm(self.secs)

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        signal.alarm(0)

    def _sigalrm_handler(self, signum, frame):
        def pretty_print_stack():
            for line in traceback.format_stack():
                if "timeout_decorators.py" not in line:
                    for part in line.splitlines():
                        yield ">  %s" % part

        msg = (
            "Step {step_name} timed out after {hours} hours, "
            "{minutes} minutes, {seconds} seconds".format(
                step_name=self.step_name, **self.attributes
            )
        )
        self.logger(msg)
        raise TimeoutException(
            "%s\nStack when the timeout was raised:\n%s"
            % (msg, "\n".join(pretty_print_stack()))
        )


def get_run_time_limit_for_task(step_decos):
    run_time_limit = 5 * 24 * 60 * 60  # 5 days.
    for deco in step_decos:
        if isinstance(deco, TimeoutDecorator):
            run_time_limit = deco.secs
    return run_time_limit
