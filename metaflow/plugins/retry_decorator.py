import sys
import traceback
from functools import wraps
from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException, METAFLOW_EXIT_DISALLOW_RETRY
from metaflow.metaflow_config import MAX_ATTEMPTS


SYSTEM_ERROR_MESSAGE_PATTERNS = [
    "spot instance interruption",
    "connection reset by peer",
    "connection timed out",
    "resource temporarily unavailable",
    "internal error",
    "service unavailable",
    "oom-killer",
    "out of memory",
    "sigterm",
    "sigkill",
    "ehostunreach",
    "no space left on device",
    "too many open files",
]


def is_system_error(exception):
    error_msg = str(exception).lower()

    for pattern in SYSTEM_ERROR_MESSAGE_PATTERNS:
        if pattern.lower() in error_msg:
            return True

    if hasattr(exception, "errno"):
        system_errno = {
            4,  # EINTR
            5,  # EIO
            11,  # EAGAIN
            12,  # ENOMEM
            13,  # EACCES
            24,  # EMFILE
            104,  # ECONNRESET
            110,  # ETIMEDOUT
        }

        if exception.errno in system_errno:
            return True

    return False


class RetryDecorator(StepDecorator):
    """
    Specifies the number of times the task corresponding
    to a step needs to be retried.

    This decorator is useful for handling transient errors, such as networking issues.
    If your task contains operations that can't be retried safely, e.g. database updates,
    it is advisable to annotate it with `@retry(times=0)`.

    This can be used in conjunction with the `@catch` decorator. The `@catch`
    decorator will execute a no-op task after all retries have been exhausted,
    ensuring that the flow execution can continue.

    Parameters
    ----------
    times : int, default 3
        Number of times to retry this task.
    minutes_between_retries : int, default 2
        Number of minutes between retries.
    only_system : bool, default False
        If True, only retry on system-level failures
    """

    name = "retry"
    defaults = {"times": "3", "minutes_between_retries": "2", "only_system": False}

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # The total number of attempts must not exceed MAX_ATTEMPTS.
        # attempts = normal task (1) + retries (N) + @catch fallback (1)
        if int(self.attributes["times"]) + 2 > MAX_ATTEMPTS:
            raise MetaflowException(
                "The maximum number of retries is "
                "@retry(times=%d)." % (MAX_ATTEMPTS - 2)
            )

    def step_task_retry_count(self):
        times = int(self.attributes["times"])
        if self.attributes["only_system"]:
            return 0, times
        return times, 0

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        @wraps(step_func)
        def fallback_step(*args, **kwargs):
            try:
                step_func(*args, **kwargs)
            except Exception as ex:
                if not is_system_error(ex):
                    traceback.print_exc()
                    sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
                raise

        if self.attributes["only_system"]:
            return fallback_step

        return step_func
