from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import MAX_ATTEMPTS


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
    """

    name = "retry"
    defaults = {"times": "3", "minutes_between_retries": "2"}

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # The total number of attempts must not exceed MAX_ATTEMPTS.
        # attempts = normal task (1) + retries (N) + @catch fallback (1)
        if int(self.attributes["times"]) + 2 > MAX_ATTEMPTS:
            raise MetaflowException(
                "The maximum number of retries is "
                "@retry(times=%d)." % (MAX_ATTEMPTS - 2)
            )

    def step_task_retry_count(self):
        return int(self.attributes["times"]), 0
