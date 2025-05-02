from enum import Enum

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import MAX_ATTEMPTS


class RetryEvents(Enum):
    STEP = "step"
    PREEMPT = "instance-preemption"


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
    only_on : List[str], default None
        List of failure events to retry on. Not specifying values will retry on all known events.
        Accepted values are
        'step', 'instance-preemption'
    """

    name = "retry"
    defaults = {"times": "3", "minutes_between_retries": "2", "only_on": None}

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        # The total number of attempts must not exceed MAX_ATTEMPTS.
        # attempts = normal task (1) + retries (N) + @catch fallback (1)
        if int(self.attributes["times"]) + 2 > MAX_ATTEMPTS:
            raise MetaflowException(
                "The maximum number of retries is "
                "@retry(times=%d)." % (MAX_ATTEMPTS - 2)
            )

        if self.attributes["only_on"] is not None:
            if not isinstance(self.attributes["only_on"], list):
                raise MetaflowException("'only_on=' must be a list of values")

            def _known_event(event: str):
                try:
                    RetryEvents(event)
                    return True
                except ValueError:
                    return False

            unsupported_events = [
                event for event in self.attributes["only_on"] if not _known_event(event)
            ]
            if unsupported_events:
                raise MetaflowException(
                    "The event(s) %s are not supported for only_on="
                    % ", ".join("*%s*" % event for event in unsupported_events)
                )

    def step_task_retry_count(self):
        return int(self.attributes["times"]), 0
