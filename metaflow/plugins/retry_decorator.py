import os
import signal
import sys
import threading
from time import sleep

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import MAX_ATTEMPTS
from metaflow import current

SUPPORTED_RETRY_EVENTS = ["all", "spot-termination"]


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
        List of failure events to retry on. Accepted values are
        'all', 'spot-termination'
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

            unsupported_events = [
                event
                for event in self.attributes["only_on"]
                if event not in SUPPORTED_RETRY_EVENTS
            ]
            if unsupported_events:
                raise MetaflowException(
                    "The event(s) %s are not supported for only_on="
                    % ", ".join("*%s*" % event for event in unsupported_events)
                )

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
        pid = os.getpid()

        def _termination_timer():
            sleep(30)
            os.kill(pid, signal.SIGALRM)

        def _spot_term_signal_handler(*args, **kwargs):
            if os.path.isfile(current.spot_termination_notice):
                print(
                    "Spot instance termination detected. Starting a timer to end the Metaflow task."
                )
                timer_thread = threading.Thread(target=_termination_timer, daemon=True)
                timer_thread.start()

        def _curtain_call(*args, **kwargs):
            # custom exit code in case of Spot termination
            sys.exit(154)

        signal.signal(signal.SIGUSR1, _spot_term_signal_handler)
        signal.signal(signal.SIGALRM, _curtain_call)

    def step_task_retry_count(self):
        return int(self.attributes["times"]), 0
