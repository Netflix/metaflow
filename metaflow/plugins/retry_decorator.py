from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException
from metaflow.metaflow_config import MAX_ATTEMPTS


class RetryDecorator(StepDecorator):
    """
    Step decorator to specify that a step should be retried on failure.

    This decorator indicates that if this step fails, it should be retried a certain number of times.

    This decorator is useful if transient errors (like networking issues) are likely in your step.

    This can be used in conjunction with the @retry decorator. In that case, catch will only
    activate if all retries fail and will catch the last exception thrown by the last retry.

    To use, annotate your step as follows:
    ```
    @retry(times=3)
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    times : int
        Number of times to retry this step. Defaults to 3
    minutes_between_retries : int
        Number of minutes between retries
    """
    name = 'retry'
    defaults = {'times': '3',
                'minutes_between_retries': '2'}

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        # The total number of attempts must not exceed MAX_ATTEMPTS.
        # attempts = normal task (1) + retries (N) + @catch fallback (1)
        if int(self.attributes['times']) + 2 > MAX_ATTEMPTS:
            raise MetaflowException('The maximum number of retries is '
                                    '@retry(times=%d).' % (MAX_ATTEMPTS - 2))

    def step_task_retry_count(self):
        return int(self.attributes['times']), 0
