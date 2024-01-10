import functools
from typing import Dict

from metaflow.decorators import FlowDecorator
from metaflow.plugins.aip.aip_constants import (
    EXIT_HANDLER_RETRY_COUNT,
    RETRY_BACKOFF_FACTOR,
    BACKOFF_DURATION_INT,
)
from metaflow.plugins.aip.aip_decorator import AIPException


def exit_handler_resources(cpu=None, memory=None):
    """
    Args:
        cpu : Union[int, float, str]
            AIP: Number of CPUs required for this step. Defaults to None - use cluster setting.
                Accept int, float, or str.
                Support millicpu requests using float or string ending in 'm'.
                Requests with decimal points, like 0.1, are converted to 100m by aip
                Precision finer than 1m is not allowed.
        memory : Union[int, str]
            AIP: Memory required for this step. Default to None - use cluster setting.
                See notes above for more units.
    """

    def inner_decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            response = f(*args, **kwargs)
            return response

        wrapped.cpu = cpu
        wrapped.memory = memory
        return wrapped

    return inner_decorator


def exit_handler_retry(
    times: int = EXIT_HANDLER_RETRY_COUNT,
    minutes_between_retries: int = int(BACKOFF_DURATION_INT),
    retry_backoff_factor: int = RETRY_BACKOFF_FACTOR,
):
    """
    Args:
        times : int
            Number of times to retry this step. Defaults to 3
        minutes_between_retries : int
            Number of minutes between retries
        retry_backoff_factor : int
            Exponential backoff factor. If set to 3, the time between retries will triple each time.
            Defaults to 3.
    """
    # validate that minutes_between_retries is an integer
    if not isinstance(minutes_between_retries, int):
        raise AIPException("minutes_between_retries must be an integer")

    def inner_decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            response = f(*args, **kwargs)
            return response

        wrapped.retries = times
        wrapped.minutes_between_retries = minutes_between_retries
        wrapped.retry_backoff_factor = retry_backoff_factor
        return wrapped

    return inner_decorator


class ExitHandlerDecorator(FlowDecorator):
    """
    Exit handler function that is run after the Flow run has completed,
    irrespective of the run success or failure.  Given that an exit_handler has
    a retry, please note that it could be invoked multiple times, and hence
    should ideally be idempotent, or not cause issues if the whole function is
    run multiple times.

    Note:
    You can only specify this decorator once in a Flow, else a
    DuplicateFlowDecoratorException is raised.

    Parameters
    ----------
    func (ExitHandlerDecorator): The user defined exit handler to invoke.
    on_failure (bool): Whether to invoke the exit handler on Flow failure. The default is True.
    on_success (bool): Whether to invoke the exit handler on Flow success. The default is False.
    Returns
    ------
        None: This function does not return anything.

    >>> from metaflow import FlowSpec, step, exit_handler

    >>> @exit_handler_resources(memory="2G")
    >>> def my_exit_handler(
    >>>     status: str,
    >>>     flow_parameters: dict,
    >>>     argo_workflow_run_name: str,
    >>>     metaflow_run_id: str,
    >>>     argo_ui_url: str,
    >>> ) -> None:
    >>>     '''
    >>>     Args:
    >>>         status (str): The success/failure status of the Argo run.
    >>>         flow_parameters (dict): The parameters passed to the Flow.
    >>>         argo_workflow_run_name (str): The name of the Argo workflow run.
    >>>         metaflow_run_id (str): The Metaflow run ID, it may not exist.
    >>>         argo_ui_url (str): The URL of the Argo UI.
    >>>     Returns:
    >>>         None: This function does not return anything.
    >>>     '''
    >>>     pass
    >>>
    >>> @exit_handler(func=my_exit_handler)
    >>> class MyFlow
    >>>   pass

    """

    name = "exit_handler"
    defaults = {
        "func": None,
        "on_failure": True,
        "on_success": False,
    }

    def flow_init(
        self, flow, graph, environment, flow_datastore, metadata, logger, echo, options
    ):
        func = self.attributes.get("func")
        if func is None:
            raise AIPException("must specify the function.")

        if not callable(func):
            raise AIPException("func must be callable")

        # validate that either on_failure or on_success is True
        if not self.attributes["on_failure"] and not self.attributes["on_success"]:
            raise AIPException("Either on_failure or on_success must be True")
