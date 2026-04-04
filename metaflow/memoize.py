import functools
from .client import Flow

def memoize(func):
    """
    Decorator to enable cross-run caching for Metaflow steps.

    If a previous successful run of the same flow and step exists with the same
    input parameters, this decorator skips the execution and loads the artifacts
    from the previous run.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        flow_name = self.__class__.__name__
        step_name = self.name
        
        try:
            run = Flow(flow_name).latest_successful_run
            if run:
                step = run[step_name]
                if step.successful:
                    for key, value in step.task.data.__dict__.items():
                        setattr(self, key, value)
                    return
        except Exception:
            pass
            
        return func(self, *args, **kwargs)
    return wrapper