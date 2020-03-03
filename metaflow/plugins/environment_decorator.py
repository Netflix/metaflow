import os

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator,FlowDecorator

class GlobalEnvironmentDecorator(FlowDecorator):
    """
    Flow Decorator to add a general environment variables to ever decorator. 

    To use, annotate your flows as follows:
    ```
    @global_environment(vars={'MY_ENV': 'value'})
    class MyFlow(FlowSpec):
        ...
    ```

    Any step level Environment decorator will add/update mote env variables set via this decorator. 

    Parameters
    ----------
    vars : Dict
        Dictionary of environment variables to add/update prior to executing your step.
    """
    name = 'global_environment'
    defaults = {
        'vars':{}
    }
    def flow_init(self, flow, graph, environment, datastore, logger):
        os.environ.update(self.attributes['vars'].items())

class EnvironmentDecorator(StepDecorator):
    """
    Step decorator to add or update environment variables prior to the execution of your step.

    The environment variables set with this decorator will be present during the execution of the
    step both locally or on Batch.

    To use, annotate your step as follows:
    ```
    @environment(vars={'MY_ENV': 'value'})
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    vars : Dict
        Dictionary of environment variables to add/update prior to executing your step.
    """
    name = 'environment'
    defaults = {'vars': {}}

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        os.environ.update(self.attributes['vars'].items())
