from metaflow.decorators import FlowDecorator
from metaflow.environment import InvalidEnvironmentException


class CondaFlowDecorator(FlowDecorator):
    """
    Conda decorator that sets a default Conda step decorator for all
    steps in the flow.

    To use, add this decorator directly on top of your Flow class:
    ```
    @conda_base
    class MyFlow(FlowSpec):
        ...
    ```

    Any step level Conda decorator will override any setting by this decorator.

    Parameters
    ----------
    libraries : Dict
        Libraries to use for this flow. The key is the name of the package and the value
        is the version to use. Defaults to {}
    python : string
        Version of Python to use (for example: '3.7.4'). Defaults to None
        (specified at the step level)
    disabled : bool
        If set to True, disables Conda (note that this is overridden if a step level decorator
        sets to True). Defaults to None (specified at the step level)
    Raises
    ------
    InvalidEnvironmentException
        Raised if --environment=conda is not specified
    """
    name = 'conda_base'
    defaults = {'libraries': {},
                'python': None,
                'disabled': None}

    def flow_init(self, flow, graph,  environment, datastore, logger, echo, options):
        if environment.TYPE != 'conda':
            raise InvalidEnvironmentException('The *@conda* decorator requires '
                                              '--environment=conda')