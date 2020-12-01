from metaflow.decorators import FlowDecorator
from metaflow.decorators import StepDecorator
from metaflow.plugins.mlf.argo_workflow import ArgoException


class ArgoFlowDecorator(FlowDecorator):
    """
    Flow decorator for argo workflows, that sets a default
    for all steps in the flow.
    To use, add this decorator directly on top of your Flow class:
    ```
    @argo_base(image=python:3.8)
    class MyFlow(FlowSpec):
        ...
    ```

    Any step level argo decorator will override any setting by this decorator.

    Parameters
    ----------
    image : string
        Docker image to use for argo template. If not specified, a default image mapping to
        a base Python/ML container is used
    """
    name = 'argo_base'
    defaults = {
        'image': None
    }


class ArgoStepDecorator(StepDecorator):
    """
    Step decorator for argo workflows
    ```
    @argo
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    image : string
        Docker image to use for argo template. If not specified, a default image mapping to
        a base Python/ML container is used
    nodeSelector: json
        node selector expression, e.g. {"gpu": "nvidia-tesla-k80"}

    """
    name = 'argo'
    defaults = {
        'image': None,
        'nodeSelector': None
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(ArgoStepDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise ArgoException('The *@argo* decorator requires --datastore=s3.')
