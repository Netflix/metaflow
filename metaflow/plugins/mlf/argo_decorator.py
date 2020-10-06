from metaflow.decorators import FlowDecorator
from metaflow.decorators import StepDecorator
from metaflow.plugins.mlf.argo_workflow import ArgoException
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator


class ArgoFlowDecorator(FlowDecorator, StepDecorator):
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
    """
    name = 'argo'
    defaults = {
        'image': None
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(ArgoStepDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise ArgoException('The *@argo* decorator requires --datastore=s3.')

        self.logger = logger
        self.environment = environment
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # we use the larger of @resources and @argo attributes  TODO: need ResourcesDecorator additionally?
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = str(max(int(my_val or 0), int(v or 0)))
