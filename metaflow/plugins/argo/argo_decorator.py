import json
from metaflow.decorators import FlowDecorator
from metaflow.decorators import StepDecorator
from .argo_exception import ArgoException


class ArgoFlowDecorator(FlowDecorator):
    """
    Flow decorator for Argo Workflows, that sets a default
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
    image: string
        Docker image is used for Argo Workflows template. If not specified, a default image mapping to
        a base Python/ML container is used
    labels: part of workflow metadata
    annotations: part of workflow metadata
    imagePullSecrets: credentials for pulling docker images
    """
    name = 'argo_base'
    defaults = {
        'image': None,
        'labels': {},
        'annotations': {},
        'imagePullSecrets': []
    }


class ArgoStepDecorator(StepDecorator):
    """
    Step decorator for Argo Workflows
    ```
    @argo
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    image : string
        Docker image to use for Argo Workflows template. If not specified, a default image mapping to
        a base Python/ML container is used
    nodeSelector: json
        Node selector expression, e.g. {"gpu": "nvidia-tesla-k80"}
    artifacts: list
        Argo outputs artifacts list
   """
    name = 'argo'
    defaults = {
        'image': None,
        'nodeSelector': None,
        'artifacts': None
    }

    def __init__(self, attributes=None, statically_defined=False):
        super(ArgoStepDecorator, self).__init__(attributes, statically_defined)

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise ArgoException('The *@argo* decorator requires --datastore=s3.')


class ArgoInternalStepDecorator(StepDecorator):
    name = 'argo_internal'
    splits_file_path = '/tmp/num_splits'

    def task_finished(self,
                      step_name,
                      flow,
                      graph,
                      is_task_ok,
                      retry_count,
                      max_user_code_retries):

        if not is_task_ok:
            # The task finished with an exception - execution won't
            # continue so no need to do anything here.
            return

        # For foreaches, we need to export the cardinality of the fan-out
        # into a file that can be read by Argo Workflows output parameter and this be consumable in the next step
        # TODO: nested foreach
        if graph[step_name].type == 'foreach':
            self._save_foreach_cardinality(flow._foreach_num_splits)

    def _save_foreach_cardinality(self, num_splits):
        with open(self.splits_file_path, 'w') as file:
            json.dump(list(range(num_splits)), file)
