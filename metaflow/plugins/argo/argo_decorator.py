import json
from metaflow.decorators import FlowDecorator
from metaflow.decorators import StepDecorator


class ArgoFlowDecorator(FlowDecorator):
    """
    Flow decorator for Argo Workflows, that sets a default
    for all steps in the flow.
    To use, add this decorator directly on top of your Flow class:
    ```
    @argo_base(image='python:3.8')
    class MyFlow(FlowSpec):
        ...
    ```

    Any step level argo decorator will override any setting by this decorator.

    Parameters
    ----------
    image: string
        Docker image is used for Argo Workflows template.
        If not specified, a default image mapping to
        a base Python/ML container is used.
    labels: json
        Labels to be attached to the workflow template.
    annotations: json
        Annotations to be attached to the workflow template.
    env: list
        Additional environment variables to be set in all steps.
        Follows the k8s spec.
    envFrom: list
        Additional environment variables to be set in all steps.
        Follows the k8s spec.
    imagePullSecrets: list
        Credentials for pulling docker images.
    volumes: list
        Definition of K8s volumes.
    """
    name = 'argo_base'
    defaults = {
        'image': None,
        'labels': {},
        'annotations': {},
        'env': [],
        'envFrom': [],
        'imagePullSecrets': [],
        'volumes': []
    }


class ArgoStepDecorator(StepDecorator):
    """
    Step decorator for Argo Workflows
    ```
    @argo(env=[{'name': 'DEBUG'},
               {'name': 'DEPLOYMENT',
                'value': 'DEV'},
               {'name': 'PASSWORD',
                'valueFrom': {
                    'secretKeyRef': {
                        'name': 'my-secret',
                        'key': 'password'}}}])
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    image: string
        Docker image to use for Argo Workflows template.
        If not specified, a default image from the flow spec is used.
    labels: json
        Labels to be attached to the step's container.
    annotations: json
        Annotations to be attached to the step's container.
    env: list
        Environment variables merged with a container spec.
    envFrom: list
        Environment variables merged with a container spec.
    nodeSelector: json
        Node selector expression, e.g. {"gpu": "nvidia-tesla-k80"}.
    input_artifacts: list
        Argo input artifacts list.
    output_artifacts: list
        Argo outputs artifacts list.
    volumeMounts: list
        Define volume mounts.
   """
    name = 'argo'
    defaults = {
        'image': None,
        'labels': {},
        'annotations': {},
        'env': [],
        'envFrom': [],
        'nodeSelector': {},
        'input_artifacts': [],
        'output_artifacts': [],
        'volumeMounts': [],
    }


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
        # into a file that can be read by Argo Workflows output parameter
        # and this be consumable in the next step
        if graph[step_name].type == 'foreach':
            self._save_foreach_cardinality(flow._foreach_num_splits)

    def _save_foreach_cardinality(self, num_splits):
        with open(self.splits_file_path, 'w') as file:
            json.dump(list(range(num_splits)), file)
