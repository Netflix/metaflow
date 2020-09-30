from metaflow.decorators import StepDecorator


class ResourcesDecorator(StepDecorator):
    """
    Step decorator to specify the resources needed when executing this step.
    This decorator passes this information along when requesting resources
    to execute this step.

    This decorator can be used for two purpose:
    When using with AWS Batch decorator, only 'cpu', 'gpu', and 'memory' parameters are supported.
    When using for Kubeflow Pipeline, 'cpu', and 'memory' sets resource requests;
        'cpu_limit', 'gpu', 'memory_limit' sets resource limit.
        For more details please refer to
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    This decorator is ignored if the execution of the step does not happen on Batch.
    To use, annotate your step as follows:
    ```
    @resources(cpu=32, memory="15G")
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    cpu : Union[int, float, str]
        AWS Batch: Number of CPUs required for this step. Defaults to 1. Must be integer.
        KFP: Number of CPUs required for this step. Defaults to 1. Can input int, float, or str.
            Support millicpu requests using float or string ending in 'm'.
            A request with a decimal point, like 0.1, is converted to 100m by the API,
            and precision finer than 1m is not allowed.
    cpu_limit : Union[int, float, str]
        Not for AWS Batch.
        KFP: Number of CPUs limited for this step. Defaults None - relying on Kubernetes defaults.
    gpu : int
        AWS Batch: Number of GPUs required for this step. Defaults to 0.
        KFP: GPU limit for this step. Defaults to 0.
            GPU are only supposed to be specified in limit section.
            See https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/
    gpu_vendor : str
        Not for AWS Batch.
        KFP: "nvidia" or "amd". Defaults to "nvidia".
    memory : Union[int, str]
        AWS Batch: Memory size (in MB) required for this step. Defaults to 4000.
        KFP: Memory size required for this step. Default to 4000 MB. For int input, unit defaults to MB.
            More memory units are supported, including "E", "P", "T", "G", "M", "K". (i.e. "4000M")
    memory_limit : Union[int, str]
        Not for AWS Batch.
        KFP: Memory limit for this step. Default unit is MB.
            More memory units are supported, including "E", "P", "T", "G", "M", "K". (i.e. "4000M")
             Defaults None - relying on Kubernetes defaults.
    """
    name = 'resources'

    # Actual defaults are set in .aws.batch.batch_decorator.BatchDecorator and
    # .kfp.kfp.KubeflowPipelines._get_resource_requirements respectively.
    # The defaults here simply lists accepted attributes.
    defaults = {
        # AWS Batch and KFP supported attributes
        "cpu": None,
        "gpu": None,
        "memory": None,

        # Only KFP supported attributes
        "cpu_limit": None,
        "gpu_vendor": None,
        "memory_limit": None,
    }
