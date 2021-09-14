from metaflow.decorators import StepDecorator


class ResourcesDecorator(StepDecorator):
    """
    Step decorator to specify the resources needed when executing this step.
    This decorator passes this information along when requesting resources
    to execute this step.

    This decorator can be used for two purpose:
    When using with AWS Batch decorator, only 'cpu', 'gpu', and 'memory'
        parameters are supported.
    When using for Kubeflow Pipeline, 'cpu', and 'memory' sets resource requests;
        'cpu_limit', 'gpu', 'memory_limit' sets resource limit.
        For more details please refer to
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    This decorator is ignored if the execution of the step does not happen on
    Batch or Kubeflow Pipeline

    To use, annotate your step as follows:
    ```
    @resources(cpu=32, memory="15G")
    @step
    def myStep(self):
        ...
    ```

    For KFP plug-in, more units are available when specifying requirements.
    Units for memory or disk spaces, differing by 1000 each in descending order:
        "E", "P", "T", "G", "M", "K"
    Alternatively power of 2 estimates of these units can be used:
        "Ei", "Pi", "Ti", "Gi", "Mi", "Ki"
    For example:
        K = 10**3  # Kilo
        Ki = 1 << 10  # Kilo: power-of-two approximate
    The unit defaults to MB following Metaflow standard if not specified.

    Parameters
    ----------
    cpu : Union[int, float, str]
        AWS Batch: Number of CPUs required for this step. Defaults to 1.
            Must be integer.
        KFP: Number of CPUs required for this step. Defaults to 1.
            Accept int, float, or str.
            Support millicpu requests using float or string ending in 'm'.
            Requests with decimal points, like 0.1, are converted to 100m by kfp
            Precision finer than 1m is not allowed.
    cpu_limit : Union[int, float, str]
        Not for AWS Batch.
        KFP: Number of CPUs limited for this step.
            Defaults None - relying on Kubernetes defaults.
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
        KFP: Memory required for this step. Default to 4000 MB.
            See notes above for more units.
    memory_limit : Union[int, str]
        Not for AWS Batch.
        KFP: Memory limit for this step. Default unit is MB - see notes above.
            Defaults None - relying on Kubernetes defaults.
    shared_memory : int
        Not for KFP
        AWS Batch: The value for the size (in MiB) of the /dev/shm volume for this step.
            This parameter maps to the --shm-size option to docker run .
    local_storage: Union[int, str]
        Not for AWS Batch.
        KFP: Local ephemeral storage required.
            Defaults None - relying on Kubernetes defaults.
            **Note:** If you need to increase local ephemeral storage,
            then we recommend requesting storage which creates a volume instead.
            This is local disk storage per step and lost after the step.
            Default unit is MB. See notes above for more units.
    local_storage_limit: Union[int, str]
        Not for AWS Batch.
        KFP: Local ephemeral storage limit.
    volume: Union[int, str]
        Not for AWS Batch.
        KFP: Attaches a volume which by default is not accessible to subsequent steps.
            Defaults None - relying on Kubernetes defaults.
            **Note:** The volume persists state across step (container) retries.
            Default unit is MB - see notes above for more units.
    volume_mode: str
        Not for AWS batch.
        [ReadWriteOnce, ReadWriteMany]
        ReadWriteOnce: can be used by this step only
        ReadWriteMany:
            A volume to be shared across foreach split nodes, but not downstream steps.
            An example use case is PyTorch distributed training where gradients are communicated
            via the shared volume.
    volume_dir: str
        Default "/opt/metaflow_volume"
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

        # Only AWS Batch supported attributes
        'shared_memory': None,

        # Only KFP supported attributes
        "gpu_vendor": None,
        "local_storage": None,
        "volume": None,
        "volume_mode": "ReadWriteOnce",
        "volume_dir": "/opt/metaflow_volume"
    }
