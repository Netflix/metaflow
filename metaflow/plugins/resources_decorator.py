from metaflow.decorators import StepDecorator


class ResourcesDecorator(StepDecorator):
    """
    Step decorator to specify the resources needed when executing this step.

    This decorator passes this information along to container orchestrator
    (AWS Batch, Kubernetes, etc.) when requesting resources to execute this
    step.

    This decorator is ignored if the execution of the step happens locally.

    To use, annotate your step as follows:
    ```
    @resources(cpu=32, memory="15G")
    @step
    def myStep(self):
        ...
    ```

    For AIP plug-in, more units are available when specifying requirements.
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
        AWS Batch: Number of CPUs required for this step. Defaults to 1 in batch decorator.
            Must be integer.
        AIP: Number of CPUs required for this step. Defaults to None - use cluster setting.
            Accept int, float, or str.
            Support millicpu requests using float or string ending in 'm'.
            Requests with decimal points, like 0.1, are converted to 100m by aip
            Precision finer than 1m is not allowed.
    gpu : int
        AWS Batch: Number of GPUs required for this step. Defaults to 0.
        AIP: GPU limit for this step. Defaults to 0.
            GPU are only supposed to be specified in limit section.
            See https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/
    gpu_vendor : str
        Not for AWS Batch.
        AIP: "nvidia" or "amd". Defaults to "nvidia".
    memory : Union[int, str]
        AWS Batch: Memory size (in MB) required for this step. Defaults to 4096 in batch decortor.
        AIP: Memory required for this step. Default to None - use cluster setting.
            See notes above for more units.
    shared_memory : int
        Not for AIP
        AWS Batch: The value for the size (in MiB) of the /dev/shm volume for this step.
            This parameter maps to the --shm-size option to docker run .
    volume: Union[int, str]
        Not for AWS Batch.
        AIP: Attaches a volume which by default is not accessible to subsequent steps.
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
    volume_type: str
        Default None (the cluster or system default)
        This is the type of volume to use.  On AWS this would be the EBS volume type.
    """

    name = "resources"

    # Actual defaults are set in .aws.batch.batch_decorator.BatchDecorator and
    # .aip.aip.KubeflowPipelines._get_resource_requirements respectively.
    # The defaults here simply lists accepted attributes.
    defaults = {
        # AWS Batch and AIP supported attributes
        "cpu": None,
        "gpu": None,
        "memory": None,
        "shared_memory": None,
        # Only AIP supported attributes
        "gpu_vendor": None,
        "volume": None,
        "volume_mode": "ReadWriteOnce",
        "volume_dir": "/opt/metaflow_volume",
        "volume_type": None,
        # Deprecated - kept only to show a meaningful error message
        "local_storage": None,
    }
