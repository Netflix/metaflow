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
    @resources(cpu=32)
    @step
    def my_step(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1
    gpu : int
        Number of GPUs required for this step. Defaults to 0
    memory : int
        Memory size (in MB) required for this step. Defaults to 4096
    shared_memory : int
        The value for the size (in MiB) of the /dev/shm volume for this step.
        This parameter maps to the --shm-size option to docker run .
    """

    name = "resources"
    defaults = {"cpu": "1", "gpu": "0", "memory": "4096", "shared_memory": None}
