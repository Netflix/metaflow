from metaflow.decorators import StepDecorator


class ResourcesDecorator(StepDecorator):
    """
    Specifies the resources needed when executing this step.

    Use `@resources` to specify the resource requirements
    independently of the specific compute layer (`@batch`, `@kubernetes`).

    You can choose the compute layer on the command line by executing e.g.
    ```
    python myflow.py run --with batch
    ```
    or
    ```
    python myflow.py run --with kubernetes
    ```
    which executes the flow on the desired system using the
    requirements specified in `@resources`.

    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1.
    gpu : int
        Number of GPUs required for this step. Defaults to 0.
    memory : int
        Memory size (in MB) required for this step. Defaults to 4096.
    shared_memory : int
        The value for the size (in MiB) of the /dev/shm volume for this step.
        This parameter maps to the `--shm-size` option in Docker.
    """

    name = "resources"
    defaults = {"cpu": "1", "gpu": "0", "memory": "4096", "shared_memory": None}
