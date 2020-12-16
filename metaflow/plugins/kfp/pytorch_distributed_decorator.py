from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


class PyTorchDistributedDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify that the parallel node is a pytorch cluster.

    To use, annotate your step as follows:
    ```
    @step
    def start(self):
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @pytorch_distributed
    @step
    def train(self):
        self.rank = self.input
        # pytorch code
        ...
    ```

    Parameters
    ----------
    mode: str
        Default's to DDP -> distributed data parallel.
        Coming soon: "RPC"
    shared_volume_size : str
        Shared volume size limit. Default unit is MB.
            Other units are supported, including "E", "P", "T", "G", "M", "K". (i.e. "4000M")
            Defaults 100M
    shared_volume_dir : str
        Where to mount the shared volume.
        Defaults to: /opt/pytorch_shared
    """

    name = "pytorch_distributed"

    defaults = {
        "mode": "DDP",
        # KFP supported attributes
        "shared_volume_size": "100M",
        "shared_volume_dir": "/opt/pytorch_shared/",
    }

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        node = graph[step]
        if step == "start":
            raise MetaflowException("@pytorch_distributed cannot be on the start step.")

        if len(node.in_funcs) > 1 or graph[node.in_funcs[0]].type != "foreach":
            raise MetaflowException("@pytorch_distributed must be within a foreach.")
