from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException

from metaflow.plugins.aip.aip_decorator import AIPException


class AcceleratorDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify type of hardware accelerator used in a step.

    To use, follow the example below.
    ```
    @step
    def start(self):
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @accelerator(type="nvidia-tesla-v100")
    @step
    def train(self):
        self.rank = self.input
        # code requiring accelerator for performance
        ...
    ```

    Parameters
    ----------
    type: str
        Defaults to None.
        Available values: nvidia-tesla-v100
        More GPUs will be added based on customer needs.
    """

    name = "accelerator"

    defaults = {
        "type": None,
    }

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if self.attributes["type"] and not isinstance(self.attributes["type"], str):
            raise AIPException("type must be a string.")
