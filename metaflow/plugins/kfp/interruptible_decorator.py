from metaflow.decorators import StepDecorator


class interruptibleDecorator(StepDecorator):
    """
    For KFP orchestrator plugin only.

    Step decorator to specify that the pod be can be interrupted (ex: Spot, pod consolidation, etc)

    To use, follow the example below.
    ```
    @interruptible()
    @step
    def train(self):
        self.rank = self.input
        # code running on interruptible instance
        ...
    ```

    Parameters
    ----------
    """

    name = "interruptible"
