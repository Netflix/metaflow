from metaflow import FlowSpec, step, batch, current, pytorch_parallel, Parameter
import pytorch_simple_reduce
from metaflow.plugins.frameworks.pytorch import PyTorchHelper


class PytorchLightningParallelTest(FlowSpec):
    """
    Test flow to test @pytorch_parallel with pytorch lightning.
    """

    num_nodes = Parameter(
        "num_nodes",
        help="Number of nodes in cluster",
        default=3,
    )
    num_local_processes = Parameter(
        "num_local_processes", help="Number of local processes per nodde", default=2
    )

    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=self.num_nodes)

    @pytorch_parallel
    @step
    def parallel_step(self):
        """
        Run a simple torch parallel program which learns to sum a vector and also
        includes a simple parallel reduction to validate the parallel environment setup.
        """
        self.reduced_tensor_value = PyTorchHelper.run_trainer(
            self,
            target=pytorch_simple_reduce.train,
            num_local_processes=self.num_local_processes,
        )
        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        """
        Check the validity of the parallel execution.
        """
        j = 0
        for input in inputs:
            assert input.reduced_tensor_value == sum(
                range(1, self.num_nodes * self.num_local_processes + 1)
            )
            j += 1
        assert j == self.num_nodes
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    PytorchLightningParallelTest()
