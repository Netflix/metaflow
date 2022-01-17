from metaflow import FlowSpec, step, batch, current, pytorch_parallel, Parameter


class PytorchParallelTest(FlowSpec):
    """
    Test flow to test @pytorch_parallel.
    """

    num_parallel = Parameter(
        "num_parallel", help="Number of nodes in cluster", default=3
    )

    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=self.num_parallel)

    @pytorch_parallel
    @step
    def parallel_step(self):
        """
        Run a simple torch parallel program where each node creates a 3 x 3 tensor
        with each entry equaling their rank + 1. Then, all reduce is called to sum the
        tensors up.
        """
        import torch
        import torch.distributed as dist

        # Run very simple parallel pytorch program
        dist.init_process_group(
            "gloo",
            rank=current.parallel.node_index,
            world_size=current.parallel.num_nodes,
        )

        # Each node creates a 3x3 matrix with values corresponding to their rank + 1
        my_tensor = torch.ones(3, 3) * (dist.get_rank() + 1)
        assert int(my_tensor[0, 0]) == current.parallel.node_index + 1

        # Then sum the tensors up
        print("Reducing tensor", my_tensor)
        dist.all_reduce(my_tensor, op=dist.ReduceOp.SUM)
        print("Result:", my_tensor)

        # Assert the values are as expected
        for i in range(3):
            for j in range(3):
                assert int(my_tensor[i, j]) == sum(
                    range(1, current.parallel.num_nodes + 1)
                )
        dist.destroy_process_group()

        self.node_index = current.parallel.node_index
        self.num_nodes = current.parallel.num_nodes
        self.reduced_tensor_value = int(my_tensor[0, 0])

        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        """
        Check the validity of the parallel execution.
        """
        j = 0
        for input in inputs:
            assert input.node_index == j
            assert input.num_nodes == self.num_parallel
            assert input.reduced_tensor_value == sum(range(1, input.num_nodes + 1))
            j += 1
        assert j == self.num_parallel
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    PytorchParallelTest()
