from metaflow import FlowSpec, step, batch, current, parallel, Parameter


class ParallelTest(FlowSpec):
    """
    Test flow to test @parallel.
    """

    num_parallel = Parameter(
        "num_parallel", help="Number of nodes in cluster", default=3
    )

    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=self.num_parallel)

    @parallel
    @step
    def parallel_step(self):
        self.node_index = current.parallel.node_index
        self.num_nodes = current.parallel.num_nodes
        print("parallel_step: node {} finishing.".format(self.node_index))
        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        j = 0
        for input in inputs:
            assert input.node_index == j
            assert input.num_nodes == self.num_parallel
            j += 1
        assert j == self.num_parallel
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ParallelTest()
