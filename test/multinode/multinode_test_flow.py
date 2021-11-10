from metaflow import FlowSpec, step, batch, retry, resources, Parameter, multinode
import os


class MultinodeTest(FlowSpec):

    cluster_size = Parameter(
        "cluster_size", help="Number of nodes in cluster", default=3
    )

    @step
    def start(self):
        print("Start")
        self.next(self.multinode_step, cluster_size=self.cluster_size)

    @multinode
    # @batch
    @step
    def multinode_step(self):
        self.node_index = int(os.environ["MF_MULTINODE_NODE_INDEX"])
        self.num_nodes = int(os.environ["MF_MULTINODE_NUM_NODES"])
        print("multinode_step: node {} finishing.".format(self.node_index))
        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        j = 0
        for input in inputs:
            assert input.node_index == j
            assert input.num_nodes == self.cluster_size
            j += 1
        assert j == self.cluster_size
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MultinodeTest()
