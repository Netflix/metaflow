import os

from metaflow import FlowSpec, step, parallel, project, current


@project(name="parallel_test")
class ParallelFlow(FlowSpec):
    """
    A flow that tests the @parallel decorator.

    The @parallel decorator creates multiple tasks (a "gang") that run
    concurrently. Each task sees current.parallel with num_nodes,
    node_index, and main_ip. The control task (node_index=0) coordinates
    the gang and collects results in the join step.
    """

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.next(self.train, num_parallel=2)

    @parallel
    @step
    def train(self):
        self.node_index = current.parallel.node_index
        self.num_nodes = current.parallel.num_nodes
        self.main_ip = current.parallel.main_ip
        self.next(self.join)

    @step
    def join(self, inputs):
        self.node_indices = sorted([inp.node_index for inp in inputs])
        self.total_nodes = inputs[0].num_nodes
        # Verify main_ip was set for all workers
        self.all_have_main_ip = all(
            inp.main_ip is not None and inp.main_ip != "" for inp in inputs
        )
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ParallelFlow()
