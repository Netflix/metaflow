from metaflow import (
    FlowSpec,
    step,
    batch,
    current,
    resources,
    dask_distributed,
    Parameter,
)


class DaskDistributed(FlowSpec):
    """
    Test flow to test @parallel dask.
    """

    num_parallel = Parameter(
        "num_parallel", help="Number of nodes in cluster", default=3
    )

    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=self.num_parallel)

    @dask_distributed
    #  @batch(image="daskdev/dask")
    @resources(cpu=4, memory=30000, shared_memory=4000)
    @step
    def parallel_step(self):
        from dask.distributed import Client
        import dask.array as da

        client = Client("{}:{}".format(current.parallel.main_ip, 8786))
        x = da.random.random(size=(1000, 1000), chunks=(100, 100))

        self.mean = client.compute(x.mean(axis=0).mean(axis=0)).result()
        print("mean: ", self.mean)
        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        for input in inputs:
            if hasattr(input, "mean"):
                self.mean = input.mean
        print("mean=", self.mean)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DaskDistributed()
