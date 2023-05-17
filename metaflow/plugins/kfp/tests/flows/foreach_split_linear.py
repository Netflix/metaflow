from metaflow import FlowSpec, step
import time


class ForeachSplitLinear(FlowSpec):
    """
    foreach -> split -> linear -> foreach -> linear -> join
    """

    @step
    def start(self):
        self.x = "ab"
        self.next(self.split_a_b, foreach="x")

    @step
    def split_a_b(self):
        # AIP-6717 sleeps to avoid Datadog OOM events because of too many pods
        # being created and decommissioned in a short time frame.
        time.sleep(1)
        self.next(self.a, self.b)

    @step
    def a(self):
        time.sleep(1)
        self.z = "aa"
        self.next(self.linear_1, foreach="z")

    @step
    def b(self):
        time.sleep(1)
        self.z = "bb"
        self.next(self.linear_2, foreach="z")

    @step
    def linear_1(self):
        time.sleep(1)
        self.next(self.foreach_join_a)

    @step
    def linear_2(self):
        time.sleep(1)
        self.next(self.foreach_join_b)

    @step
    def foreach_join_a(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_a_and_b)

    @step
    def foreach_join_b(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_a_and_b)

    @step
    def foreach_join_a_and_b(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        time.sleep(1)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachSplitLinear()
