from metaflow import FlowSpec, step
import time


class ForeachLinearSplit(FlowSpec):
    """
    foreach -> linear -> split -> linear -> foreach -> join
    """

    @step
    def start(self):
        self.x = "ab"
        self.next(self.linear_1, foreach="x")

    @step
    def linear_1(self):
        # AIP-6717 sleeps to avoid Datadog OOM events because of too many pods
        # being created and decommissioned in a short time frame.
        time.sleep(1)
        self.next(self.split_a_and_b)

    @step
    def split_a_and_b(self):
        time.sleep(1)
        self.next(self.a, self.b)

    @step
    def a(self):
        time.sleep(1)
        self.next(self.foreach_split_a)

    @step
    def b(self):
        time.sleep(1)
        self.next(self.foreach_split_b)

    @step
    def foreach_split_a(self):
        time.sleep(1)
        self.z = "ef"
        self.next(self.linear_2, foreach="z")

    @step
    def foreach_split_b(self):
        time.sleep(1)
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @step
    def linear_2(self):
        time.sleep(1)
        self.next(self.foreach_join_a)

    @step
    def linear_3(self):
        time.sleep(1)
        self.next(self.foreach_join_b)

    @step
    def foreach_join_a(self, inputs):
        time.sleep(1)
        self.next(self.join_a_and_b)

    @step
    def foreach_join_b(self, inputs):
        time.sleep(1)
        self.next(self.join_a_and_b)

    @step
    def join_a_and_b(self, inputs):
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
    ForeachLinearSplit()
