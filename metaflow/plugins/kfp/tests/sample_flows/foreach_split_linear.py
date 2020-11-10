# -*- coding: utf-8 -*-
from metaflow import FlowSpec, step


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
        self.next(self.a, self.b)

    @step
    def a(self):
        self.z = "aa"
        self.next(self.linear_1, foreach="z")

    @step
    def b(self):
        self.z = "bb"
        self.next(self.linear_2, foreach="z")

    @step
    def linear_1(self):
        self.next(self.foreach_join_a)

    @step
    def linear_2(self):
        self.next(self.foreach_join_b)

    @step
    def foreach_join_a(self, inputs):
        self.next(self.foreach_join_a_and_b)

    @step
    def foreach_join_b(self, inputs):
        self.next(self.foreach_join_a_and_b)

    @step
    def foreach_join_a_and_b(self, inputs):
        # pprint.pprint([(input.x, input.y, input.z) for input in inputs])
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachSplitLinear()
