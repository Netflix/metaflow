# -*- coding: utf-8 -*-
from metaflow import FlowSpec, step


class ForeachLinearForeach(FlowSpec):
    """
    foreach -> linear -> linear -> foreach -> linear -> linear -> join
    """

    @step
    def start(self):
        self.x = "ab"
        self.next(self.linear_1, foreach="x")

    @step
    def linear_1(self):
        self.next(self.linear_2)

    @step
    def linear_2(self):
        self.next(self.foreach_split_z)

    @step
    def foreach_split_z(self):
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @step
    def linear_3(self):
        self.next(self.linear_4)

    @step
    def linear_4(self):
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachLinearForeach()
