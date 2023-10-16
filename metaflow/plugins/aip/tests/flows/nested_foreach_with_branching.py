from metaflow import FlowSpec, step

import time


class NestedForeachWithBranching(FlowSpec):
    """
    split -> foreach -> foreach -> foreach -> linear -> linear -> join -> join -> join -> join (with below split)
          -> split -> join  (with above split)
    """

    @step
    def start(self):
        self.next(self.foreach_split_x, self.split_w)

    @step
    def foreach_split_x(self):
        # AIP-6717 sleeps to avoid Datadog OOM events because of too many pods
        # being created and decommissioned in a short time frame.
        time.sleep(1)
        self.x = "ab"
        self.next(self.foreach_split_y, foreach="x")

    @step
    def split_w(self):
        time.sleep(1)
        self.var1 = 100
        self.next(self.w1, self.w2)

    @step
    def w1(self):
        time.sleep(1)
        self.var1 = 150
        self.next(self.join_w)

    @step
    def w2(self):
        time.sleep(1)
        self.var1 = 250
        self.next(self.join_w)

    @step
    def join_w(self, w_inp):
        time.sleep(1)
        self.var1 = w_inp.w1.var1
        assert self.var1 == 150
        self.next(self.foreach_join_w_x)

    @step
    def foreach_split_y(self):
        time.sleep(1)
        self.y = "cd"
        self.next(self.foreach_split_z, foreach="y")

    @step
    def foreach_split_z(self):
        time.sleep(1)
        self.z = "ef"
        self.next(self.foreach_inner, foreach="z")

    @step
    def foreach_inner(self):
        time.sleep(1)
        [x, y, z] = self.foreach_stack()

        # assert that lengths are correct
        assert len(self.x) == x[1]
        assert len(self.y) == y[1]
        assert len(self.z) == z[1]

        # assert that variables are correct given their indices
        assert x[2] == self.x[x[0]]
        assert y[2] == self.y[y[0]]
        assert z[2] == self.z[z[0]]

        self.combo = x[2] + y[2] + z[2]
        self.next(self.foreach_inner_2)

    @step
    def foreach_inner_2(self):
        time.sleep(1)
        assert self.input in "ef"
        self.next(self.foreach_join_z)

    @step
    def foreach_join_z(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_y)

    @step
    def foreach_join_y(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        time.sleep(1)
        self.next(self.foreach_join_w_x)

    @step
    def foreach_join_w_x(self, input):
        time.sleep(1)
        assert input.join_w.var1 == 150
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeachWithBranching()
