from metaflow import FlowSpec, step


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
        self.next(self.split_a_and_b)

    @step
    def split_a_and_b(self):
        self.next(self.a, self.b)

    @step
    def a(self):
        self.next(self.foreach_split_a)

    @step
    def b(self):
        self.next(self.foreach_split_b)

    @step
    def foreach_split_a(self):
        self.z = "ef"
        self.next(self.linear_2, foreach="z")

    @step
    def foreach_split_b(self):
        self.z = "ef"
        self.next(self.linear_3, foreach="z")

    @step
    def linear_2(self):
        self.next(self.foreach_join_a)

    @step
    def linear_3(self):
        self.next(self.foreach_join_b)

    @step
    def foreach_join_a(self, inputs):
        self.next(self.join_a_and_b)

    @step
    def foreach_join_b(self, inputs):
        self.next(self.join_a_and_b)

    @step
    def join_a_and_b(self, inputs):
        self.next(self.foreach_join_start)

    @step
    def foreach_join_start(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachLinearSplit()
