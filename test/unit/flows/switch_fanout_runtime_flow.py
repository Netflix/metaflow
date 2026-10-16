from metaflow import FlowSpec, Parameter, step


class SwitchFanoutRuntimeFlow(FlowSpec):
    route = Parameter("route")

    @step
    def start(self):
        self.next(
            {
                "a": (self.a_split, self.a_foreach),
                "b": [self.b_one, self.b_two, self.b_three],
            },
            condition="route",
        )

    @step
    def a_split(self):
        self.next(self.a_left, self.a_right)

    @step
    def a_left(self):
        self.next(self.a_split_join)

    @step
    def a_right(self):
        self.next(self.a_split_join)

    @step
    def a_split_join(self, inputs):
        self.next(self.shared_join)

    @step
    def a_foreach(self):
        self.values = [1, 2]
        self.next(self.a_worker, foreach="values")

    @step
    def a_worker(self):
        self.next(self.a_foreach_join)

    @step
    def a_foreach_join(self, inputs):
        self.next(self.shared_join)

    @step
    def b_one(self):
        self.next(self.shared_join)

    @step
    def b_two(self):
        self.next(self.shared_join)

    @step
    def b_three(self):
        self.next(self.shared_join)

    @step
    def shared_join(self, inputs):
        self.input_count = sum(1 for _ in inputs)
        self.next(self.end)

    @step
    def end(self):
        print("INPUT_COUNT=%d" % self.input_count)


if __name__ == "__main__":
    SwitchFanoutRuntimeFlow()
