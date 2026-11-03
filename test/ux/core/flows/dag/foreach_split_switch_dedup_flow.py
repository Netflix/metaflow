from metaflow import FlowSpec, step


class ForeachJoinDedupFlow(FlowSpec):
    @step
    def start(self):
        self.optional_mode = "run"
        self.next(
            {"skip": self.fan_gate, "run": self.optional_step},
            condition="optional_mode",
        )

    @step
    def optional_step(self):
        self.next(self.fan_gate)

    @step
    def fan_gate(self):
        self.fan_mode = "run"
        self.next(
            {"skip": self.end, "run": self.fan_out},
            condition="fan_mode",
        )

    @step
    def fan_out(self):
        self.scan_items = [1, 2]
        self.next(self.fan_step, foreach="scan_items")

    @step
    def fan_step(self):
        self.next(self.join_step)

    @step
    def join_step(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachJoinDedupFlow()
