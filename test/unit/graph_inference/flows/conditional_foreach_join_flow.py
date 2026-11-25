from metaflow import FlowSpec, current, step


class ConditionalForeachJoinFlow(FlowSpec):
    @step
    def start(self):
        self.groups = [["left", "right"], ["right", "left"]]
        self.next(self.fan_out, foreach="groups")

    @step
    def fan_out(self):
        self.items = self.input
        self.next(self.choose, foreach="items")

    @step
    def choose(self):
        self.route = self.input
        self.next({"left": self.left, "right": self.right}, condition="route")

    @step
    def left(self):
        self.branch_pathspec = current.pathspec
        self.next(self.join_items)

    @step
    def right(self):
        self.branch_pathspec = current.pathspec
        self.next(self.join_items)

    @step
    def join_items(self, inputs):
        self.input_pathspecs = [item.branch_pathspec for item in inputs]
        self.next(self.join_groups)

    @step
    def join_groups(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConditionalForeachJoinFlow()
