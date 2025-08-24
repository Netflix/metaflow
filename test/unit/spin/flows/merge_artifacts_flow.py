from metaflow import FlowSpec, step


class MergeArtifactsFlow(FlowSpec):

    @step
    def start(self):
        self.pass_down = "a"
        self.next(self.a, self.b)

    @step
    def a(self):
        self.common = 5
        self.x = 1
        self.y = 3
        self.from_a = 6
        self.next(self.join)

    @step
    def b(self):
        self.common = 5
        self.x = 2
        self.y = 4
        self.next(self.join)

    @step
    def join(self, inputs):
        self.x = inputs.a.x
        self.merge_artifacts(inputs, exclude=["y"])
        print("x is %s" % self.x)
        print("pass_down is %s" % self.pass_down)
        print("common is %d" % self.common)
        print("from_a is %d" % self.from_a)
        self.next(self.c)

    @step
    def c(self):
        self.next(self.d, self.e)

    @step
    def d(self):
        self.conflicting = 7
        self.next(self.join2)

    @step
    def e(self):
        self.conflicting = 8
        self.next(self.join2)

    @step
    def join2(self, inputs):
        self.merge_artifacts(inputs, include=["pass_down", "common"])
        print("Only pass_down and common exist here")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MergeArtifactsFlow()
