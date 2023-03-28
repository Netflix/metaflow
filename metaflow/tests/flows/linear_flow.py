from metaflow import FlowSpec, step


class LinearFlow(FlowSpec):
    """Simple flow with 3 linear steps that read/write flow properties

    Also includes a `@property` and a method, verifying that they can coexist with `@step` functions
    """

    @step
    def start(self):
        self.next(self.one)

    @step
    def one(self):
        self.a = 111
        self.next(self.two)

    @step
    def two(self):
        self.b = self.a * 2
        self.next(self.three)

    @step
    def three(self):
        assert (self.a, self.b, self.foo, self.mth()) == (111, 222, "`foo`", "`mth`")
        self.checked = True
        self.next(self.end)

    @step
    def end(self):
        pass

    @property
    def foo(self):
        return "`foo`"

    def mth(self):
        return "`mth`"


if __name__ == "__main__":
    LinearFlow()
