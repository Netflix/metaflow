"""Example file with multiple flows."""

from metaflow import FlowSpec, step


class LinearFlow1(FlowSpec):
    @step
    def start(self):
        self.a = 111
        self.next(self.end)

    @step
    def end(self):
        self.b = 222
        assert self.a == 111


class LinearFlow2(FlowSpec):
    @step
    def start(self):
        self.c = 333
        self.next(self.end)

    @step
    def end(self):
        self.d = 444
        assert self.c == 333


class LinearFlow3(FlowSpec):
    @step
    def start(self):
        self.e = 555
        self.next(self.end)

    @step
    def end(self):
        self.f = 666
        assert self.e == 555


if __name__ == "__main__":
    """`__main__` handlers are still supported when multiple flows coexist in a file"""
    LinearFlow2()
