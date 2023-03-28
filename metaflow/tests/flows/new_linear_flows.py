"""Example file with multiple new-style Flows."""
from metaflow.api import FlowSpec, step


class NewLinearFlow1(FlowSpec):
    @step
    def aaa(self):
        self.a = 111

    @step
    def bbb(self):
        self.b = self.a * 2
        assert self.a == 111


class NewLinearFlow2(FlowSpec):
    @step
    def ccc(self):
        self.c = 333

    @step
    def ddd(self):
        self.d = 444
        assert self.c == 333


class NewLinearFlow3(FlowSpec):
    @step
    def eee(self):
        self.e = 555

    @step
    def fff(self):
        self.f = 666
        assert self.e == 555


if __name__ == "__main__":
    """`__main__` handlers are still supported when multiple flows coexist in a file"""
    NewLinearFlow2()
