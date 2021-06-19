from metaflow import api as ma, FlowSpec, Parameter
from metaflow.api import FlowSpecMeta, step


class ParameterFlow1(FlowSpec):
    debug = Parameter("debug", required=False, type=bool, default=None)

    @step
    def start(self):
        if self.debug is True:
            self.msg = "debug mode"
        elif self.debug is False:
            self.msg = "regular mode"
        else:
            assert self.debug is None
            self.msg = "default mode"
        self.next(self.end)

    @step
    def end(self):
        pass


class NewParameterFlow1(ma.FlowSpec):
    debug = Parameter("debug", required=False, type=bool, default=None)

    @step
    def run(self):
        if self.debug is True:
            self.msg = "debug mode"
        elif self.debug is False:
            self.msg = "regular mode"
        else:
            assert self.debug is None
            self.msg = "default mode"


class ParameterFlow2(FlowSpec):
    string = Parameter("str", required=False, type=str, default="default")

    @step
    def start(self):
        self.upper = self.string.upper()
        self.next(self.end)

    @step
    def end(self):
        pass


class NewParameterFlow2(ma.FlowSpec):
    string = Parameter("str", required=False, type=str, default="default")

    @step
    def run(self):
        self.upper = self.string.upper()


class ParameterFlow3(FlowSpec):
    int = Parameter("int", required=True, type=int, default=1)

    @step
    def start(self):
        self.squared = self.int * self.int
        self.next(self.end)

    @step
    def end(self):
        pass


class NewParameterFlow3(ma.FlowSpec):
    int = Parameter("int", required=True, type=int, default=1)

    @step
    def run(self):
        self.squared = self.int * self.int
