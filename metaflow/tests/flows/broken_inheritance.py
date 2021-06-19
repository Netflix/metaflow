from metaflow.api import FlowSpec, step


class A1(FlowSpec):
    @step
    def a(self):
        pass


class A2(FlowSpec):
    @step
    def a(self):
        pass


class A(A1, A2):
    pass
