from metaflow.api import step, FlowSpec


# Note that the explicit `FlowSpec` inheritance here is optional (it will be added by the `Flow` metaclass if omitted).
# Including it helps IntelliJ to analyze/syntax-highlight member accesses downstream.
#
# Some functionality, like referencing `self.input` in a join step, gets flagged by Pylint if the FlowSpec-inheritance
# isn't made explicit.
#
# TODO: get Pylint to accept self.input references in Flows w/o FlowSpec explicitly specified
class NewLinearFlow(FlowSpec):
    @step
    def one(self):
        self.a = 111

    @step
    def two(self):
        self.b = self.a * 2

    @step
    def three(self):
        assert (self.a, self.b, self.foo, self.mth()) == (111, 222, "`foo`", "`mth`")
        self.checked = True

    @property
    def foo(self):
        return "`foo`"

    def mth(self):
        return "`mth`"


if __name__ == "__main__":
    NewLinearFlow()
