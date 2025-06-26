from test_included_modules.my_decorators import (
    time_step,
    with_args,
    MyComplexDecorator,
)

from hellodecos_base import MyBaseFlowSpec

from metaflow import FlowSpec, step
from metaflow import config_expr, Config


class DecoFlow(MyBaseFlowSpec):

    cfg = Config("cfg", default_value={"bar": 43})

    @step
    def start(self):
        print("Starting flow")
        self.next(self.middle)

    @time_step
    @with_args(foo=cfg.bar, bar="baz")
    @step
    def middle(self):
        print("Middle step")
        self.next(self.middle2)

    @MyComplexDecorator(skip_steps=["middle2"])
    # TODO: Does not yet work with middle2 because we need to update the transition
    # manually. It does skip the step though :)
    @step
    def middle2(self):
        print("Middle2 step")
        self.next(self.end)

    @MyComplexDecorator(excluded_step_names=["end"])
    @step
    def end(self):
        print("Ending flow")


if __name__ == "__main__":
    DecoFlow()
