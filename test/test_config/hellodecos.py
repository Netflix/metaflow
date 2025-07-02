from my_decorators import (
    time_step,
    with_args,
    AddArgsDecorator,
    AddTimeStep,
    SkipStep,
)

from hellodecos_base import MyBaseFlowSpec

from metaflow import step
from metaflow import Config


class DecoFlow(MyBaseFlowSpec):
    cfg = Config(
        "cfg",
        default_value={
            "args_decorator": "with_args",
            "user_retry_decorator": "my_decorators.retry",
            "bar": 43,
        },
    )

    @step
    def start(self):
        print("Starting flow")
        print("Added decorators: ", self.user_added_step_decorators)
        assert self.user_added_step_decorators[0] == "time_step"
        self.next(self.m0)

    @time_step
    @with_args(foo=cfg.bar, bar="baz")
    @step
    def m0(self):
        print("Added decorators: ", self.user_added_step_decorators)
        assert self.user_added_step_decorators[0] == "time_step"
        assert (
            self.user_added_step_decorators[1] == "with_args({'foo': 43, 'bar': 'baz'})"
        )
        print("m0")
        self.next(self.m1)

    # Shows how a step can be totally skipped
    @SkipStep(skip_steps=["m1"])
    @step
    def m1(self):
        assert False, "This step should not be executed"
        self.next(self.m2)

    @AddArgsDecorator(bar=cfg.bar, baz="baz")
    @AddTimeStep
    @step
    def m2(self):
        print("Added decorators: ", self.user_added_step_decorators)
        assert (
            self.user_added_step_decorators[0] == "with_args({'bar': 43, 'baz': 'baz'})"
        )
        assert self.user_added_step_decorators[1] == "time_step"
        print("m2")
        self.next(self.end)

    @step
    def end(self):
        print("Flow completed successfully")


if __name__ == "__main__":
    DecoFlow()
