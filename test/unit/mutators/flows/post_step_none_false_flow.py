"""Regression flow: a UserStepDecorator whose post_step returns (None, False).

Before the fix, (None, False) caused task.py to try to treat False as a valid
self.next override and raise RuntimeError("Invalid value passed to self.next").
(None, False) should be a no-op: no exception, no self.next override.
"""

from metaflow import FlowSpec, step
from metaflow.user_decorators.user_step_decorator import UserStepDecorator


class noop_post_step(UserStepDecorator):
    """post_step returns (None, False) — no exception, no override."""

    def pre_step(self, step_name, flow, inputs=None):
        flow._pre_step_ran = True

    def post_step(self, step_name, flow, exception=None):
        flow._post_step_ran = True
        return None, False


class PostStepNoneFalseFlow(FlowSpec):

    @noop_post_step
    @step
    def start(self):
        self.pre_step_ran = getattr(self, "_pre_step_ran", False)
        self.next(self.end)

    @step
    def end(self):
        self.post_step_ran = getattr(self, "_post_step_ran", False)


if __name__ == "__main__":
    PostStepNoneFalseFlow()
