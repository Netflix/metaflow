"""Flow testing dual UserStepDecorator + StepMutator inheritance."""

import os
from metaflow import FlowSpec, step, environment
from metaflow.user_decorators.user_step_decorator import UserStepDecorator, StepMutator


class dual_deco(UserStepDecorator, StepMutator):
    """A decorator that is both a UserStepDecorator and a StepMutator.

    mutate() adds an environment variable (proves config_decorators path works).
    pre_step() sets an artifact (proves wrappers path works).
    post_step() sets another artifact (proves wrappers path works).
    """

    def init(self, marker="default"):
        self._marker = marker

    def mutate(self, mutable_step):
        mutable_step.add_decorator(
            environment, deco_kwargs={"vars": {"DUAL_DECO_MUTATE": self._marker}}
        )

    def pre_step(self, step_name, flow, inputs=None):
        flow._dual_deco_pre_step_ran = True

    def post_step(self, step_name, flow, exception=None):
        flow._dual_deco_post_step_ran = True
        if exception:
            return exception, None
        return None, {}


class DualInheritFlow(FlowSpec):

    @dual_deco(marker="hello")
    @step
    def start(self):
        self.mutate_env_var = os.environ.get("DUAL_DECO_MUTATE", "NOT_SET")
        self.pre_step_ran = getattr(self, "_dual_deco_pre_step_ran", False)
        self.next(self.end)

    @step
    def end(self):
        self.post_step_ran = getattr(self, "_dual_deco_post_step_ran", False)


if __name__ == "__main__":
    DualInheritFlow()
