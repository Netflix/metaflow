"""Flow testing string-based StepMutator addition via MutableStep.add_decorator."""

import os
from metaflow import FlowSpec, FlowMutator, step, environment
from metaflow.user_decorators.user_step_decorator import StepMutator


class StringAddedStepMutator(StepMutator):
    """StepMutator added by string name."""

    def init(self, env_val="default"):
        self._env_val = env_val

    def mutate(self, mutable_step):
        mutable_step.add_decorator(
            environment,
            deco_kwargs={"vars": {"STRING_STEP_MUTATOR": self._env_val}},
        )


class AdderFlowMutator(FlowMutator):
    """FlowMutator that adds a StepMutator by string name to all steps."""

    def pre_mutate(self, mutable_flow):
        for name, s in mutable_flow.steps:
            s.add_decorator("StringAddedStepMutator:env_val=from_string")


@AdderFlowMutator
class StringStepMutatorFlow(FlowSpec):

    @step
    def start(self):
        self.step_mutator_val = os.environ.get("STRING_STEP_MUTATOR", "NOT_SET")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    StringStepMutatorFlow()
