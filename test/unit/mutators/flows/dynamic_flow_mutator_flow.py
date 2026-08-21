"""Flow testing dynamic FlowMutator addition via MutableFlow.add_decorator."""

import os
from metaflow import FlowSpec, FlowMutator, step, environment


class InnerMutator(FlowMutator):
    """FlowMutator added dynamically by OuterMutator."""

    def init(self, env_key="INNER_PRE"):
        self._env_key = env_key

    def pre_mutate(self, mutable_flow):
        # Add an environment variable to all steps to prove pre_mutate ran
        for name, s in mutable_flow.steps:
            s.add_decorator(
                environment,
                deco_kwargs={"vars": {self._env_key: "inner_pre_mutate_ran"}},
            )

    def mutate(self, mutable_flow):
        # Add another environment variable to prove mutate ran
        for name, s in mutable_flow.steps:
            for deco_name, _, _, deco_attrs in s.decorator_specs:
                if deco_name == "environment":
                    deco_attrs["vars"]["INNER_MUTATE"] = "inner_mutate_ran"
                    s.add_decorator(
                        environment,
                        deco_kwargs=deco_attrs,
                        duplicates=s.OVERRIDE,
                    )
                    break


class OuterMutator(FlowMutator):
    """FlowMutator that dynamically adds InnerMutator during pre_mutate."""

    def pre_mutate(self, mutable_flow):
        # Dynamically add InnerMutator — it should get its pre_mutate called
        # by the ongoing iteration
        mutable_flow.add_decorator(InnerMutator)


@OuterMutator
class DynamicFlowMutatorFlow(FlowSpec):

    @step
    def start(self):
        self.inner_pre = os.environ.get("INNER_PRE", "NOT_SET")
        self.inner_mutate = os.environ.get("INNER_MUTATE", "NOT_SET")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DynamicFlowMutatorFlow()
