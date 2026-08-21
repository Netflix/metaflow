"""Flow testing string-based FlowMutator addition via MutableFlow.add_decorator."""

import os
from metaflow import FlowSpec, FlowMutator, step, environment


class StringAddedMutator(FlowMutator):
    """FlowMutator that is added by string name."""

    def init(self, tag="default_tag"):
        self._tag = tag

    def pre_mutate(self, mutable_flow):
        for name, s in mutable_flow.steps:
            s.add_decorator(
                environment,
                deco_kwargs={"vars": {"STRING_MUTATOR_TAG": self._tag}},
            )

    def mutate(self, mutable_flow):
        for name, s in mutable_flow.steps:
            for deco_name, _, _, deco_attrs in s.decorator_specs:
                if deco_name == "environment":
                    deco_attrs["vars"]["STRING_MUTATOR_MUTATE"] = "yes"
                    s.add_decorator(
                        environment,
                        deco_kwargs=deco_attrs,
                        duplicates=s.OVERRIDE,
                    )
                    break


class StringAdder(FlowMutator):
    """FlowMutator that adds StringAddedMutator by string name during pre_mutate."""

    def pre_mutate(self, mutable_flow):
        # Use the fully qualified module.class string to add the mutator
        mutable_flow.add_decorator(
            "StringAddedMutator:tag=from_string",
        )


@StringAdder
class StringFlowMutatorFlow(FlowSpec):

    @step
    def start(self):
        self.string_tag = os.environ.get("STRING_MUTATOR_TAG", "NOT_SET")
        self.string_mutate = os.environ.get("STRING_MUTATOR_MUTATE", "NOT_SET")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    StringFlowMutatorFlow()
