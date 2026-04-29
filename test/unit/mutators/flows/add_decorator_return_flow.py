"""Flow testing that MutableStep.add_decorator returns the decorator instance."""

import os
from metaflow import FlowSpec, step, environment
from metaflow.user_decorators.user_step_decorator import StepMutator


class check_return(StepMutator):
    """StepMutator that verifies add_decorator returns the decorator instance."""

    def mutate(self, mutable_step):
        # Adding a new decorator should return the instance
        deco = mutable_step.add_decorator(
            environment, deco_kwargs={"vars": {"ADDED_VAR": "from_mutator"}}
        )
        # Store info about the returned decorator as class-level markers
        # (we can't set flow artifacts here since mutate runs at config time)
        check_return._returned_deco = deco
        check_return._returned_is_none = deco is None

        # Adding a duplicate with IGNORE should return None
        deco2 = mutable_step.add_decorator(
            environment,
            deco_kwargs={"vars": {"SHOULD_NOT_EXIST": "1"}},
            duplicates=mutable_step.IGNORE,
        )
        check_return._duplicate_is_none = deco2 is None


class AddDecoratorReturnFlow(FlowSpec):

    @check_return
    @step
    def start(self):
        self.added_var = os.environ.get("ADDED_VAR", "NOT_SET")
        self.should_not_exist = os.environ.get("SHOULD_NOT_EXIST")
        # Transfer class-level markers to artifacts
        self.returned_is_none = check_return._returned_is_none
        self.duplicate_is_none = check_return._duplicate_is_none
        self.returned_has_name = hasattr(check_return._returned_deco, "name")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    AddDecoratorReturnFlow()
