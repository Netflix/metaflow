from metaflow import FlowSpec, step, retry, FlowMutator


class AddRetryToOnly(FlowMutator):
    """Adds @retry to every step. Verifies mutators reach the sole step."""

    def pre_mutate(self, mutable_flow):
        for _, s in mutable_flow.steps:
            s.add_decorator(retry, deco_kwargs={"times": 1})


@AddRetryToOnly
class SingleStepWithFlowMutatorFlow(FlowSpec):
    """Single-step flow with a FlowMutator applied at the class level."""

    @step(start=True, end=True)
    def only(self):
        self.v = 1


if __name__ == "__main__":
    SingleStepWithFlowMutatorFlow()
