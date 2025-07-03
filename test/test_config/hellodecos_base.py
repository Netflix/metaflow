from metaflow import FlowSpec, FlowMutator

from my_decorators import time_step


class MyMutator(FlowMutator):
    def mutate(self, flow):
        for step_name, step in flow.steps:
            if step_name == "start":
                step.add_decorator(time_step)


@MyMutator
class MyBaseFlowSpec(FlowSpec):
    pass
