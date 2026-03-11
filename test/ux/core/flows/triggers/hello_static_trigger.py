"""
Flow with @trigger decorator using static event parameter mappings.

Tests four parameter mapping patterns:
1. Simple string name (param maps to same-named event field)
2. List of strings
3. Tuple mapping (flow_param, event_field)
4. Dict mapping {flow_param: event_field}

This flow is 100% generic — no orchestrator-specific imports.
"""

from metaflow import FlowSpec, Parameter, project, step, trigger


@trigger(
    event={
        "name": "test_event",
        "parameters": {
            "alpha": "event_alpha",
        },
    }
)
@project(name="hello_static_trigger")
class HelloStaticTriggerFlow(FlowSpec):
    alpha = Parameter("alpha", type=str, default="default_alpha")

    @step
    def start(self):
        self.received_alpha = self.alpha
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloStaticTriggerFlow()
