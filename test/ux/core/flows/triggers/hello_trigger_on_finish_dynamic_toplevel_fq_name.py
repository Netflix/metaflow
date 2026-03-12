from metaflow import (
    FlowSpec,
    step,
    project,
    current,
    trigger_on_finish,
    config_expr,
    Config,
)


def myfunction(context):
    return f"{current.project_name}.{current.branch_name}.dummytriggerflow1"


@trigger_on_finish(flow=myfunction)
@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class TriggerOnFinishFlowDynamicTopLevelFQNameFlow(FlowSpec):
    config = Config("config")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In TriggerOnFinishFlowDynamicTopLevelFQNameFlow")
        self.trigger = current.trigger
        print(f"Project Flow name: {current.project_flow_name}")
        print(f"self.trigger: {self.trigger.event}")
        print(f"Trigger name: {self.trigger.event.name}")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TriggerOnFinishFlowDynamicTopLevelFQNameFlow()
