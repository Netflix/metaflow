from metaflow import (
    FlowSpec,
    step,
    project,
    current,
    trigger,
    config_expr,
    Config,
    Parameter,
)


def myfunction(context):
    return [
        {
            "name": f"{current.branch_name}.HelloDeployTimeTriggerTopLevelListFlow1",
            "parameters": ["param1"],
        },
        {
            "name": f"{current.branch_name}.HelloDeployTimeTriggerTopLevelListFlow2",
            "parameters": ["param2"],
        },
    ]


@trigger(events=myfunction)
@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class HelloDeployTimeTriggerTopLevelListFlow(FlowSpec):
    config = Config("config")

    param1 = Parameter(
        "param1", type=str, default="default_value", help="flow parameter"
    )
    param2 = Parameter(
        "param2", type=str, default="default_value", help="flow parameter"
    )

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloDeployTimeTriggerTopLevelListFlow")
        print("param1: ", self.param1)
        print("param2: ", self.param2)
        self.trigger = current.trigger
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloDeployTimeTriggerTopLevelListFlow()
