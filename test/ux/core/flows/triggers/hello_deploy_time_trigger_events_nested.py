from metaflow import (
    FlowSpec,
    step,
    Parameter,
    trigger,
    current,
    config_expr,
    Config,
    project,
)


def myfunc1(context):
    return f"{current.branch_name}.HelloDeployTimeTriggerEventsNestedFlow1"


def myfunc2(context):
    return ["param1"]


def myfunc3(context):
    return ["param2"]


def myfunc4(context):
    return {
        "name": config_expr(
            "config.branch + '.HelloDeployTimeTriggerEventsNestedFlow3'"
        ),
        "parameters": ["param3"],
    }


@trigger(
    events=[
        config_expr("config.branch + '.HelloDeployTimeTriggerEventsNestedFlow'"),
        {
            "name": myfunc1,
            "parameters": myfunc2,
        },
        {
            "name": config_expr(
                "config.branch + '.HelloDeployTimeTriggerEventsNestedFlow2'"
            ),
            "parameters": myfunc3,
        },
        myfunc4,
        {
            "name": config_expr(
                "config.branch + '.HelloDeployTimeTriggerEventsNestedFlow4'"
            ),
            "parameters": {"param4": "param_from_trigger"},
        },
    ]
)
@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class HelloDeployTimeTriggerEventsNestedFlow(FlowSpec):
    config = Config("config")
    param1 = Parameter("param1", type=str, default="default_value")
    param2 = Parameter("param2", type=str, default="default_value")
    param3 = Parameter("param3", type=str, default="default_value")
    param4 = Parameter("param4", type=str, default="default_value")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloDeployTimeTriggerEventsNestedFlow")
        print(f"param1: {self.param1}")
        print(f"param2: {self.param2}")
        print(f"param3: {self.param3}")
        print(f"param4: {self.param4}")
        self.trigger = current.trigger
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloDeployTimeTriggerEventsNestedFlow()
