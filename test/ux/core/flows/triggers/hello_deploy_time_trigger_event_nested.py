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


def myfunction1(context):
    return f"{current.branch_name}.HelloDeployTimeTriggerEventNestedFlow"


def myfunction2(context):
    return [
        "param",
        (
            "param_field",
            "events_field",
        ),
    ]


@trigger(event={"name": myfunction1, "parameters": myfunction2})
@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class HelloDeployTimeTriggerEventNestedFlow(FlowSpec):
    config = Config("config")

    param = Parameter(
        "param",
        help="flow parameter",
        default="default_value",
    )
    param_field = Parameter(
        "param_field",
        help="flow parameter",
        default="default_value",
    )

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloDeployTimeTriggerEventNestedFlow")
        print(f"param: {self.param}")
        print(f"param_field: {self.param_field}")
        self.trigger = current.trigger
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloDeployTimeTriggerEventNestedFlow()
