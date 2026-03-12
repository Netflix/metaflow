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


@trigger(
    events=[
        config_expr("config.branch + '.HelloStaticTriggerName'"),
        {
            "name": config_expr("config.branch + '.HelloStaticTriggerName1'"),
            "parameters": ["param1"],
        },
        {
            "name": config_expr("config.branch + '.HelloStaticTriggerName2'"),
            "parameters": [("param2", "param2_events_field")],
        },
        {
            "name": config_expr("config.branch + '.HelloStaticTriggerName3'"),
            "parameters": {"param3": "param3_events_field"},
        },
    ]
)
@project(name=config_expr("config.project"))
class HelloStaticTriggerFlow(FlowSpec):
    config = Config("config")
    param1 = Parameter("param1", type=str, default="default_value")
    param2 = Parameter("param2", type=str, default="default_value")
    param3 = Parameter("param3", type=str, default="default_value")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloStaticTriggerFlow")
        print(f"param1: {self.param1}")
        print(f"param2: {self.param2}")
        print(f"param3: {self.param3}")
        self.trigger = current.trigger
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloStaticTriggerFlow()
