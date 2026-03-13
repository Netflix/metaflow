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


@trigger(event=config_expr("config.branch + '.HelloDeployTimeTriggerTopLevelNameFlow'"))
@project(name="dummy_project")
class HelloDeployTimeTriggerTopLevelNameFlow(FlowSpec):
    config = Config("config")

    param1 = Parameter("param1", type=str, default="default_value")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloDeployTimeTriggerTopLevelNameFlow")
        self.trigger = current.trigger
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloDeployTimeTriggerTopLevelNameFlow()
