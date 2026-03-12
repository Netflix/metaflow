from metaflow import (
    FlowSpec,
    step,
    project,
    trigger_on_finish,
    current,
    Config,
    config_expr,
)


@trigger_on_finish(flow="DummyTriggerFlow1")
@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class HelloTriggerOnFinishLocalFlow(FlowSpec):
    config = Config("config")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In HelloTriggerOnFinishLocalFlow")
        self.fake_flow_var = current.trigger.run.data.my_var
        print(f"fake_flow_var: {self.fake_flow_var}")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloTriggerOnFinishLocalFlow()
