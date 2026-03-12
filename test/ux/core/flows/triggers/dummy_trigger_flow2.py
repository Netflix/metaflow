from metaflow import FlowSpec, step, project, Config, config_expr, current
import time
import json
import uuid


@project(
    name=config_expr("config.project"),
    branch=config_expr("config.branch"),
)
class DummyTriggerFlow2(FlowSpec):
    config = Config("config")

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("In DummyTriggerFlow2")
        print(f"Project Flow name: {current.project_flow_name}")
        self.my_var = str(uuid.uuid4())[:8]
        self.next(self.end)

    @step
    def end(self):
        print("HelloFlow is all done.")


if __name__ == "__main__":
    DummyTriggerFlow2()
