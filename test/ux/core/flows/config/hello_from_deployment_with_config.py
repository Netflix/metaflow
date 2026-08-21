from metaflow import FlowSpec, Config, step, config_expr, project

default_config = {"batch_size": 32, "packages": {"pandas": "2.2.3"}}


@project(name="hello_from_deployment_config")
class HelloFromDeploymentFlowConfig(FlowSpec):
    """Simple flow combining config, config_expr, and from_deployment testing."""

    simple_config = Config("simple_config", default_value=default_config)

    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        self.batch_size = self.simple_config.batch_size
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloFromDeploymentFlowConfig()
