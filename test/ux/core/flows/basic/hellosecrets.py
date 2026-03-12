from metaflow import FlowSpec, step, secrets, project


@project(name="hello_secrets")
class HelloSecretsFlow(FlowSpec):
    @step
    def start(self):
        from metaflow import metaflow_version

        print(f"In start step and using metaflow: {metaflow_version.get_version()}")
        print("HelloSecretsFlow is starting.")
        self.next(self.hello)

    @secrets(sources=["mf_secrets_test"])
    @step
    def hello(self):
        import os

        self.secrets = os.environ["mf_secrets_test"]
        print("Hello secrets: %s" % self.secrets)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloSecretsFlow()
