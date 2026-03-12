from metaflow import FlowSpec, step, secrets, project


@project(name="hello_secrets")
class HelloSecretsFlow(FlowSpec):
    @step
    def start(self):
        print("HelloSecretsFlow is starting.")
        self.next(self.hello)

    @secrets(
        sources=[
            {
                "type": "inline",
                "id": "test-secret",
                "options": {"env_vars": {"MY_SECRET_VALUE": "hellosecrets"}},
            }
        ]
    )
    @step
    def hello(self):
        import os

        self.secrets = os.environ["MY_SECRET_VALUE"]
        print("Hello secrets: %s" % self.secrets)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloSecretsFlow()
