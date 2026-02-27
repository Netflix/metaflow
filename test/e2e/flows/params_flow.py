from metaflow import FlowSpec, step, Parameter


class ParameterFlow(FlowSpec):
    name = Parameter("name", default="world", type=str)

    @step
    def start(self):
        self.message = f"hello {self.name}"
        self.next(self.end)

    @step
    def end(self):
        print(self.message)


if __name__ == "__main__":
    ParameterFlow()
