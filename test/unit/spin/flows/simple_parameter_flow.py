from metaflow import FlowSpec, step, Parameter


class SimpleParameterFlow(FlowSpec):
    alpha = Parameter("alpha", help="Learning rate", default=0.01)

    @step
    def start(self):
        print("SimpleParameterFlow is starting.")
        print(f"Parameter alpha is set to: {self.alpha}")
        self.a = 10
        self.b = 20
        self.next(self.end)

    @step
    def end(self):
        self.a = 50
        self.x = 100
        self.y = 200
        print("Parameter alpha in end step is: ", self.alpha)
        del self.a
        del self.x
        print("SimpleParameterFlow is all done.")


if __name__ == "__main__":
    SimpleParameterFlow()
