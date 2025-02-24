from metaflow import FlowSpec, Runner, step


class RunnerFlow(FlowSpec):
    @step
    def start(self):
        with Runner("./mutable_flow.py") as r:
            r.run()
        self.next(self.end)

    @step
    def end(self):
        print("Done")


if __name__ == "__main__":
    RunnerFlow()
