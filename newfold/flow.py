from metaflow import FlowSpec, step

class ResumeFlow(FlowSpec):

    @step
    def start(self):
        print("Step 1")
        self.value = 10
        self.next(self.middle)

    @step
    def middle(self):
        print("Step 2 (will fail)")
        raise Exception("Simulated failure")

    @step
    def end(self):
        print("End")

if __name__ == '__main__':
    ResumeFlow()