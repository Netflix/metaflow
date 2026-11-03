import time

from metaflow import FlowSpec, step


class SpinDecospecFlow(FlowSpec):
    @step
    def start(self):
        time.sleep(3)
        self.done = True
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SpinDecospecFlow()
