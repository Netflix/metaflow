from metaflow import FlowSpec, step, devcontainer
import os

class HelloSandbox(FlowSpec):
    @devcontainer
    @step
    def start(self):
        dev_name = os.environ.get("DEVELOPER", "Unknown")
        stage = os.environ.get("PROJECT_STAGE", "Unknown")
        print (dict(os.environ)) 
        print(f"Hello {dev_name}! We are in the {stage} stage.")
        print("This message came from inside a spec-defined container.")
        self.next(self.end)

    @step
    def end(self):
        print("Success.")

if __name__ == '__main__':
    HelloSandbox()
