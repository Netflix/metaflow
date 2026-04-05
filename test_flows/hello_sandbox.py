from metaflow import FlowSpec, step, devcontainer
import platform

class HelloSandbox(FlowSpec):
    @devcontainer  
    @step
    def start(self):
        print(f"I am running on: {platform.system()} {platform.release()}")
        print("Hello from inside the Docker Hijack!")
        self.next(self.end)

    @step
    def end(self):
        print("Flow finished.")

if __name__ == '__main__':
    HelloSandbox()
