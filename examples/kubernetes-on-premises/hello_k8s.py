from metaflow import FlowSpec, step, kubernetes
import socket

class HelloK8sFlow(FlowSpec):
    """
    A simple flow demonstrating Metaflow on on-premises Kubernetes.
    
    This flow runs locally, then executes a step on Kubernetes,
    demonstrating that Metaflow works on any Kubernetes cluster.
    """
    
    @step
    def start(self):
        """Start locally and prepare data."""
        print("Starting on local machine")
        self.message = "Hello from on-prem K8s!"
        self.next(self.process)
    
    @kubernetes(cpu=1, memory=500)
    @step
    def process(self):
        """Process on Kubernetes cluster."""
        print(f"Processing in Kubernetes: {self.message}")
        self.hostname = socket.gethostname()
        self.next(self.end)
    
    @step
    def end(self):
        """End locally with results from K8s."""
        print(f"Completed! Message from K8s pod {self.hostname}: {self.message}")

if __name__ == '__main__':
    HelloK8sFlow()
