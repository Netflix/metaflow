"""
GPU Monitoring Example for Metaflow

This example demonstrates how to monitor GPU usage in Metaflow flows,
which is useful for ML training workloads.
"""

from metaflow import FlowSpec, step, resources, Parameter
import time

class GPUMonitorFlow(FlowSpec):
    """
    A flow that demonstrates GPU monitoring capabilities in Metaflow.
    
    This is particularly useful for ML engineers working on training
    infrastructure who need to track GPU utilization and memory usage.
    """
    
    epochs = Parameter('epochs', default=3, help='Number of training epochs')
    
    @step
    def start(self):
        """Initialize the flow and check GPU availability."""
        print("Starting GPU monitoring example flow")
        self.next(self.check_gpu)
    
    @resources(gpu=1, memory=16000)
    @step
    def check_gpu(self):
        """Check GPU availability and print GPU information."""
        try:
            import torch
            self.gpu_available = torch.cuda.is_available()
            if self.gpu_available:
                self.gpu_name = torch.cuda.get_device_name(0)
                self.gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1e9
                print(f"GPU Available: {self.gpu_available}")
                print(f"GPU Name: {self.gpu_name}")
                print(f"GPU Memory: {self.gpu_memory:.2f} GB")
            else:
                print("No GPU available, will simulate training")
        except ImportError:
            print("PyTorch not installed, skipping GPU check")
            self.gpu_available = False
        
        self.next(self.train)
    
    @resources(gpu=1, memory=16000, cpu=4)
    @step
    def train(self):
        """Simulate a training workload with GPU monitoring."""
        print(f"Training for {self.epochs} epochs...")
        
        for epoch in range(self.epochs):
            # Simulate training
            time.sleep(1)
            print(f"Epoch {epoch + 1}/{self.epochs} completed")
            
            # In real scenario, you would monitor GPU here
            if hasattr(self, 'gpu_available') and self.gpu_available:
                try:
                    import torch
                    # Check GPU memory usage
                    allocated = torch.cuda.memory_allocated() / 1e9
                    reserved = torch.cuda.memory_reserved() / 1e9
                    print(f"  GPU Memory - Allocated: {allocated:.2f} GB, Reserved: {reserved:.2f} GB")
                except:
                    pass
        
        self.training_completed = True
        self.next(self.end)
    
    @step
    def end(self):
        """Finalize the flow and print summary."""
        print("\nFlow completed successfully!")
        print(f"Training completed: {self.training_completed}")
        if hasattr(self, 'gpu_name'):
            print(f"Used GPU: {self.gpu_name}")

if __name__ == '__main__':
    GPUMonitorFlow()