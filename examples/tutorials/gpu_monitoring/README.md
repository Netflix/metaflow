# GPU Monitoring Example

This example demonstrates how to monitor GPU usage in Metaflow flows, which is essential for ML training workloads.

## Features

- GPU availability detection
- GPU memory monitoring
- Resource allocation with `@resources` decorator
- Simulated training workflow

## Running the Example

```bash
python gpu_flow.py run
```

To run with custom epochs:
```bash
python gpu_flow.py run --epochs 5
```

## Use Cases

This pattern is useful for:
- ML model training pipelines
- GPU utilization monitoring
- Cost optimization for GPU workloads
- Multi-GPU training setup validation

## Requirements

- Metaflow
- PyTorch (optional, for actual GPU detection)

## Notes

The example gracefully handles environments without GPUs or PyTorch installed,
making it suitable for testing in various environments.

## Example Output

When running on a GPU-enabled environment:
```
Starting GPU monitoring example flow
GPU Available: True
GPU Name: NVIDIA Tesla V100
GPU Memory: 16.00 GB
Training for 3 epochs...
Epoch 1/3 completed
  GPU Memory - Allocated: 0.12 GB, Reserved: 0.25 GB
Epoch 2/3 completed
  GPU Memory - Allocated: 0.12 GB, Reserved: 0.25 GB
Epoch 3/3 completed
  GPU Memory - Allocated: 0.12 GB, Reserved: 0.25 GB

Flow completed successfully!
Training completed: True
Used GPU: NVIDIA Tesla V100
```

When running on CPU-only environment:
```
Starting GPU monitoring example flow
PyTorch not installed, skipping GPU check
Training for 3 epochs...
Epoch 1/3 completed
Epoch 2/3 completed
Epoch 3/3 completed

Flow completed successfully!
Training completed: True
```

## Advanced Usage

### Monitoring Multiple GPUs

To extend this example for multi-GPU monitoring, you can modify the `check_gpu` step:

```python
@resources(gpu=2, memory=32000)
@step
def check_gpu(self):
    """Check multiple GPUs."""
    import torch
    if torch.cuda.is_available():
        gpu_count = torch.cuda.device_count()
        print(f"Number of GPUs: {gpu_count}")
        for i in range(gpu_count):
            print(f"GPU {i}: {torch.cuda.get_device_name(i)}")
```

### Real-time Monitoring

For production use cases, you might want to log GPU metrics to a monitoring system:

```python
# Log to your monitoring system
metrics = {
    'gpu_utilization': torch.cuda.utilization(),
    'gpu_memory_used': torch.cuda.memory_allocated(),
    'gpu_temperature': torch.cuda.temperature()
}
# Send metrics to CloudWatch, Datadog, etc.
```

## Related Examples

- See the `pytorch_tutorial` for more PyTorch-specific patterns
- Check `distributed_training` for multi-node GPU training examples

## Contributing

Found an issue or want to improve this example? Pull requests are welcome!