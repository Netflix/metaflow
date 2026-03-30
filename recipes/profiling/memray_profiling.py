"""
Memory profiling with `memray` (modern, low-overhead alternative).

`memray` is a fast, drop-in memory profiler from Bloomberg that produces
interactive flamegraphs and supports native (C/C++) stack frames. It has
become the de-facto standard for production memory profiling in Python as of
2023.

Usage:
    pip install memray
    memray run -o report.bin python memray_profiling.py run
    memray flamegraph report.bin          # opens an HTML flamegraph
    memray summary   report.bin           # text summary in terminal
    memray tree      report.bin           # allocation tree

For remote Metaflow tasks (AWS Batch / Kubernetes) you can capture the
`.bin` file as a Metaflow artifact and download it for local analysis:
    run = Run('MemrayProfilingFlow/42')
    with open('remote_report.bin', 'wb') as f:
        f.write(run['end'].task.data.memray_report)
"""

import io
import os
import tempfile

from metaflow import FlowSpec, step


def with_memray(output_artifact: str = "memray_report"):
    """
    Decorator factory that wraps a Metaflow step with memray profiling.

    The binary report is saved as a Metaflow artifact so it survives
    across remote executions.

    Parameters
    ----------
    output_artifact : str
        The name of the artifact that will store the raw memray binary.

    Example
    -------
    ::

        @with_memray("train_report")
        @step
        def train(self):
            ...
    """
    try:
        import memray
    except ImportError:
        # Graceful no-op when memray is not installed.
        def decorator(func):
            return func

        return decorator

    import functools

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self):
            with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
                tmp_path = f.name
            try:
                with memray.Tracker(tmp_path):
                    result = func(self)
                with open(tmp_path, "rb") as f:
                    setattr(self, output_artifact, f.read())
                print(
                    f"[memray] Report for '{func.__name__}' saved to "
                    f"artifact '{output_artifact}' "
                    f"({os.path.getsize(tmp_path):,} bytes). "
                    "Run `memray flamegraph <file>` locally to visualize."
                )
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            return result

        return wrapper

    return decorator


class MemrayProfilingFlow(FlowSpec):
    """
    A flow demonstrating memory profiling using `memray`.

    Each profiled step saves a raw memray binary blob as a Metaflow artifact.
    You can later download it and generate a flamegraph:

        from metaflow import Run
        run = Run('MemrayProfilingFlow/<RUN_ID>')
        with open('training.bin', 'wb') as f:
            f.write(run['train'].task.data.train_memray_report)

        # Then in your terminal:
        # memray flamegraph training.bin
    """

    @step
    def start(self):
        self.next(self.train)

    @with_memray("train_memray_report")
    @step
    def train(self):
        """
        Simulate a training workload: allocate a weight matrix and run
        several fake 'gradient update' iterations.
        """
        import random

        # Simulate a dense weight matrix (e.g., a small neural net layer)
        weights = [[random.gauss(0, 1) for _ in range(1024)] for _ in range(1024)]

        # Simulate gradient updates (triggers lots of small allocs)
        for _ in range(5):
            gradients = [[w * 0.01 for w in row] for row in weights]
            weights = [
                [w - g for w, g in zip(row, grad)]
                for row, grad in zip(weights, gradients)
            ]

        self.loss = sum(w for row in weights for w in row)
        self.next(self.end)

    @step
    def end(self):
        print(f"Final loss: {self.loss:.4f}")
        has_report = hasattr(self, "train_memray_report") and self.train_memray_report
        if has_report:
            size_kb = len(self.train_memray_report) / 1024
            print(f"memray report stored as artifact ({size_kb:.1f} KB).")
            print("Retrieve it with:")
            print("  from metaflow import Run")
            print("  run = Run('MemrayProfilingFlow/<RUN_ID>')")
            print("  with open('train.bin', 'wb') as f:")
            print("      f.write(run['end'].task.data.train_memray_report)")
            print("  # memray flamegraph train.bin")


if __name__ == "__main__":
    MemrayProfilingFlow()
