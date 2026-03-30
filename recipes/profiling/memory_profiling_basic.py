"""
Step-level memory profiling with `memory_profiler`.

Usage:
    pip install memory_profiler
    python memory_profiling_basic.py run

After the run completes, memory usage per step is printed to stdout.
You can also export a time-series flamegraph with:
    mprof run python memory_profiling_basic.py run
    mprof plot
"""

from metaflow import FlowSpec, step


def profile_memory(func):
    """
    A decorator that wraps a Metaflow step with memory_profiler's
    `memory_usage` utility, printing peak and delta RSS after each step.

    Falls back gracefully if memory_profiler is not installed.
    """
    try:
        from memory_profiler import memory_usage
    except ImportError:
        # No-op if memory_profiler is not available
        return func

    import functools

    @functools.wraps(func)
    def wrapper(self):
        mem_before = memory_usage(-1, interval=0.1, timeout=1)
        result = func(self)
        mem_after = memory_usage(-1, interval=0.1, timeout=1)
        peak_before = max(mem_before)
        peak_after = max(mem_after)
        print(
            f"[memory] Step '{func.__name__}': "
            f"before={peak_before:.1f} MiB, "
            f"after={peak_after:.1f} MiB, "
            f"delta={peak_after - peak_before:+.1f} MiB"
        )
        return result

    return wrapper


class MemoryProfilingBasicFlow(FlowSpec):
    """
    A simple flow demonstrating step-level memory profiling.

    This flow allocates and discards increasingly large lists of floats so you
    can observe how RSS changes between steps.

    Artifacts
    ---------
    sizes : list[int]
        The byte-sizes of the data allocated in each step.
    """

    @step
    def start(self):
        """Initialise the list that will track allocation sizes."""
        self.sizes = []
        self.next(self.small_alloc)

    @profile_memory
    @step
    def small_alloc(self):
        """Allocate ~10 MB of data."""
        data = [0.0] * (10 * 1024 * 128)  # ~10 MiB (float = 8 bytes)
        self.sizes.append(len(data) * 8)
        del data
        self.next(self.large_alloc)

    @profile_memory
    @step
    def large_alloc(self):
        """Allocate ~200 MB of data to make the peak clearly visible."""
        data = [0.0] * (200 * 1024 * 128)  # ~200 MiB
        self.sizes.append(len(data) * 8)
        del data
        self.next(self.end)

    @step
    def end(self):
        """Print a summary of bytes allocated across steps."""
        total_mb = sum(self.sizes) / (1024**2)
        print(f"Total data allocated across steps: {total_mb:.1f} MiB")


if __name__ == "__main__":
    MemoryProfilingBasicFlow()
