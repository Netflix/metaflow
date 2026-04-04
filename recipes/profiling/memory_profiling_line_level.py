"""
Line-level memory profiling with `memory_profiler`.

Usage:
    pip install memory_profiler
    python -m memory_profiler memory_profiling_line_level.py

The `@profile` decorator from memory_profiler annotates each *line* of the
decorated function with its memory increment, making it easy to pinpoint the
exact allocation that causes a memory spike.

Note: The `@profile` decorator is injected by memory_profiler at runtime when
you invoke `python -m memory_profiler <script>`. It must **not** be imported
from anywhere — just use it as a bare name. The try/except block below makes
the flow safe to `python flow.py run` without memory_profiler installed.
"""

from metaflow import FlowSpec, step

try:
    # When running with `python -m memory_profiler`, the decorator is
    # already injected as a built-in by the profiler's import hook.
    profile  # noqa: F821  (already defined by memory_profiler)
except NameError:
    # Running normally (python flow.py run) -- use a no-op decorator.
    def profile(func):
        return func


class MemoryProfilingLineLevelFlow(FlowSpec):
    """
    A flow demonstrating *line-level* memory profiling using memory_profiler.

    Run with:
        python -m memory_profiler memory_profiling_line_level.py run

    The output will show memory usage per line for every decorated method,
    for example::

        Line #    Mem usage    Increment   Line Contents
        ================================================
            10    52.3 MiB     52.3 MiB   @profile
            ...
            14    62.5 MiB    +10.2 MiB       data = [0.0] * (10 * 1024 * 128)
    """

    @step
    def start(self):
        self.next(self.process)

    @profile
    @step
    def process(self):
        """
        This step's lines will each show their memory delta.
        Inspect the output to find which line allocates the most.
        """
        # Line-level annotations will show memory after each assignment:
        small = [0] * 1_000_000  # ~8 MiB
        medium = [0.0] * 5_000_000  # ~40 MiB
        large = list(range(10_000_000))  # ~80 MiB

        # Releasing memory explicitly:
        del large  # -80 MiB visible on next line
        result = sum(small) + sum(medium)

        self.total = result
        self.next(self.end)

    @step
    def end(self):
        print(f"Result: {self.total}")


if __name__ == "__main__":
    MemoryProfilingLineLevelFlow()
