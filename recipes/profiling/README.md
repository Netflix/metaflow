# Memory Profiling Recipes for Metaflow

This directory contains ready-to-run examples for profiling memory usage in
Metaflow flows. Profiling is essential for debugging out-of-memory (OOM)
errors, optimising step resource allocation, and understanding how data moves
through your pipeline.

Closes #432.

---

## Why Profile Memory?

- **Right-size your `@resources` requests** – know exactly how much memory a
  step needs before setting `memory=` on AWS Batch or Kubernetes.
- **Hunt down leaks** – find allocations that accumulate over many tasks.
- **Spot the expensive line** – identify the exact line (e.g. a `pd.merge`)
  that spikes memory, not just the step.

---

## Tools

| Tool | Best for | Output |
|---|---|---|
| [`memory_profiler`](https://github.com/pythonprofilers/memory_profiler) | Quick step-level or line-level RSS tracking | Terminal table / `mprof` plot |
| [`memray`](https://github.com/bloomberg/memray) | Production-grade flamegraphs, native frames | Interactive HTML flamegraph |

---

## Recipes

### 1. Step-level profiling — `memory_profiling_basic.py`

Wraps each step with a lightweight decorator that reports peak RSS before and
after the step body runs.

**Install**

```bash
pip install memory_profiler
```

**Run**

```bash
python memory_profiling_basic.py run
```

**Sample output**

```
[memory] Step 'small_alloc': before=52.3 MiB, after=62.5 MiB, delta=+10.2 MiB
[memory] Step 'large_alloc': before=62.5 MiB, after=262.8 MiB, delta=+200.3 MiB
```

**Time-series plot** (records memory every 100 ms over the full run)

```bash
mprof run python memory_profiling_basic.py run
mprof plot          # opens an interactive matplotlib window
mprof plot --output memory_timeline.png
```

---

### 2. Line-level profiling — `memory_profiling_line_level.py`

Uses `memory_profiler`'s `@profile` decorator to annotate every **line** of a
step, showing exactly how much memory is added or freed by each statement.

**Install**

```bash
pip install memory_profiler
```

**Run** (must use the `-m memory_profiler` flag, not plain `python`)

```bash
python -m memory_profiler memory_profiling_line_level.py run
```

**Sample output**

```
Line #    Mem usage    Increment   Line Contents
================================================
    52     52.3 MiB     52.3 MiB   @profile
    53                             @step
    54                             def process(self):
    57     60.4 MiB     +8.1 MiB       small = [0] * 1_000_000
    58    100.4 MiB    +40.0 MiB       medium = [0.0] * 5_000_000
    59    180.5 MiB    +80.1 MiB       large = list(range(10_000_000))
    61    100.4 MiB    -80.1 MiB       del large
```

> **Tip:** You can decorate multiple steps; each one gets its own table.

---

### 3. Production profiling with `memray` — `memray_profiling.py`

`memray` provides native-frame flamegraphs with near-zero overhead and is the
recommended choice for profiling remote tasks (AWS Batch, Kubernetes). The
`@with_memray` decorator saves the raw binary report as a **Metaflow
artifact**, so you can download it even after a remote run finishes.

**Install**

```bash
pip install memray
```

**Local run**

```bash
memray run -o report.bin python memray_profiling.py run
memray flamegraph report.bin     # writes train_flamegraph.html
memray summary   report.bin      # prints allocation table to stdout
memray tree      report.bin      # prints allocation tree to stdout
```

**Remote run (AWS Batch / Kubernetes)**

```bash
python memray_profiling.py run --with batch
```

Retrieve the report after the run:

```python
from metaflow import Run

run = Run("MemrayProfilingFlow/<RUN_ID>")
with open("train.bin", "wb") as f:
    f.write(run["end"].task.data.train_memray_report)
```

Then generate visuals locally:

```bash
memray flamegraph train.bin      # opens train_flamegraph.html
```

---

## Using `@conda` or `@pypi` to install profilers inline

You can install profilers as part of the step environment without touching your
base image:

```python
from metaflow import FlowSpec, step, pypi

class MyFlow(FlowSpec):

    @pypi(packages={"memray": "1.13.4"})
    @step
    def heavy_step(self):
        import memray
        with memray.Tracker("output.bin"):
            # ... your heavy computation ...
            pass
        self.next(self.end)

    @step
    def end(self):
        pass
```

---

## Further Reading

- [`memory_profiler` docs](https://github.com/pythonprofilers/memory_profiler)
- [`memray` docs](https://bloomberg.github.io/memray/)
- [Metaflow `@resources` decorator](https://docs.metaflow.org/api/decorators/resources)
- [Metaflow Client API](https://docs.metaflow.org/metaflow/client)
- Original issue: [Netflix/metaflow#432](https://github.com/Netflix/metaflow/issues/432)
