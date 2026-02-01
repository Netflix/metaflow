# Metaflow GSoC 2026 Ideas List

## Open Source Metaflow Functions: Relocatable Compute with Ray and FastAPI Backends

**Difficulty:** Medium/Advanced

**Duration:** 350 hours (Large project)

**Technologies:** Python, Metaflow, Ray, FastAPI

**Mentors:** Shashank Srikanth, Nissan Pow

### Description

Metaflow Functions is a construct that enables relocatable compute; the 
ability to package a computation along with its dependencies, environment, and 
bound artifacts into a self-contained unit that can be deployed anywhere. 
The core implementation already exists and has been 
[presented publicly](https://www.infoq.com/presentations/ml-netflix/) with 
code available 
[here](https://github.com/Netflix/metaflow_rc/tree/master/nflx-metaflow-function).

The `@function` decorator solves a key pain point in ML workflows: 
dependency management across the training-to-serving boundary. When you train a 
model in a Metaflow flow, the function captures the exact environment 
(Python version, packages, custom code) and binds it with 
[task](https://docs.metaflow.org/api/client#task) artifacts. 
The resulting package can be loaded and executed in a completely different 
process or machine without the caller needing to reconstruct the original 
environment.

The goal of this project would be to open-source Metaflow 
Functions for the broader community by implementing two production-ready backends:
- **Ray backend** for distributed batch/offline inference
- **FastAPI backend** for real-time online serving

See [Metaflow Functions Expected API](#metaflow-functions-expected-api) for code
examples.

### Goals

1. **Open source the @function primitive** - Create a new Metaflow extension 
(`metaflow-functions`) that implements the `@function` decorator and 
`JsonFunction` binding.

2. **Ray backend for offline serving** - Deploy functions to Ray for scalable 
batch inference.

3. **FastAPI backend for online serving** - Wrap functions as HTTP endpoints for
real-time inference with automatic OpenAPI documentation and request validation.

4. [Stretch Goal] **Serialization framework** - Pluggable serialization 
   supporting common 
formats (JSON, Avro, custom) so functions can accept and return data appropriate
to their deployment context.

### Deliverables

- Core `@function` decorator adapted for open source Metaflow
- Function packaging and export to portable formats (local filesystem, S3)
- Ray backend with configurable resource allocation
- FastAPI backend with automatic OpenAPI schema generation
- Documentation and end-to-end examples
- Test suite

### Skills Required

- Python (intermediate/advanced)
- Ray
- FastAPI

### Links

- [Metaflow Functions Talk (InfoQ)](https://www.infoq.com/presentations/ml-netflix/)
- [Existing Implementation](https://github.com/Netflix/metaflow_rc/tree/master/nflx-metaflow-function)
- [Metaflow Documentation](https://docs.metaflow.org)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)

---

## Metaflow CI/CD: Kubernetes Integration Testing with GitHub Actions

**Difficulty:** Easy

**Duration:** 175 hours (Medium project)

**Technologies:** Python, GitHub Actions, Kubernetes, Argo Workflows, pytest

**Mentors:** Savin Goyal, Romain Cledat

### Description

Metaflow's test suite currently runs primarily against local execution 
backends. However, production Metaflow deployments typically use Kubernetes with 
Argo Workflows for orchestration. This gap means integration issues are often 
discovered late in the development cycle.

The [Metaflow Dev Stack](https://docs.metaflow.org/getting-started/devstack) 
provides a lightweight local Kubernetes environment with Argo Workflows 
pre-configured. This project aims to integrate the dev stack into Metaflow's 
GitHub Actions CI/CD pipeline, enabling automated integration tests against a 
real Kubernetes environment on every PR.

Tests should be executed using Metaflow's [Runner](https://docs.metaflow.org/metaflow/managing-flows/runner) 
and [Deployer](https://docs.metaflow.org/metaflow/managing-flows/deployer) APIs, 
which provide programmatic control over flow execution and deployment. 
The [existing QA test suite](https://github.com/saikonen/metaflow-qa-tests) serves 
as a starting point for Kubernetes integration tests.

### Goals

1. **GitHub Actions workflow for Kubernetes testing** - Create a reusable 
workflow that spins up the Metaflow dev stack (Kind + Argo Workflows) and runs 
integration tests against it using Runner/Deployer.

2. **Test result aggregation** - Build a pytest plugin or post-processing step 
that collects results from multiple test runs (local, Kubernetes, etc.) and 
generates a unified summary with links to failed test logs.

3. **PR status reporting** - Integrate with GitHub's check runs API to provide 
clear pass/fail status with expandable details showing which tests failed on 
which backend.

4. **Selective test execution** - Implement test markers and configuration to 
run specific tests against the dev stack, keeping CI times reasonable.

### Deliverables

- GitHub Actions workflow using Metaflow dev stack for Kubernetes integration tests
- Pytest plugin for multi-backend result aggregation
- GitHub check run integration with formatted test summaries
- Documentation for contributors on running Kubernetes tests locally
- Test markers for backend-specific test selection

### Skills Required

- Python (intermediate)
- GitHub Actions
- Kubernetes basics
- pytest

### Links

- [Metaflow Dev Stack](https://docs.metaflow.org/getting-started/devstack)
- [Scheduling with Argo Workflows](https://docs.metaflow.org/production/scheduling-metaflow-flows/scheduling-with-argo-workflows)
- [Runner API](https://docs.metaflow.org/metaflow/managing-flows/runner)
- [Deployer API](https://docs.metaflow.org/metaflow/managing-flows/deployer)
- [Metaflow QA Tests](https://github.com/saikonen/metaflow-qa-tests)
- [Metaflow Documentation](https://docs.metaflow.org)

---

## Metaflow VS Code Extension

**Difficulty:** Medium

**Duration:** 350 hours (Large project)

**Technologies:** TypeScript, VS Code Extension API, Python, Metaflow

**Mentors:** TBD

### Description

Developers spend most of their time in IDEs, yet Metaflow's IDE support is
minimal. The existing
[metaflow-dev-vscode](https://github.com/outerbounds/metaflow-dev-vscode)
extension provides only two keyboard shortcuts (run flow, spin step) with no
visual tooling. Setting up debugging requires
[manual launch.json configuration](https://github.com/outerbounds/vscode-debug-metaflow).
There is no way to visualize flow structure, browse artifacts, or monitor
runs without leaving the editor.

Competing workflow tools like Prefect and Dagster offer richer IDE integrations
and web UIs that provide immediate visual feedback. This gap makes Metaflow
feel less approachable to new users who expect modern developer tooling.

This project aims to build a full-featured VS Code extension that brings
Metaflow's core capabilities directly into the editor: visualize DAGs, browse
historical runs and artifacts, debug steps with one click, and configure
run parameters through a GUI.

### Goals

1. **Visual DAG viewer** - Render flow structure as an interactive graph in a
VS Code webview panel, updated live as the user edits their flow code.

2. **Artifact browser** - Tree view sidebar showing past runs organized by
flow/run/step/task, with the ability to inspect artifact values inline.

3. **One-click debugging** - Automatically generate debug configurations for
any step; set breakpoints and step through code without manual setup.

4. **Run configuration UI** - GUI panel to set flow parameters, choose compute
backend (local, Kubernetes, AWS Batch), and launch runs.

5. [Stretch Goal] **Inline card preview** - Render Metaflow cards directly in
the editor without spinning up a local server.

### Deliverables

- VS Code extension published to the marketplace
- DAG visualization panel with live updates
- Artifact browser sidebar with run history
- Debug configuration generator
- Run launcher with parameter and backend selection
- Documentation and demo video
- Test suite

### Skills Required

- TypeScript (intermediate)
- VS Code Extension API
- Python (intermediate)
- Basic understanding of DAG visualization (e.g., D3.js, Mermaid)

### Links

- [VS Code Extension API](https://code.visualstudio.com/api)
- [Existing Metaflow VS Code Extension](https://github.com/outerbounds/metaflow-dev-vscode)
- [VS Code Debug Configuration](https://github.com/outerbounds/vscode-debug-metaflow)
- [Metaflow Client API](https://docs.metaflow.org/api/client)
- [Metaflow Documentation](https://docs.metaflow.org)

---

## Jupyter-Native Metaflow

**Difficulty:** Medium

**Duration:** 350 hours (Large project)

**Technologies:** Python, Jupyter, ipywidgets, Metaflow

**Mentors:** TBD

### Description

Data scientists prototype in Jupyter notebooks, but Metaflow flows must be
defined in Python files. While Metaflow 2.12 introduced
[NBRunner](https://docs.metaflow.org/metaflow/managing-flows/notebook-runs)
for executing flows from notebooks, significant friction remains:

- The entire flow definition must fit in a **single cell**
- There is no way to define steps across multiple cells like normal notebook
  development
- Inspecting artifacts requires using the Client API with run IDs—no inline
  preview
- Converting notebook experiments into production flows requires manual
  rewriting

Tools like [Kale](https://github.com/kubeflow-kale/kale) for Kubeflow
demonstrated that cell-tagging approaches can bridge notebooks and pipelines.
This project brings similar capabilities to Metaflow: define steps naturally
across cells, visualize the DAG inline, and convert notebooks to flows
automatically.

### Goals

1. **Multi-cell flow definition** - Allow steps to be defined across multiple
notebook cells using cell tags or magic commands (e.g., `%%step train`).

2. **Notebook-to-flow converter** - Generate a standalone `.py` flow file from
a tagged notebook, suitable for production deployment.

3. **Inline artifact visualization** - Jupyter magic (e.g.,
`%mf_show self.model`) that renders artifacts (DataFrames, plots, models)
directly in notebook output.

4. **DAG widget** - ipywidget showing the flow graph with step status,
rendered inline in notebook cells.

5. [Stretch Goal] **Step-by-step execution** - Run individual steps
interactively, inspect artifacts, then continue to the next step (not the
entire DAG at once).

### Deliverables

- Jupyter extension/plugin with cell tagging support
- `%%step` magic command for defining steps in cells
- Notebook-to-flow export CLI command
- `%mf_show` magic for inline artifact rendering
- Interactive DAG widget (ipywidgets)
- Documentation with example notebooks
- Test suite

### Skills Required

- Python (intermediate/advanced)
- Jupyter extension development
- ipywidgets
- Familiarity with Metaflow flows

### Links

- [Running Flows in Notebooks](https://docs.metaflow.org/metaflow/managing-flows/notebook-runs)
- [Metaflow Card Notebook](https://github.com/outerbounds/metaflow-card-notebook)
- [Kale (Kubeflow Notebook-to-Pipeline)](https://github.com/kubeflow-kale/kale)
- [ipywidgets Documentation](https://ipywidgets.readthedocs.io/)
- [Metaflow Documentation](https://docs.metaflow.org)

---

## Metaflow UI 2.0: Modern Visualization and Standalone Mode

**Difficulty:** Medium/Advanced

**Duration:** 350 hours (Large project)

**Technologies:** TypeScript, React, Python, Metaflow

**Mentors:** TBD

### Description

The current [Metaflow UI](https://github.com/Netflix/metaflow-ui) provides
basic run monitoring but has significant limitations compared to competing
tools like Dagster and Prefect:

- **Requires Metaflow Service** - Cannot view local runs without deploying
  backend infrastructure
- **Static DAG visualization** - No live updates as steps execute
  ([requested](https://github.com/Netflix/metaflow-ui/issues/89))
- **No run comparison** - Cannot diff parameters, artifacts, or metrics
  between runs
- **No dark mode** - A common
  [user request](https://github.com/Netflix/metaflow-ui/issues/157)
- **Complex deployment** - Docker/nginx setup has
  [multiple](https://github.com/Netflix/metaflow-ui/issues/123)
  [reported](https://github.com/Netflix/metaflow-ui/issues/144) issues

Dagster's asset-centric lineage visualization and Prefect's polished
developer experience set user expectations that Metaflow's UI currently
does not meet. This project modernizes the Metaflow UI with standalone
local support, live DAG visualization, run comparison, and improved
developer experience.

### Goals

1. **Standalone local mode** - View runs from the local Metaflow datastore
without requiring Metaflow Service. Single command to launch
(e.g., `metaflow ui`).

2. **Live DAG visualization** - Steps light up in real-time as they execute,
with streaming log output and progress indicators.

3. **Run comparison view** - Side-by-side diff of two runs showing parameter
changes, artifact differences, and metric deltas.

4. **Dark mode and theming** - User-selectable themes with dark mode as a
first-class option.

5. [Stretch Goal] **Artifact lineage graph** - Visualize how artifacts flow
through the DAG across steps and runs.

### Deliverables

- Standalone UI that reads from local Metaflow datastore
- Live-updating DAG visualization with step status
- Run comparison/diff interface
- Dark mode theme
- Simplified one-command local deployment
- Documentation and migration guide from existing UI
- Test suite (Cypress)

### Skills Required

- TypeScript/React (intermediate/advanced)
- Python (intermediate)
- Data visualization (D3.js or similar)
- Understanding of Metaflow's datastore structure

### Links

- [Metaflow UI Repository](https://github.com/Netflix/metaflow-ui)
- [Metaflow UI Open Issues](https://github.com/Netflix/metaflow-ui/issues)
- [Metaflow Client API](https://docs.metaflow.org/api/client)
- [Dagster UI](https://dagster.io/) (reference for asset lineage)
- [Metaflow Documentation](https://docs.metaflow.org)

---

## Sandboxed Execution Environments with Devcontainers

**Difficulty:** Medium

**Duration:** 175 hours (Medium project)

**Technologies:** Python, Docker, Devcontainer Spec, Metaflow

**Mentors:** TBD

### Description

Metaflow steps can run in containers via `@kubernetes` or `@batch`, but these
require cloud infrastructure. For local development and CI environments,
there is no built-in way to run steps in isolated, reproducible sandboxes
without full container orchestration.

The [Development Container](https://containers.dev/) specification (used by
VS Code, GitHub Codespaces, and tools like
[DevPod](https://github.com/loft-sh/devpod) and
[Daytona](https://github.com/daytonaio/daytona)) provides a standardized way
to define reproducible development environments. These tools can run locally
with just Docker—no cloud account required.

This project adds a `@devcontainer` decorator that executes Metaflow steps
inside devcontainer-based sandboxes. This enables reproducible local execution,
safe execution of untrusted code, and a bridge between local development and
cloud deployment.

### Goals

1. **`@devcontainer` decorator** - Execute steps inside a devcontainer
environment, with support for `devcontainer.json` configuration files.

2. **Automatic environment capture** - Generate a `devcontainer.json` from
the current step's `@pypi`/`@conda` dependencies.

3. **Local Docker backend** - Run sandboxed steps on the local machine using
Docker, with no external services required.

4. **DevPod/Daytona integration** - Optional backends for users who have
these tools installed, enabling remote sandbox execution.

5. [Stretch Goal] **Sandbox security policies** - Configure network isolation,
filesystem restrictions, and resource limits for sandboxed execution.

### Deliverables

- `@devcontainer` decorator implementation
- Devcontainer.json generator from Metaflow environment specs
- Local Docker execution backend
- Optional DevPod/Daytona backend plugins
- Documentation with examples
- Test suite

### Skills Required

- Python (intermediate)
- Docker
- Familiarity with devcontainer specification
- Basic understanding of Metaflow decorators

### Links

- [Development Container Specification](https://containers.dev/)
- [DevPod](https://github.com/loft-sh/devpod)
- [Daytona](https://github.com/daytonaio/daytona)
- [Metaflow Decorators](https://docs.metaflow.org/api/decorators)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)

---

## Worker Pools for Efficient Foreach Execution

**Difficulty:** Medium/Advanced

**Duration:** 350 hours (Large project)

**Technologies:** Python, Metaflow, Kubernetes, Redis (optional)

**Mentors:** TBD

### Description

Metaflow's `foreach` construct creates one container per item, which works well
when per-item computation significantly exceeds container startup time. However,
for short-lived tasks (seconds rather than minutes), container overhead
dominates:

| Component | Typical Time |
|-----------|-------------|
| Kubernetes pod scheduling | 5-15 seconds |
| Container image pull | 10-60 seconds |
| Python interpreter + imports | 5-20 seconds |
| **Actual task work** | **1-5 seconds** |

For a foreach over 10,000 items with 5-second tasks, the current model spends
more time on container overhead than actual computation—even with `--max-workers`
limiting concurrency.

This project introduces a **worker pool** execution model: instead of N
containers for N items, spin up a fixed pool of persistent workers that
each process multiple items from a shared queue. This amortizes container
startup cost across many items and provides natural load balancing (faster
workers grab more items).

This pattern is proven in systems like [Celery](https://docs.celeryq.dev/),
[Ray actors](https://docs.ray.io/en/latest/ray-core/actors.html), and
[Dask distributed](https://distributed.dask.org/), but is not currently
available in Metaflow.

### Goals

1. **`@worker_pool` decorator** - Mark a foreach step to use pooled execution,
specifying pool size and optional configuration.

```python
@worker_pool(size=10)
@kubernetes(cpu=2, memory=4096)
@step
def process(self):
    # Each worker processes multiple items from the pool
    result = compute(self.input)
    self.output = result
    self.next(self.join)
```

2. **Local backend implementation** - Worker pool execution using Python
multiprocessing, enabling development and testing without cloud infrastructure.

3. **Kubernetes backend implementation** - Workers as long-running pods that
pull items from a coordination mechanism and write per-item artifacts.

4. **Artifact compatibility** - Ensure the join step receives artifacts from
all items, maintaining compatibility with existing foreach/join patterns.

5. [Stretch Goal] **Queue-based work distribution** - Replace simple
pre-partitioning with a Redis or in-memory queue for dynamic load balancing
and better handling of variable-duration items.

### Deliverables

- `@worker_pool` decorator implementation
- Local backend using multiprocessing pool
- Kubernetes backend with persistent worker pods
- Coordination mechanism for work distribution (pre-partitioned MVP,
  queue-based stretch)
- Per-item artifact storage compatible with join steps
- Documentation with performance benchmarks
- Test suite covering failure scenarios

### Skills Required

- Python (intermediate/advanced)
- Metaflow internals (decorators, runtime)
- Kubernetes basics
- Distributed systems concepts (work queues, coordination)

### Links

- [Metaflow Foreach Documentation](https://docs.metaflow.org/metaflow/basics#foreach)
- [Controlling Parallelism](https://docs.metaflow.org/scaling/remote-tasks/controlling-parallelism)
- [Celery Worker Pools](https://docs.celeryq.dev/en/stable/userguide/workers.html)
- [Ray Actor Pool](https://docs.ray.io/en/latest/ray-core/actors.html)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)

---

## Confidential Computing with Trusted Execution Environments

**Difficulty:** Advanced

**Duration:** 350 hours (Large project)

**Technologies:** Python, Gramine/SGX, Phala Cloud, Metaflow

**Mentors:** TBD

### Description

Machine learning workflows often process sensitive data: medical records,
financial transactions, proprietary models. Traditional isolation (containers,
VMs) protects against external attackers but not against the infrastructure
operator. Trusted Execution Environments (TEEs) provide hardware-level
isolation where even the cloud provider cannot access the computation.

TEE adoption has historically been difficult due to complex tooling, but
platforms like [Gramine](https://gramine.readthedocs.io/) (open source,
runs locally in simulation mode) and
[Phala Cloud](https://phala.com/) (managed TEE infrastructure with free
credits for developers) have made confidential computing more accessible.

This project adds a `@confidential` decorator that executes Metaflow steps
inside TEEs. Development and testing use Gramine's simulation mode locally;
production deployment targets Phala Cloud or other TEE providers.

### Goals

1. **`@confidential` decorator** - Mark steps for execution inside a TEE
with attestation verification.

2. **Gramine backend for local development** - Run steps in Gramine-SGX
simulation mode, allowing development and testing without TEE hardware.

3. **Phala Cloud backend for production** - Deploy confidential steps to
Phala's managed TEE infrastructure.

4. **Attestation verification** - Verify TEE attestation reports before
trusting computation results.

5. [Stretch Goal] **Encrypted artifact storage** - Encrypt artifacts at rest
with keys sealed to the TEE, ensuring only attested enclaves can decrypt them.

### Deliverables

- `@confidential` decorator with pluggable backend architecture
- Gramine simulation backend for local testing
- Phala Cloud backend with deployment automation
- Attestation verification utilities
- Documentation covering threat model and security properties
- Test suite (simulation mode)
- Example flow demonstrating confidential ML inference

### Skills Required

- Python (intermediate/advanced)
- Basic understanding of TEE concepts (SGX, attestation)
- Docker/containerization
- Familiarity with Metaflow decorators

### Links

- [Gramine Documentation](https://gramine.readthedocs.io/)
- [Phala Cloud](https://phala.com/)
- [Phala Cloud Pricing](https://phala.com/pricing) ($20 free credits)
- [Intel SGX Overview](https://www.intel.com/content/www/us/en/architecture-and-technology/software-guard-extensions.html)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)
- [Confidential Computing Consortium](https://confidentialcomputing.io/)

---

### Metaflow Functions Expected API

#### 1. Creating a Function

Define a function using the `@json_function` decorator:

```python
from metaflow import json_function, FunctionParameters

@json_function
def predict(data: dict, params: FunctionParameters) -> dict:
    """Run inference using the bound model."""
    features = [data[f] for f in params.feature_names]
    prediction = params.model.predict([features])[0]
    return {"prediction": int(prediction)}
```

The function receives:
- `data`: JSON-serializable input (dict, list, str, etc.)
- `params`: Access to artifacts from the bound task

#### 2. Binding to a Task

Bind the function to a completed task to capture its environment and artifacts:

```python
from metaflow import JsonFunction, Task

task = Task("MyTrainFlow/123/train/456")
inference_fn = JsonFunction(predict, task=task)

# Export portable reference
reference = inference_fn.reference
```

#### 3. Deploying with Ray (Batch Inference)

```python
from metaflow import function_from_json

fn = function_from_json(reference, backend="ray")
results = [fn(record) for record in batch_data]
```

#### 4. Deploying with FastAPI (Real-time Serving)

```python
from fastapi import FastAPI
from metaflow import function_from_json

app = FastAPI()
fn = function_from_json(reference)

@app.post("/predict")
def predict(payload: dict):
    return fn(payload)
```
