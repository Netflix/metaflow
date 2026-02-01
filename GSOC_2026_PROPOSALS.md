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

See [Expected API](#expected-api) below for code examples.

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

### Why This Matters

**For users:**
- **Eliminate the training-serving gap** - Deploy models with the exact same
  environment used during training, eliminating "works in training, breaks in
  production" issues
- **Simplify ML deployment** - No need to manually recreate environments or
  manage dependency versions across teams
- **Flexible deployment targets** - Same function works for batch inference
  (Ray) and real-time serving (FastAPI) without code changes

**For the contributor:**
- Work on a production-proven system used at Netflix scale
- Gain deep experience with ML deployment patterns and challenges
- Build a portfolio piece that demonstrates end-to-end ML engineering skills
- Learn Ray for distributed computing and FastAPI for API development
- Contribute to a widely-used open source project

### Skills Required

- Python (intermediate/advanced)
- Ray
- FastAPI

### Links

- [Metaflow Functions Talk (InfoQ)](https://www.infoq.com/presentations/ml-netflix/)
- [Existing Implementation](https://github.com/Netflix/metaflow_rc/tree/master/nflx-metaflow-function)
- [Metaflow Documentation](https://docs.metaflow.org)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)

### Expected API

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

### Why This Matters

**For users:**
- **Catch integration bugs early** - Issues with Kubernetes/Argo are discovered
  in CI, not after merging to main
- **Confidence in contributions** - Contributors can verify their changes work
  on production-like infrastructure before submitting PRs
- **Faster release cycles** - Automated testing reduces manual QA burden and
  enables more frequent releases

**For the contributor:**
- Learn modern CI/CD practices with GitHub Actions
- Gain hands-on Kubernetes experience in a real-world context
- Understand how large open source projects maintain quality at scale
- Build expertise in test infrastructure—a highly valued skill
- Great entry point for becoming a long-term Metaflow contributor

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

### Why This Matters

**For users:**
- **Stay in flow state** - No context switching between editor and browser to
  monitor runs or inspect artifacts
- **Faster debugging** - One-click debugging eliminates manual configuration
  that trips up new users
- **Lower barrier to entry** - Visual DAG and artifact browser make Metaflow
  more approachable for newcomers
- **Competitive parity** - Brings Metaflow's IDE experience up to par with
  Prefect and Dagster

**For the contributor:**
- Build a widely-used developer tool from scratch
- Learn the VS Code Extension API—a valuable skill for any developer tools role
- Gain experience with TypeScript and modern frontend development
- Create a highly visible portfolio piece (published to VS Code marketplace)
- Understand workflow orchestration systems from a tooling perspective

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

### Why This Matters

**For users:**
- **Natural notebook workflow** - Define flows the same way you write notebooks,
  not crammed into a single cell
- **Seamless prototyping-to-production** - Convert notebook experiments to
  production flows with one command
- **Inline feedback** - See DAG structure and artifact values without leaving
  the notebook
- **Lower friction** - Data scientists can adopt Metaflow without changing
  their preferred development style

**For the contributor:**
- Deep dive into Jupyter's extension architecture
- Learn how notebook-to-pipeline tools work (applicable to Kubeflow, Airflow,
  etc.)
- Build interactive widgets with ipywidgets
- Understand the data science workflow and tooling ecosystem
- Create a tool that directly impacts data scientists' daily experience

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

### Why This Matters

**For users:**
- **Zero-infrastructure local UI** - View and debug local runs without deploying
  any backend services
- **Real-time visibility** - Watch flows execute live instead of refreshing
  static pages
- **Debug faster** - Compare runs side-by-side to identify what changed when
  something breaks
- **Modern developer experience** - Dark mode and polished UX that meets 2025
  expectations

**For the contributor:**
- Work on a full-stack application (React frontend + Python backend)
- Learn real-time data visualization techniques
- Gain experience with the Metaflow datastore and client APIs
- Build a standalone product that can be demoed and showcased
- Opportunity to improve UX for thousands of Metaflow users

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

### Why This Matters

**For users:**
- **Reproducible local execution** - Run steps in isolated containers locally,
  matching production behavior
- **Safe code execution** - Sandbox untrusted or experimental code without
  risking host system
- **Smooth local-to-cloud transition** - Same container spec works locally
  and on Kubernetes
- **CI-friendly** - Run integration tests in isolated environments without
  cloud costs

**For the contributor:**
- Learn the devcontainer specification used by VS Code, Codespaces, and modern
  dev tools
- Understand container isolation and security at a practical level
- Gain experience writing Metaflow decorators (extensible pattern used
  throughout the ecosystem)
- Work with Docker APIs and container lifecycle management
- Build a feature that bridges local development and production deployment

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

**Technologies:** Python, Metaflow, Kubernetes/Argo Workflows

**Mentors:** TBD

### Description

Metaflow's `foreach` construct creates one task per item. For local execution,
this means lightweight subprocesses with minimal overhead. However, when running
on **Kubernetes** (via Argo Workflows) or **AWS Batch**, each task becomes a
separate container, and startup overhead dominates for short-lived tasks:

| Component | Typical Time |
|-----------|-------------|
| Pod/container scheduling | 5-15 seconds |
| Container image pull (cached) | 5-30 seconds |
| Python interpreter + imports | 5-20 seconds |
| **Actual task work** | **1-5 seconds** |

For a foreach over 10,000 items with 5-second tasks, the current model spends
more time on container overhead than actual computation—even with `--max-workers`
limiting concurrency. Users must either batch items manually within each task
or accept the overhead.

This project introduces a **worker pool** execution model for remote backends:
instead of N containers for N items, spin up a fixed pool of persistent
worker pods that each process multiple items. This amortizes container startup
cost across many items and provides natural load balancing (faster workers
process more items).

```python
class MyFlow(FlowSpec):

    @step
    def start(self):
        self.items = range(10000)  # 10k items to process
        self.next(self.process, foreach='items')

    @worker_pool(size=10, max_items_per_worker=1000)
    @kubernetes(cpu=2, memory=4096)
    @step
    def process(self):
        # Called once per item, but workers are reused
        self.result = expensive_compute(self.input)
        self.next(self.join)

    @step
    def join(self, inputs):
        # Receives all 10k results, same as regular foreach
        self.results = [inp.result for inp in inputs]
        self.next(self.end)
```

Instead of 10,000 pod launches, only 10 persistent workers are created. Each
worker processes ~1,000 items sequentially, writing per-item artifacts that
the join step consumes normally.

This pattern is proven in systems like [Celery](https://docs.celeryq.dev/),
[Ray actors](https://docs.ray.io/en/latest/ray-core/actors.html), and
[Dask distributed](https://distributed.dask.org/), but is not currently
available in Metaflow for remote execution.

### Goals

1. **`@worker_pool` decorator** - Mark a foreach step to use pooled execution
with configurable pool size and optional per-worker item limits (to bound
memory accumulation).

2. **Kubernetes/Argo backend** - Workers as long-running pods that process
item batches and write per-item artifacts to the Metaflow datastore.

3. **Artifact compatibility** - Each item produces its own task artifacts,
ensuring the join step works identically to regular foreach (no user code
changes required in join).

4. **Failure handling** - Define clear semantics: if a worker crashes, its
in-progress item fails and can be retried. Completed items are durable.
Integration with `@retry` decorator for per-item retries.

5. **Local backend for testing** - Worker pool execution using Python
multiprocessing, enabling development and testing without cloud infrastructure.

6. [Stretch Goal] **Dynamic work distribution** - Replace static
pre-partitioning with a queue-based approach for better load balancing when
item processing times vary significantly.

7. [Stretch Goal] **AWS Batch backend** - Extend worker pool support to
AWS Batch, potentially leveraging Batch array jobs.

### Design Considerations

The project should address these trade-offs:

- **Pool sizing**: Too few workers = underutilization; too many = diminishing
  returns. Consider auto-sizing based on item count.
- **Memory management**: Long-running workers may accumulate memory.
  `max_items_per_worker` allows recycling workers periodically.
- **Observability**: Metaflow UI shows per-task status. With worker pools,
  need to show per-item progress within each worker.
- **Cost trade-off**: Idle workers still cost money. Document when worker
  pools help vs. hurt.

### Deliverables

- `@worker_pool` decorator implementation
- Kubernetes/Argo backend with persistent worker pods
- Local backend using multiprocessing (for development/testing)
- Per-item artifact storage compatible with join steps
- Failure handling with `@retry` integration
- Documentation with performance benchmarks and sizing guidance
- Test suite covering normal operation, failures, and retries

### Why This Matters

**For users:**
- **10-100x faster foreach for short tasks** - Eliminate container startup
  overhead that dominates short-lived computations
- **Lower cloud costs** - Fewer container launches means less scheduling
  overhead and potentially lower bills
- **No code changes to existing flows** - Just add `@worker_pool` decorator;
  join steps work unchanged
- **Natural load balancing** - Faster workers automatically process more items

**For the contributor:**
- Deep dive into Metaflow's runtime and execution model
- Learn distributed systems patterns (work queues, coordination, failure
  handling) through hands-on implementation
- Gain Kubernetes experience with long-running pods and job management
- Understand the trade-offs in distributed task execution (Celery, Ray, Dask)
- Work on a performance-critical feature with measurable impact

### Skills Required

- Python (intermediate/advanced)
- Metaflow internals (decorators, runtime, datastore)
- Kubernetes basics
- Distributed systems concepts (work distribution, failure handling)

### Links

- [Metaflow Foreach Documentation](https://docs.metaflow.org/metaflow/basics#foreach)
- [Controlling Parallelism](https://docs.metaflow.org/scaling/remote-tasks/controlling-parallelism)
- [Scheduling with Argo Workflows](https://docs.metaflow.org/production/scheduling-metaflow-flows/scheduling-with-argo-workflows)
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

### Why This Matters

**For users:**
- **Process sensitive data safely** - Run ML on medical, financial, or
  proprietary data with hardware-level protection
- **Zero-trust infrastructure** - Even cloud providers cannot access your
  computation or data
- **Compliance enablement** - Meet regulatory requirements (HIPAA, GDPR) for
  data processing
- **Verifiable computation** - Attestation proves code ran in a secure enclave
  without tampering

**For the contributor:**
- Learn cutting-edge confidential computing technology (TEEs, SGX, attestation)
- Understand security at the hardware level—a rare and valuable skill
- Work with emerging cloud infrastructure (confidential VMs are becoming
  mainstream)
- Build expertise applicable to blockchain, secure enclaves, and privacy tech
- Contribute to an increasingly important area as AI privacy concerns grow

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

## Metaflow Nomad Integration

**Difficulty:** Medium

**Duration:** 350 hours (Large project)

**Technologies:** Python, HashiCorp Nomad, Metaflow

**Mentors:** Madhur Tandon

### Description

Metaflow supports various compute backends for executing steps remotely: `@kubernetes`, `@batch` (AWS Batch), and community extensions like [`@slurm`](https://github.com/outerbounds/metaflow-slurm) for HPC clusters. However, many organizations use [HashiCorp Nomad](https://www.nomadproject.io/) as their workload orchestrator — a lightweight alternative to Kubernetes that's simpler to operate and supports diverse workload types (containers, VMs, binaries).

Nomad is particularly popular in organizations already using HashiCorp's stack (Vault, Consul) and in edge computing scenarios where Kubernetes' complexity is overkill. Despite this, there's currently no way to run Metaflow steps on Nomad clusters.

This project aims to implement a `@nomad` decorator that executes Metaflow steps as Nomad jobs, bringing Metaflow's workflow capabilities to the Nomad ecosystem. The [`@slurm` extension](https://github.com/outerbounds/metaflow-slurm) provides a reference implementation for integrating custom compute backends.

### Goals

1. **`@nomad` decorator** - Execute Metaflow steps as Nomad batch jobs with basic resource configuration (CPU, memory).

2. **Docker task driver support** - Run steps in Docker containers, similar to how `@kubernetes` and `@batch` work.

3. **Job submission and monitoring** - Submit jobs to Nomad, poll for completion, and retrieve exit codes.

4. **Log streaming** - Capture and display stdout/stderr from Nomad allocations in the Metaflow CLI.

5. **Basic retry support** - Integrate with Metaflow's `@retry` decorator to resubmit failed jobs.

6. [Stretch Goal] **Exec driver support** - Support Nomad's exec driver for running binaries directly without containers.

7. [Stretch Goal] **GPU resource allocation** - Support GPU constraints using Nomad's device plugins.

### Deliverables

- `@nomad` decorator implementation following Metaflow extension patterns
- Nomad job submission and monitoring backend
- Docker task driver support
- Basic resource configuration (CPU, memory)
- Log streaming from Nomad allocations
- Documentation with setup guide and basic examples
- Test scenarios covering job submission, execution, and failures
- Example flows demonstrating Docker-based execution

### Why This Matters

**For users:**
- **Use existing Nomad infrastructure** - Leverage Nomad clusters without needing Kubernetes or cloud batch services
- **Simpler operations** - Nomad's lightweight architecture reduces operational complexity compared to Kubernetes
- **HashiCorp ecosystem integration** - Natural fit for teams already using Vault, Consul, or Terraform
- **Edge and hybrid deployments** - Run ML workflows on edge infrastructure where Kubernetes is too heavy

**For the contributor:**
- Learn HashiCorp Nomad—increasingly popular in the infrastructure space
- Understand how to extend Metaflow with custom compute backends (applicable to other schedulers)
- Gain experience with job orchestration, lifecycle management, and failure handling
- Work with a real-world reference implementation (`@slurm`) as a guide
- Build a foundation that the community can enhance with advanced features later

### Skills Required

- Python (intermediate)
- Basic familiarity with HashiCorp Nomad
- Docker
- Understanding of Metaflow decorators (or willingness to learn)

### Links

- [HashiCorp Nomad Documentation](https://www.nomadproject.io/docs)
- [Nomad Jobs API](https://developer.hashicorp.com/nomad/api-docs/jobs)
- [Metaflow Slurm Extension (Reference)](https://github.com/outerbounds/metaflow-slurm)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)
- [Metaflow Step Decorators](https://docs.metaflow.org/api/step-decorators)
- [Metaflow Documentation](https://docs.metaflow.org)

---
