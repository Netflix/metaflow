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
[presented publicly](https://www.infoq.com/presentations/ml-netflix/).

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
- Gain experience with TypeScript and modern frontend development
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
- Work with emerging cloud infrastructure (confidential VMs are becoming
  mainstream)
- Build expertise applicable to blockchain, secure enclaves, and privacy tech

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

## Metadata service request improvements

**Difficulty:** Easy

**Duration:** 175 hours (Medium project)

**Technologies:** Python, Docker, PostgreSQL

**Mentors:** Sakari Ikonen

### Description

The current metadata service for Metaflow does not provide paginated responses for its endpoints. Introducing pagination is required for some backfill-patterns that need to iterate over existing resources, in order to keep the resource requirements of these operations limited. Currently the payloads returned over the wire are not capped, and can be significant in size with more established deployments.

Resources can also be filtered by tags in the Metaflow client. This is currently still happening in-memory over the response payload, as the API does not support filtering. Being able to apply filters on the request level would also cut down on the resource use.

### Goals

1. Being able to return filtered, paginated responses from metadata-service

2. Backwards compatibility with older Metaflow clients that do not support pagination. Possibly by feature-gating via client version in request headers.

3. Handling paginated responses in Metaflow client

4. handling filtering by tag in Metaflow client on the request level, not in-memory.


---

## Metaflow-services eventing rework to a message broker architecture

**Difficulty**: Hard

**Duration**: 300 hours (Large project)

**Technologies**: Python, Docker, PostgreSQL, Language of choice (f.ex. Rust/Go)

**Mentors**: Sakari Ikonen

### Description

The current backend architecture relies heavily on PostgreSQL features for broadcasting and subscribing to database events (INSERT/UPDATE) in order to be able to provide real-time updates. This is a hard vendor-lock to PostgreSQL which is imposed by the architecture choice. The messaging mechanism in the database has proven to fall short in high-volume deployments more than once, so exploring alternatives to this is expected to be beneficial.

As all data insertion and updates are handled by the metadata-service, and currently the only service that is interested in the events is the ui_backend service, a simple message broker between these two services should be the most straightforward solution.

### Considerations

Some considerations for the implementation are
- The usual ui backend db is a replica. If the events come off a broker that receives its messages based on inserts on a main db, then there is no guarantee that the replica is up-to-date when the message gets processed. Therefore some retry logic needs to be introduced on top of the message handling
- The volume of messages is significant on large deployments, so performance of the broker is of utmost importance
- Messages need to have some guarantee of in-order arrival within certain scopes (flow level for runs, run level for tasks etc.)

### Goals

- Develop a PoC message broker service that metadata-service can publish messages to, and ui_backend can subscribe to topic in order to receive only messages of interest.
- Completely replace currently used LISTEN/NOTIFY mechanism in favour of message broker service.
- Being able to deploy ui service with a pure read-replica instead of a logical replica

---

## Agent-Friendly Metaflow Client: Analyzing and Addressing Client API Inefficiencies

**Difficulty:** Hard

**Duration:** 350 hours (Large project)

**Technologies:** Python, Metaflow Client API, Metaflow Metadata Service

**Mentors:** Valay Dave

### Description

AI coding agents (Cursor, Claude Code, Codex, etc.) are increasingly used to
author, execute, and debug Metaflow workflows. These agentic tools get a window into 
all current/past metaflow executions through the 
[Metaflow Client API](https://docs.metaflow.org/api/client).

The Client API is powerful but when agents use it programmatically at scale 
as a means for search then, several inefficiencies emerge that are not obvious 
from the API surface alone. These inefficiencies span two layers:

**At the Client API layer** (`metaflow.client`):
- Finding a failed task requires iterating `Run → Steps → Tasks` and
  checking `.successful` on each object. On runs with many parallel tasks
  (e.g., `foreach` over 1000 items), this triggers hundreds of individual
  metadata requests.
- `task.stdout` loads the entire log as a single string. For training steps
  that produce megabytes of output, this is wasteful when the agent only
  needs the last few lines or lines matching an error pattern.
- Filtering is limited to tags (`flow.runs("my_tag")` or `namespace("foo")`). 
  There is no way to filter by status, date range, or failure type without 
  iterating all runs and checking each one in memory.
- Time-based queries are not first-class. There is no efficient way to ask
  "show me runs from the last 24 hours" or "find tasks that ran between
  Tuesday and Wednesday." The `created_at` property exists on client objects,
  but using it requires fetching every run first and filtering in Python —
  the metadata service does not support time-range predicates on its
  endpoints.
- Searching across artifacts is expensive and unsupported. An agent asking
  "which run produced an artifact called `model` with size > 100MB?" or
  "find the task where `accuracy` was highest" must iterate runs, steps,
  and tasks, then inspect each artifact individually. There is no
  cross-run or cross-task artifact search capability — neither in the
  Client API nor the metadata service.

**At the metadata provider / service layer:**

The Client API fetches data through a
[metadata provider](https://github.com/Netflix/metaflow/blob/master/metaflow/plugins/metadata_providers/service.py)
(`ServiceMetadataProvider`), which translates client queries into HTTP
requests against the
[metadata service](https://github.com/Netflix/metaflow-service). The
provider's single query method (`_get_object_internal`) constructs REST
paths like `/flows/{id}/runs` and returns full, unfiltered JSON responses.
Several gaps exist at this layer:

- No pagination — collection endpoints (e.g., listing all runs for a
  flow) return unbounded responses that grow with deployment age. The
  provider's `_get_object_internal` issues bare GET requests with no
  `limit` or `offset` parameters.
- Limited server-side filtering — the provider does support server-side
  metadata filtering via `filter_tasks_by_metadata` (service >= 2.5.0),
  but tag-based filters from `_apply_filter` are applied client-side
  after the full response is returned. There is no server-side status
  or time-range filtering.
- Certain compound queries (e.g., "which tasks in this run failed?") have
  no direct endpoint, forcing the provider to make many individual
  requests.
- The mapping from Client API operations to HTTP requests is implicit,
  making it hard to reason about the true cost of a client call.

This project has two parts: **analysis** and **implementation**. The
contributor will first systematically map out how the Client API translates
to metadata service calls, identify the specific inefficiencies that arise
for common agent use cases, and then build a set of utility functions that
work around or address those inefficiencies.

### Goals

#### 1. Client API Efficiency Audit

Trace the common agent use cases (listed below) through the Client API
and metadata service, documenting exactly which HTTP requests each
operation triggers and where the performance bottlenecks are. The use
cases to analyze:

- Listing recent runs for a flow, filtered by success/failure status
- Listing runs/tasks for a flow, filtered based on time range
- Finding the failed task(s) in a run and retrieving error details
- Getting artifact metadata (names, sizes, types) for a task without
  loading artifact data
- Retrieving bounded/filtered log output for a task
- Searching for artifacts across runs and tasks (name/size/type/data
  in artifact etc.)

For each use case, the contributor should determine whether the current
metadata service already supports the query via its existing endpoints.
The implementation strategy depends on where the gap is:

- If the service supports it but the existing provider doesn't expose
  it efficiently → the extension implements a new metadata provider
  that inherits from `ServiceMetadataProvider` in the Metaflow
  codebase and adds or overrides methods to expose the capability.
  Utility functions are built on top of this extended provider.
- If the service doesn't support the query at all and a new endpoint
  is needed → add the endpoint to the
  [metadata service](https://github.com/Netflix/metaflow-service),
  and wire it through the extended provider in the extension.
- If the query can be answered client-side with bounded cost using
  existing endpoints → build utility functions directly, with
  explicit bounds and structured output.

#### 2. Metadata Service Gap Analysis

Review the
[metaflow-service](https://github.com/Netflix/metaflow-service) API
routes and identify what is missing or insufficient for efficient agent
queries. This includes examining:

- Which endpoints support pagination and which do not
- Whether status-based or time-range filtering is available server-side
- Whether there are endpoints that return lightweight summaries vs
  full objects
- Whether artifact-level queries (by name, size, type) are possible
  without loading artifact data
- What new endpoints or query parameters would eliminate the need for
  expensive client-side iteration

The output is a concrete list of gaps, and for each gap, a determination
of where the fix belongs: a new method on the extended provider
(inheriting from `ServiceMetadataProvider`), a new endpoint on the
metadata service, or a client-side utility with bounded iteration.

#### 3. Query Utilities via Extensions Package

Build a `metaflow-agent` extensions package containing an extended
metadata provider (inheriting from `ServiceMetadataProvider`) and
utility functions for the analyzed use cases. Some utilities will wrap
existing provider capabilities with bounds and structured output. Others
will use new methods on the extended provider, or new metadata service
endpoints identified in Goals 1 and 2. Target utilities:

- **Run listing with filters** — By status, tags, and time range,
  with bounded results
- **Run summary** — Structured overview of a run's status, steps,
  and failure info
- **Failure details** — Failed task(s) with error type, message,
  and traceback
- **Artifact search** — Find artifacts across runs/tasks by name,
  size threshold, or type, without unpickling data
- **Bounded log access** — Last N lines or pattern-matched lines
  from task logs

#### 4. [Stretch Goal] New Metadata Service Endpoints

For the highest-impact gaps that require server-side support (e.g.,
paginated listing, time-range filtering, failed-task queries), implement
the endpoints in the
[metadata service](https://github.com/Netflix/metaflow-service), add
corresponding methods to the extended provider, and demonstrate the
efficiency improvement over client-side workarounds.

### Deliverables

- **Audit and gap analysis document** — A combined report covering the
  Client API efficiency audit (Goal 1) and the metadata service gap
  analysis (Goal 2): which use cases are supported by existing endpoints,
  which require provider-level changes, and which need new service
  endpoints. For each utility built, documents the metadata service calls
  it makes and how it scales with run complexity.
- **An extensions package** — An extended metadata provider
  (inheriting from `ServiceMetadataProvider`) and utility functions for
  run listing/filtering, run summary, failure details, artifact search,
  and bounded log access.
- **Test suite** covering the utility functions and extended provider

### Why This Matters

**For users:**
- **Agents can debug flows without hammering the backend** — Today, naive
  agent use of the Client API can generate hundreds of metadata service
  requests for a single inspection task. Utilities designed with awareness
  of the backend cost prevent this.
- **Informs future Metaflow development** — The audit and gap analysis
  produce actionable insight for improving both the Client API and the
  metadata service, benefiting all users — not just agents.
- **Structured utilities for any programmatic use** — While motivated by
  agents, the utilities are useful for any programmatic Metaflow consumer:
  CI/CD pipelines, monitoring scripts, dashboards.

**For the contributor:**
- Gain deep understanding of the Metaflow Client API, metadata service
  architecture, and how they interact
- Learn to analyze and design APIs with performance constraints in mind
- Develop skills in systems-level profiling and efficiency analysis
- Build a practical tool at the intersection of AI agents and ML
  infrastructure

### Skills Required

- Python (intermediate)
- Ability to read and trace through library code (the Client API
  internals and metadata service routes)
- Understanding of REST APIs and database-backed services
- Familiarity with performance analysis (request counting, response
  size estimation)

### Links

- [Metaflow Client API](https://docs.metaflow.org/api/client)
- [Metaflow Metadata Service](https://github.com/Netflix/metaflow-service)
- [Metaflow Extensions Template](https://github.com/Netflix/metaflow-extensions-template)
- [Metaflow Documentation](https://docs.metaflow.org)
