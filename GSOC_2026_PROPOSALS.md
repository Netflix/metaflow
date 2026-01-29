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
