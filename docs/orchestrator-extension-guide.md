# Metaflow Orchestrator Extension Guide

This guide documents everything learned building five orchestrator extensions for Metaflow (Kestra, Prefect, Temporal, Dagster, Flyte). Use it to build a correct, complete orchestrator implementation without repeating the same mistakes.

The guide is organized as a developer spec. Read every section before writing a single line of code. Several of the mistakes documented here consumed days of debugging that a five-minute read would have prevented.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [CLI Commands](#2-cli-commands)
3. [The Deployer Attribute File Protocol](#3-the-deployer-attribute-file-protocol)
4. [Environment Variables That MUST Be Passed to Step Subprocesses](#4-environment-variables-that-must-be-passed-to-step-subprocesses)
5. [Decorator Interactions](#5-decorator-interactions)
6. [run_params Serialization](#6-run_params-serialization)
7. [The from_deployment Contract](#7-the-from_deployment-contract)
8. [Testing Requirements](#8-testing-requirements)
9. [The Complete Test Checklist](#9-the-complete-test-checklist)
10. [GHA Workflow Template](#10-gha-workflow-template)
11. [Common Mistakes (Anti-patterns)](#11-common-mistakes-anti-patterns)

---

## 1. Architecture Overview

A Metaflow orchestrator extension is a Metaflow plugin that maps a Metaflow `FlowGraph` to the concepts of an external scheduler. The key interfaces are:

- A **CLI group** (e.g., `python myflow.py kestra`) with subcommands (`create`, `trigger`, `resume`, etc.)
- A **DeployerImpl** subclass that wires the CLI group into the Metaflow Deployer API
- A **DeployedFlow** subclass that holds the deployed workflow handle
- A **TriggeredRun** subclass that wraps a single scheduler execution and resolves it to a Metaflow `Run`

The complete call chain when the test framework deploys a flow is:

```
Deployer(flow_file, **tl_kwargs).my_scheduler(**scheduler_kwargs).create(**deploy_kwargs)
  -> DeployerImpl.__init__                 # builds MetaflowAPI, resolves env
  -> DeployerImpl._create(DeployedFlowClass, **kwargs)
       -> runs: python myflow.py <top_level_kwargs> my_scheduler create \
                --deployer-attribute-file <fifo> <deploy_kwargs>
       -> reads JSON from the FIFO file
       -> returns DeployedFlow(deployer=self)
```

And when a test triggers a run:

```
deployed_flow.trigger(**run_kwargs)
  -> builds command: python myflow.py ... my_scheduler trigger \
                     --deployer-attribute-file <fifo> [--run-param k=v ...]
  -> reads JSON from the FIFO file
  -> returns TriggeredRun(deployer=self, content=json_string)
```

---

## 2. CLI Commands

Every orchestrator must implement these subcommands under its CLI group. The exact names must match the deployer TYPE (see Section 3).

### Required Commands

| Command | Purpose |
|---------|---------|
| `create` | Compile and deploy the flow to the scheduler |
| `trigger` | Trigger a new run of a previously deployed flow |
| `resume` | Re-run a failed flow, skipping already-completed steps |

### Recommended Commands

| Command | Purpose |
|---------|---------|
| `compile` | Compile to scheduler format without deploying (useful for debugging) |
| `run` | Compile, deploy, trigger, and wait (convenience shortcut) |

### Common Options Every Command Must Accept

**`create`:**

```
--tag TAG                  (multiple=True) Metaflow tags for all runs
--namespace TEXT           Metaflow namespace
--with TEXT                (multiple=True) Step decorator injection (e.g. --with=kubernetes)
--branch TEXT              @project branch name
--production               Flag: use the @project production branch
--workflow-timeout INT     Wall-clock timeout in seconds
--deployer-attribute-file  Hidden: write deployment JSON to this path
```

**`trigger`:**

```
--run-param TEXT           (multiple=True) "key=value" flow parameters
--deployer-attribute-file  Hidden: write triggered-run JSON to this path
[connection options]       Scheduler-specific: host, namespace, credentials
```

**`resume`:**

```
--run-id / --clone-run-id  The Metaflow run ID of the failed run
--run-param TEXT           (multiple=True) override parameters for the new run
--deployer-attribute-file  Hidden: write resumed-run JSON to this path
```

### Anatomy of a Well-Formed CLI Group

```python
@click.group()
def cli():
    pass

@cli.group(help="Commands related to MyScheduler orchestration.")
@click.pass_obj
def my_scheduler(obj):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)

@my_scheduler.command(help="Compile and deploy this flow.")
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option("--tag", "tags", multiple=True)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--deployer-attribute-file", default=None, hidden=True)
@click.pass_obj
def create(obj, branch, production, tags, with_decorators, deployer_attribute_file):
    ...
    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump({
                "name": deployment_name,       # plain string, NOT JSON
                "flow_name": obj.flow.name,
                "metadata": "{}",
                "additional_info": {...},
            }, f)
```

---

## 3. The Deployer Attribute File Protocol

When `create` and `trigger` are invoked with `--deployer-attribute-file <path>`, they MUST write a JSON object to that path before the subprocess exits. The Metaflow Deployer API reads this file via a FIFO; if the file is not written, the parent process hangs indefinitely.

### `create` writes:

```json
{
  "name": "plain-deployment-name",
  "flow_name": "MyFlow",
  "metadata": "{}",
  "additional_info": {
    "scheduler_specific_key": "value"
  }
}
```

The `name` field becomes `deployed_flow.deployer.name`. It MUST be a **plain string** (e.g., the deployment name, a Kestra flow ID, or the Metaflow flow class name). Do NOT embed JSON inside `name` unless you have an explicit reason (see the Dagster and Flyte exceptions below and their `from_deployment` implications).

### `trigger` writes:

```json
{
  "pathspec": "MyFlow/scheduler-<run_id>",
  "name": "deployment-name",
  "metadata": "{}"
}
```

The `pathspec` is used by the base `TriggeredRun.run` property to construct a `metaflow.Run`. It MUST be `"FlowClassName/run_id"` where `run_id` is the string that was passed to `metaflow step --run-id` when executing each step.

### Run ID Convention

Use a deterministic prefix so Metaflow runs are recognizable in the datastore:

```
kestra-<md5(execution_id)[:16]>
temporal-<workflow_id>
prefect-<flow_run_uuid>
dagster-<sha1(dagster_run_uuid)[:12]>
flyte-<execution_id>
```

This prefix must exactly match what you pass as `--run-id` to the `metaflow step` subprocess.

---

## 4. Environment Variables That MUST Be Passed to Step Subprocesses

This is the most error-prone area. Every orchestrator missed at least one of these. When a step subprocess is launched, it inherits whatever environment is present on the worker. That environment is almost never the same as the environment at compile time.

### The complete list

| Variable | Why it matters | Where to get it |
|----------|----------------|-----------------|
| `METAFLOW_FLOW_CONFIG_VALUE` | `config_expr` expressions and `@project` name resolution happen at task runtime. Without this, `config_expr` silently returns wrong values and `@project` may fail. | Read from `flow._flow_state[FlowStateItems.CONFIGS]` at compile time |
| `METAFLOW_DEFAULT_METADATA` | Which metadata provider to use. If the worker uses a different default than the compile-time environment, runs become invisible. | Copy from the compile-time env |
| `METAFLOW_DEFAULT_DATASTORE` | Which datastore backend. Workers may not have the same `.metaflowconfig` as the deployer. | Copy from the compile-time env |
| `METAFLOW_DATASTORE_SYSROOT_LOCAL` | **Must be pinned at compile time.** The local metadata sysroot is used to find artifact files. If the worker inherits a different sysroot (or none), it writes artifacts somewhere the test process cannot read. | Copy from the compile-time env; use `os.path.expanduser("~")` as fallback |
| `METAFLOW_S3_ENDPOINT_URL` | For local S3 emulators (MinIO). Workers in containers may not know the local endpoint URL. | Copy from the compile-time env |
| `METAFLOW_SERVICE_URL` | For the Metaflow Service metadata provider. | Copy from the compile-time env |
| `METAFLOW_CODE_URL`, `METAFLOW_CODE_SHA`, `METAFLOW_CODE_METADATA` | Required for the code package to be available in step containers. | Provided by `MetaflowPackage` after packaging |

### The pattern for reading METAFLOW_FLOW_CONFIG_VALUE at compile time

Use this exact pattern in your compiler/codegen class:

```python
from metaflow.flowspec import FlowStateItems

def _extract_flow_config_value(flow) -> Optional[str]:
    try:
        flow_configs = flow._flow_state[FlowStateItems.CONFIGS]
        config_env = {
            name: value
            for name, (value, _is_plain) in flow_configs.items()
            if value is not None
        }
        if config_env:
            return json.dumps(config_env)
    except Exception:
        pass
    return None
```

Then bake it as a constant in every compiled file or set it as an env var on every step subprocess.

### How to pass env vars to step subprocesses

The step subprocess is always launched as:

```
python myflow.py --datastore ... --metadata ... step <step_name> \
    --run-id <run_id> --task-id <task_id> \
    --retry-count <N> --max-user-code-retries <M> \
    --input-paths <paths>
```

You must inject env vars either:

1. In the environment inherited by the subprocess (set them in the worker process env), or
2. Explicitly in the `env=` argument to `subprocess.Popen` / `subprocess.run`

The safest approach is to inject them in the worker's environment at startup. For schedulers where the worker environment is a container (Kestra, Dagster), bake them as YAML variables or Python constants in the compiled file.

For schedulers where you control the subprocess directly (Prefect, Flyte local, Dagster trigger), inject them via a dict passed to `subprocess.Popen`:

```python
env = os.environ.copy()
env["METAFLOW_FLOW_CONFIG_VALUE"] = flow_config_value
env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
subprocess.Popen(cmd, env=env, ...)
```

### The `--branch` argument

For `@project` flows, every step subprocess must also receive `--branch <branch_name>` as a CLI argument. This determines the `project_branch` tag and the `branch_name` property in `current`. Without it, every step runs as `user.<username>` which breaks `test_hello_project`.

The branch name comes from the `--branch` option on `create`. If the user did not pass `--branch`, check whether the top-level Deployer API set it (look at `current.branch_name`) and bake it into the compiled workflow.

The Kestra pattern for detecting this:

```python
effective_branch = branch
if effective_branch is None and not production:
    try:
        from metaflow import current as _current
        bn = getattr(_current, "branch_name", None)
        if bn and not bn.startswith("user.") and not bn.startswith("prod"):
            if bn.startswith("test."):
                effective_branch = bn[len("test."):]
            else:
                effective_branch = bn
    except Exception:
        pass
```

---

## 5. Decorator Interactions

### @project

When a flow uses `@project(name="myproject")`:

- The Metaflow flow class name is stored in the datastore under a composite name (e.g., `myproject.test.mybranch.MyFlow`).
- Every `metaflow step` subprocess must receive `--branch <branch>` to produce the correct project-qualified run artifacts.
- The `trigger` command must also use the project-qualified name to construct the correct pathspec.
- Tags `project:<name>` and `project_branch:<branch>` are automatically added by Metaflow when `--branch` is passed.

### @retry

This is the second most commonly broken decorator. The scheduler knows which attempt number it is on (usually 1-indexed). The step subprocess must receive `--retry-count <N>` where N is `attempt_number - 1` (0-indexed). All five orchestrators initially hardcoded `--retry-count 0`, which made `test_retry` fail because the flow logic checked `current.retry_count < 1: raise`.

Pattern for deriving retry count:

- Kestra: `{{ taskrun.attemptsCount | default(1) | int - 1 }}`
- Prefect: `max(0, task_run.run_count - 1)` from `get_run_context()`
- Temporal: inject via the activity input struct; use `temporalio`'s attempt number
- Dagster: Dagster does not natively retry Metaflow steps; handle via `max_retries` on the op and pass the attempt number from op context
- Flyte: use `flytekit`'s retry mechanism and pass the attempt number from task context

Also set `METAFLOW_CURRENT_RETRY_COUNT=<N>` in the step environment so that decorators that read `current.retry_count` at decorator application time get the correct value.

### @conda

Do NOT disable conda tests by passing `conda: false` to the GHA action or by adding `-m "not conda"` to your pytest invocation. If conda is broken, fix the PATH. The conda binary must be on PATH when step subprocesses are launched.

When Metaflow's conda environment is active for a step, the step subprocess is run inside a conda-managed Python environment. The worker process must have `conda` (or `mamba`) on its PATH. For container-based workers this means the container image must include conda.

Setting `conda: false` in the GHA action is a workaround that hides real failures. The correct fix is to ensure the worker has conda available.

### @catch

`@catch` stores exceptions and allows the flow to continue from the next step. Your orchestrator must not special-case this; Metaflow handles it internally in the step subprocess. If the step subprocess succeeds (exit code 0), the flow continues normally even if a caught exception was stored.

The only orchestrator-level implication is that you must always proceed to the next step when a step exits cleanly, regardless of whether the step's logic raised an exception internally.

### @timeout

`@timeout` sets a wall-clock limit on step execution. You can optionally read `get_run_time_limit_for_task` from `metaflow.plugins.timeout_decorator` to extract the timeout value and apply it as a scheduler-side timeout on the task. However, the Metaflow runtime also enforces the timeout by signaling the subprocess, so scheduler-side enforcement is optional.

### @resources

`@resources` declares CPU, memory, and GPU requirements. The orchestrator should:

1. Read `resource_cpu`, `resource_memory`, `resource_gpu` from the step's decorators.
2. Forward them as `--with=resources:cpu=N,memory=M` in the step subprocess command, so that compute backend decorators (e.g., `@kubernetes`) can pick them up.
3. Optionally also apply them as scheduler-native resource constraints (Prefect task tags, Flyte task resources, etc.).

### Nested foreach

Nested foreach (a foreach step whose body is itself a foreach step) is the most complex graph pattern. It requires recursive fan-out at execution time.

Known support status:
- Kestra: supported (nested `ForEach` tasks)
- Prefect: supported (recursive `_chain_wiring_lines`)
- Temporal: supported via recursive activity dispatch
- Dagster: supported
- Flyte: **not supported** — the Flyte codegen wires foreach-body steps as fixed tasks inside a `@dynamic` expander and cannot recurse to produce a second level of `@dynamic` fan-out for a body step that is itself a foreach.
- Airflow: **not supported**

If your orchestrator cannot support nested foreach, skip the test cleanly with `pytest.skip()` and a descriptive reason:

```python
def test_nested_foreach(exec_mode, decospecs, compute_env, tag, scheduler_config):
    if exec_mode == "deployer" and scheduler_config.scheduler_type == "my_scheduler":
        pytest.skip(
            "Nested foreach is not supported by the MyScheduler deployer: <reason>"
        )
    ...
```

---

## 6. run_params Serialization

When calling `deployed_flow.trigger(key1=val1, key2=val2)`, the `run()` method of your `DeployedFlow` subclass must convert keyword arguments to `--run-param` CLI flags. Always serialize to a **`list`**, never a `tuple`.

The Metaflow click API type-checks `run_params` against `Optional[Union[List[str], Tuple[str]]]`. In Python's type system, `Tuple[str]` means a tuple with exactly one element. A tuple with more than one element fails this check, raising a typeguard validation error that surfaces as a cryptic `TypeError` at trigger time.

```python
# CORRECT
run_params = [f"{k}={v}" for k, v in kwargs.items()]

# WRONG — breaks with 2+ parameters
run_params = tuple(f"{k}={v}" for k, v in kwargs.items())
```

---

## 7. The from_deployment Contract

`DeployedFlow.from_deployment(identifier, impl="my_scheduler")` recovers a `DeployedFlow` from just an identifier string. The test that exercises this is `test_from_deployment`.

### What the identifier is

`identifier` is `deployed_flow.deployer.name` — a plain string, NOT `deployed_flow.id` (which may be a JSON blob).

In the test:

```python
deployment_id = deployed_flow.deployer.name
recovered = DeployedFlow.from_deployment(deployment_id, impl=impl)
```

Your `from_deployment` classmethod receives `deployment_id` as the `identifier` argument. This is the value you wrote as `"name"` in the deployer attribute file. If you wrote a plain deployment name (recommended), then `identifier` is that plain name.

### What your `from_deployment` must do

1. Parse the identifier (plain string, or JSON if you chose to embed JSON in `name`).
2. Reconstruct a deployer object that has at minimum:
   - `deployer.name` set to the deployment name
   - `deployer.flow_name` set to the Metaflow flow class name
   - `deployer.env_vars` populated with the required env vars (especially `METAFLOW_DEFAULT_METADATA`, `METAFLOW_DATASTORE_SYSROOT_LOCAL`)
   - `deployer.additional_info` with scheduler-specific connection info
3. Return `cls(deployer=deployer)`.

### The stub deployer pattern

When the original flow file is unavailable (the common case for `from_deployment`), you need a deployer that does not call `DeployerImpl.__init__` (which requires a valid flow file). Use the stub pattern:

```python
def _make_stub_deployer(name: str):
    from .my_scheduler_deployer import MySchedulerDeployer
    stub = object.__new__(MySchedulerDeployer)
    stub._deployer_kwargs = {}
    stub.flow_file = ""
    stub.show_output = False
    stub.profile = None
    stub.env = None
    stub.cwd = os.getcwd()
    stub.file_read_timeout = 3600
    stub.env_vars = os.environ.copy()
    stub.spm = SubprocessManager()
    stub.top_level_kwargs = {}
    stub.api = None
    stub.name = name
    stub.flow_name = name
    stub.metadata = "{}"
    stub.additional_info = {}
    return stub
```

Alternatively, if you have a flow file path available (e.g., from a locally persisted metadata file), use `generate_fake_flow_file_contents` to create a minimal valid flow file:

```python
from metaflow.runner.deployer import generate_fake_flow_file_contents
import tempfile

fake_contents = generate_fake_flow_file_contents(flow_name=flow_name, param_info={})
with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
    tmp.write(fake_contents)
    flow_file = tmp.name

deployer = MySchedulerDeployer(flow_file=flow_file, deployer_kwargs={})
```

### Environments in from_deployment

After reconstructing the deployer, you must restore the env vars that were active at compile time. The `trigger()` call on the recovered deployer will launch a subprocess with `deployer.env_vars` as its environment. If you do not restore `METAFLOW_DEFAULT_METADATA` and `METAFLOW_DATASTORE_SYSROOT_LOCAL`, the trigger subprocess will look for the flow in the wrong metadata provider and the run will never become visible to the test process.

The Flyte deployer stores these in `additional_info.saved_env` and applies them in `from_deployment`:

```python
saved_env = (deployer.additional_info or {}).get("saved_env", {})
if saved_env:
    deployer.env_vars = dict(deployer.env_vars)
    deployer.env_vars.update(saved_env)
```

Apply this pattern for any scheduler where you cannot guarantee the environment is identical between the `create` call and the `from_deployment` call.

---

## 8. Testing Requirements

### Use the REAL Orchestrator

Every test that runs in `exec_mode == "deployer"` must use the REAL scheduler. This means:

- For server-based schedulers (Kestra, Prefect, Dagster): a real server must be running and reachable.
- For cluster-based schedulers (Temporal): a real Temporal server and a running worker must be present.
- For remote execution schedulers (Flyte): a real Flyte cluster or local `pyflyte` must be available.

**Never use local-execution shortcuts** for deployer-mode tests:
- Do not call `execute_in_process()` (Dagster test helper that bypasses the real executor)
- Do not call `pyflyte run` without `--remote` as a substitute for cluster execution
- Do not import and call the flow function directly from within the test process

The test framework explicitly checks provenance (e.g., that the datastore type is `s3`, meaning the task actually ran in the devstack environment, not locally). Bypassing the real scheduler silently passes the test while hiding real failures that users will encounter.

### What "real orchestrator" means

| Orchestrator | What "real" means |
|---|---|
| Kestra | Kestra server running at `KESTRA_HOST`; flow is deployed via the REST API and executed in a Kestra pod/container |
| Prefect | Prefect server running (`prefect server start`); deployment registered; worker polling the work pool |
| Temporal | Temporal server running; worker process polling the task queue; workflows submitted via Temporal client |
| Dagster | Dagster server running (or Dagster's `execute_job` with a real `DagsterInstance`, not a mock); steps execute as Dagster ops |
| Flyte | `pyflyte run --remote` against a real Flyte cluster, or `pyflyte run` (local mode) where each task executes as a subprocess |

### Local Test Setup

All orchestrators should support running tests locally against a real scheduler. The entry point is:

```bash
# Export all required env vars, then:
python -m pytest test/ux/core/test_basic.py test/ux/core/test_config.py test/ux/core/test_dag.py \
  --scheduler-type my_scheduler \
  --exec-mode deployer \
  -m "not conda" \
  -v --tb=short
```

Maintain a `ux_test_config.yaml` in `test/ux/` that configures the scheduler. Example:

```yaml
backends:
  - name: my-scheduler-local
    scheduler_type: my_scheduler
    enabled: true
    decospec: null
    deploy_args:
      scheduler_host: http://localhost:8080
```

### The `ux_test_config.yaml` schema

```yaml
backends:
  - name: <unique backend name>
    scheduler_type: <orchestrator type, matches DeployerImpl.TYPE with - replaced by _>
    cluster: <optional cluster/namespace>
    enabled: true
    decospec: <optional compute decospec, e.g. "kubernetes:image=myimage">
    deploy_args:
      <scheduler-specific kwargs passed to .create()>
```

### Running Only Deployer Tests

```bash
python -m pytest test/ux/core/test_basic.py \
  --exec-mode deployer \
  --only-backend my-scheduler-local \
  -v
```

### Running Only Runner Tests (no scheduler needed)

```bash
python -m pytest test/ux/core/test_basic.py \
  --exec-mode runner \
  -v
```

---

## 9. The Complete Test Checklist

Each test below must pass in both `runner` and `deployer` mode unless marked scheduler-only.

### test_basic.py

**test_hello_world**
- Runs `basic/helloworld.py`
- Asserts `run["hello"].task.data.message == "Metaflow says: Hi!"`
- Common failure: env vars not propagated, metadata not found, wrong sysroot

**test_hello_project** (both runner and deployer)
- Runs `basic/helloproject.py` with `branch=<random_8char_uuid>`
- Asserts `"test." + branch == run["end"].task.data.branch`
- Common failure: `--branch` not passed to step subprocesses; branch defaults to `user.<username>` which does not match

**test_from_deployment** (deployer only, marked `scheduler_only`)
- Deploys `basic/hello_from_deployment.py`, triggers run1
- Recovers via `DeployedFlow.from_deployment(deployed_flow.deployer.name, impl=impl)`
- Triggers run2 from the recovered object
- Asserts both runs are successful
- Common failures:
  - `from_deployment` fails because it tries to JSON-parse a plain string
  - Second trigger uses wrong env vars (missing sysroot or metadata)
  - `deployer.name` returns a JSON blob instead of a plain name

**test_retry** (both modes)
- Runs `basic/retry_flow.py` which raises on the first attempt
- Asserts `run["flaky"].task.data.attempts == 1` (success on attempt index 1)
- Common failure: `--retry-count 0` hardcoded; Metaflow task sees retry count 0 on all attempts and never passes the `if current.retry_count < 1: raise` guard

**test_resources** (both modes)
- Asserts that `@resources` does not break execution
- No common orchestrator-specific failure; mostly a sanity check

**test_catch** (both modes)
- Asserts that `@catch` stores the exception and the flow continues
- Common failure: orchestrator marks the step as failed when the step exits 0 after catching

**test_timeout** (both modes)
- Asserts normal execution with `@timeout` present
- No common orchestrator-specific failure

**test_hello_conda** (both modes, marked `conda`)
- Requires conda on PATH in the worker environment
- Asserts specific library versions installed by `@conda`
- Common failure: `conda: false` was set to skip the test instead of fixing the PATH

### test_config.py

**test_config_simple_default**
- Deploys `config/config_simple.py` with default config
- Asserts `project:<project_name>` tag is present (requires `METAFLOW_FLOW_CONFIG_VALUE` in step env)
- Common failure: missing `METAFLOW_FLOW_CONFIG_VALUE`; `config_expr` evaluates against empty config, `@project` gets wrong name

**test_config_simple_config_value**
- Same as above with `config_value=[("cfg_default_value", {...})]` override
- Tests that `METAFLOW_FLOW_CONFIG_VALUE` carries the overridden values

**test_config_simple_config**
- Config provided via a JSON file (`config=[("cfg", "/path/to/file.json")]`)
- Simpler test; mainly verifies the config file is packaged and available

**test_mutable_flow_default** and **test_mutable_flow_config_value**
- Flow uses `FlowMutator` to add parameters and environment variables at compile time
- Asserts that the mutated parameters are present in task data
- Common failure: the flow module cache is stale between tests (the test framework handles this via `_evict_flow_module_cache`, but your deployer must not cache the compiled flow spec across calls)
- Common failure: `run_params` as tuple causes parameters not to be forwarded

**test_config_corner_cases**
- Tests `config_expr` with a function, `env_cfg`, and extra env vars injected by config
- Asserts `var1` and `var2` from config-defined env are present in task data
- These env vars are only set if `METAFLOW_FLOW_CONFIG_VALUE` is propagated correctly

### test_dag.py

**test_branch** (both modes)
- Parallel branches (split/join)
- Asserts `run["join"].task.data.values == ["a", "b"]`
- Both branches must produce results before the join step runs

**test_foreach** (both modes)
- Fan-out with `foreach`
- Asserts `run["join"].task.data.results == [2, 4, 6]`

**test_nested_foreach** (both modes)
- Nested foreach (foreach inside foreach)
- Skip with `pytest.skip()` if not supported, with a descriptive reason
- If supported, assert `run["outer_join"].task.data.all_results == ["x-1", "y-1"]`

---

## 10. GHA Workflow Template

The workflow should have two job categories:

1. **runner-tests**: No scheduler needed; test the extension in runner mode.
2. **deployer-tests**: One GHA runner per test file, parallelized via a matrix.

The key constraints are:
- Use `coverage-artifact-suffix: "${{ matrix.test }}"` to avoid artifact name collisions in matrix jobs.
- Set `conda: "true"` (the default) so all conda tests run.
- Set per-test timeouts, not global ones (deployer tests can vary significantly).
- Runner-tests need ~10 minutes; per-deployer-test jobs need ~15-20 minutes.

```yaml
name: UX Tests — MyScheduler

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  METAFLOW_REF: master

jobs:
  runner-tests:
    name: Runner tests (no scheduler)
    runs-on: ubuntu-latest
    timeout-minutes: 20

    steps:
      - uses: actions/checkout@v4

      - name: Start MinIO (if needed for local datastore)
        # Only needed if your tests use s3 datastore locally
        run: |
          docker run -d -p 9000:9000 \
            -e MINIO_ROOT_USER=minio \
            -e MINIO_ROOT_PASSWORD=minio123 \
            minio/minio server /data

      - name: Run runner-mode UX tests
        uses: Netflix/metaflow/.github/actions/run-ux-tests@master
        with:
          metaflow-ref: ${{ env.METAFLOW_REF }}
          extra-install: "."
          ux-test-config: |
            backends:
              - name: local
                scheduler_type: null
                enabled: true
                decospec: null
          pytest-args: "-n 4 -v --tb=short --timeout=300 --cov-fail-under=0"
          test-files: "test_basic.py test_config.py test_dag.py"
          conda: "true"
          coverage-artifact-suffix: "runner"
        env:
          METAFLOW_DEFAULT_DATASTORE: local
          METAFLOW_DEFAULT_METADATA: local

  deployer-tests:
    name: Deployer — ${{ matrix.test }}
    runs-on: ubuntu-latest
    timeout-minutes: 30

    strategy:
      fail-fast: false
      matrix:
        test: [test_basic.py, test_config.py, test_dag.py]

    steps:
      - uses: actions/checkout@v4

      - name: Start MyScheduler server
        run: |
          # Start your scheduler here, e.g.:
          docker compose up -d my-scheduler
          # Wait for it to be ready
          sleep 10

      - name: Run deployer UX tests (${{ matrix.test }})
        uses: Netflix/metaflow/.github/actions/run-ux-tests@master
        with:
          metaflow-ref: ${{ env.METAFLOW_REF }}
          extra-install: "."
          ux-test-config: |
            backends:
              - name: my-scheduler
                scheduler_type: my_scheduler
                enabled: true
                decospec: null
                deploy_args:
                  scheduler_host: http://localhost:8080
          pytest-args: >-
            -v --tb=short --timeout=1200 --cov-fail-under=0
            --exec-mode deployer
          test-files: ${{ matrix.test }}
          conda: "true"
          coverage-artifact-suffix: "${{ matrix.test }}"
        env:
          METAFLOW_DEFAULT_DATASTORE: local
          METAFLOW_DEFAULT_METADATA: local
          MY_SCHEDULER_HOST: http://localhost:8080

  coverage-report:
    name: Upload combined coverage
    needs: [runner-tests, deployer-tests]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          pattern: "coverage-*"
          merge-multiple: true
          path: coverage-reports/
      - name: Upload to Codecov
        uses: codecov/codecov-action@v5
        with:
          token: ${{ secrets.CODECOV_TOKEN }}
          files: coverage-reports/coverage.xml
```

### Critical GHA Notes

**Coverage artifact names must be unique.** Matrix jobs running in parallel all upload a `.coverage` file. If they use the same artifact name, GitHub Actions silently drops all but one. Always pass `coverage-artifact-suffix: "${{ matrix.test }}"`.

**The `.coverage` file is a hidden file.** The upload-artifact action requires `include-hidden-files: true` to capture it. This is already handled in the shared `run-ux-tests` action but mention it in your own upload steps if you add any.

**Do not upload coverage to Codecov from matrix jobs.** Only upload from the final aggregation step to avoid token conflicts and rate limits.

---

## 11. Common Mistakes (Anti-patterns)

The following mistakes were made across all five orchestrator implementations. Each one is documented with the symptom, root cause, and fix.

### Anti-pattern 1: Setting `conda: false` instead of fixing PATH

**Symptom:** `test_hello_conda` passes in CI but conda is not actually tested.

**Root cause:** The scheduler worker does not have `conda` on PATH. Instead of fixing the image or setup, `conda: false` is passed to the GHA action or `-m "not conda"` is added to pytest args.

**Fix:** Ensure conda (or mamba) is installed in the worker's environment. For container-based workers, install miniconda in the Docker image. For local workers, install conda before starting the worker process.

### Anti-pattern 2: Using execute_in_process() or pyflyte local mode as "deployer" mode

**Symptom:** Deployer-mode tests pass, but the scheduler is never actually exercised. CI passes, but users encounter failures when they deploy to a real scheduler.

**Root cause:** Dagster's `execute_in_process()` and Flyte's `pyflyte run` (without `--remote`) both run tasks in the same Python process without using the real scheduler execution model. Retries, resources, and environment isolation are all bypassed.

**Fix:** Use the real scheduler. For Dagster: use `execute_job()` with a `DagsterInstance`. For Flyte: use `pyflyte run --remote` with a real Flyte cluster.

### Anti-pattern 3: run_params as tuple

**Symptom:** `TypeError` when triggering with 2+ parameters. The error message mentions a type validation failure on `run_params`.

**Root cause:** The Metaflow click API validates `run_params` against `Union[List[str], Tuple[str]]` where the tuple variant means a 1-tuple. Multi-element tuples fail.

**Fix:** Always use a list comprehension: `run_params = [f"{k}={v}" for k, v in kwargs.items()]`

### Anti-pattern 4: Not propagating METAFLOW_FLOW_CONFIG_VALUE

**Symptom:** `test_config_simple_default` fails. The `project:<name>` tag is missing from the run because `config_expr` evaluated with an empty config and returned the wrong project name. Or: config-derived environment variables (`var1`, `var2`) are not present in task data.

**Root cause:** `METAFLOW_FLOW_CONFIG_VALUE` was not set in the step subprocess environment. The config was evaluated at the deployer side but not transmitted to the workers.

**Fix:** Read `flow._flow_state[FlowStateItems.CONFIGS]` at compile time. Serialize to JSON. Set it in every step subprocess environment. See the pattern in Section 4.

### Anti-pattern 5: from_deployment expecting a JSON identifier

**Symptom:** `test_from_deployment` fails with a JSON parse error or `KeyError: "flow_file"`.

**Root cause:** `from_deployment` was written to expect the `id` property (which may be a JSON blob) but receives `deployer.name` (which is a plain string).

**Fix:** Write `from_deployment` to accept a plain string. If you also want to support the JSON format for backwards compatibility, try JSON parsing first and fall back to treating the value as a plain name.

```python
@classmethod
def from_deployment(cls, identifier: str, metadata=None):
    try:
        info = json.loads(identifier)
    except (json.JSONDecodeError, ValueError):
        # Plain name — look up from local state or env vars
        info = {"name": identifier, "flow_name": identifier}
    ...
```

### Anti-pattern 6: --branch not passed to step subprocesses

**Symptom:** `test_hello_project` fails. The branch in the run output is `user.<username>` instead of `test.<branch>`.

**Root cause:** The compiled workflow calls `metaflow step` without `--branch`. The `@project` decorator defaults to `user.<username>` when no branch is specified.

**Fix:** Bake the `--branch` argument into every step subprocess invocation at compile time. If the user did not pass `--branch` explicitly, inherit it from `current.branch_name` (stripping the `test.` prefix).

### Anti-pattern 7: Retry count hardcoded to 0

**Symptom:** `test_retry` fails. The flow always raises because `current.retry_count` is always 0, so the guard `if current.retry_count < 1: raise` is always true.

**Root cause:** `--retry-count 0` is hardcoded in the step subprocess command template.

**Fix:** Derive the retry count from the scheduler's attempt number at runtime. Pass it as `--retry-count <N>` where N is the 0-indexed attempt count. Also set `METAFLOW_CURRENT_RETRY_COUNT=<N>` in the subprocess environment.

### Anti-pattern 8: Nested foreach not supported but not skipped

**Symptom:** `test_nested_foreach` either fails with a cryptic graph compilation error or produces wrong results.

**Root cause:** The compiler does not handle the case where a foreach body step is itself a foreach step.

**Fix:** Either implement support (see Kestra, Prefect, Temporal, Dagster for examples), or skip the test cleanly:

```python
if exec_mode == "deployer" and scheduler_config.scheduler_type == "my_scheduler":
    pytest.skip(
        "Nested foreach is not supported by the MyScheduler deployer: "
        "<specific reason why, e.g., 'the compiled template does not support "
        "recursive fan-out inside a task template'>"
    )
```

Do not leave the test to fail silently or produce incorrect results.

### Anti-pattern 9: Using GHA as the primary debugging loop

**Symptom:** 30+ CI runs to diagnose a single issue. Each iteration takes 10-20 minutes.

**Root cause:** The developer did not set up a local scheduler instance and cannot run tests locally.

**Fix:** Before writing any code, establish a local test environment:
1. Start the scheduler locally (Docker, `docker compose`, local binary, etc.).
2. Run a single test case locally with `python -m pytest ... -v -k test_hello_world`.
3. Only push to CI when local tests pass.

For Kestra: `docker run -p 8080:8080 kestra/kestra:latest server standalone --worker-thread=128`

For Prefect: `prefect server start` and `prefect worker start --pool default-agent-pool`

For Temporal: `temporal server start-dev`

For Dagster: `dagster dev -f my_dagster_defs.py`

For Flyte (local): just run `pyflyte run` locally; no server needed

### Anti-pattern 10: Hardcoding datastore sysroot instead of pinning at compile time

**Symptom:** Tests pass locally but fail in CI, or pass in deployer mode but the metadata provider reports the run as missing.

**Root cause:** The worker uses `os.path.expanduser("~")` at runtime to find metadata, but the deployer wrote metadata to a different path. Or: `METAFLOW_DATASTORE_SYSROOT_LOCAL` is read from the worker's environment which is different from the deployer's environment.

**Fix:** At compile time, read `METAFLOW_DATASTORE_SYSROOT_LOCAL` from `os.environ` and bake it into the compiled workflow as a constant. Do not rely on the worker's runtime environment to have this set correctly.

```python
# In your compiler, at compile time:
datastore_sysroot_local = os.environ.get(
    "METAFLOW_DATASTORE_SYSROOT_LOCAL",
    os.path.expanduser("~")
)

# Bake into generated file:
f"DATASTORE_SYSROOT_LOCAL: str = {datastore_sysroot_local!r}"

# In every step subprocess:
env["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = DATASTORE_SYSROOT_LOCAL  # the constant
```

---

## Appendix: DeployerImpl Registration

Register your deployer in the extension's `mfextinit_<name>.py`:

```python
from metaflow.plugins import DEPLOYER_IMPL_PROVIDERS

DEPLOYER_IMPL_PROVIDERS += [MySchedulerDeployer]
```

And register the CLI group:

```python
from metaflow.cli import start

@start.command()
@click.pass_obj
def my_scheduler(obj):
    ...
```

Or use the `EXTENSIONS` mechanism and `mfextinit_<name>.py` to inject into `DEPLOYER_IMPL_PROVIDERS` and add the CLI group.

The `DeployerImpl.TYPE` must exactly match the CLI group name, using hyphens (not underscores). The Deployer metaclass automatically converts hyphens to underscores when injecting methods onto `Deployer`:

```python
class MySchedulerDeployer(DeployerImpl):
    TYPE = "my-scheduler"  # hyphens here

# This makes the following work automatically:
deployer = Deployer(flow_file)
deployed = deployer.my_scheduler(**kwargs).create(...)  # underscores here
```

---

## Appendix: TriggeredRun.run Property Pattern

All five orchestrators implement the same pattern for the `run` property. Copy this exactly:

```python
@property
def run(self):
    import metaflow
    from metaflow.exception import MetaflowNotFound

    env_vars = getattr(self.deployer, "env_vars", {}) or {}
    meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
    sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

    old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
    old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    try:
        if meta_type:
            os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
            metaflow.metadata(meta_type)
        if meta_type == "local" and sysroot is None:
            sysroot = os.path.expanduser("~")
        if sysroot:
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
        return metaflow.Run(self.pathspec, _namespace_check=False)
    except MetaflowNotFound:
        return None
    except Exception:
        return None
    finally:
        if old_meta is None:
            os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
        else:
            os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
        if old_sysroot is None:
            os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
        else:
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot
```

The try/finally ensures the process environment is restored even if an exception occurs. The `_namespace_check=False` is required because deployer tests often run outside the default namespace.
