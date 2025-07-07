# Tutorial: Hello Nomad - Running Metaflow Steps on HashiCorp Nomad

This tutorial demonstrates how to execute a Metaflow step on a HashiCorp Nomad cluster using the `@nomad` decorator.

## Prerequisites

1.  **Metaflow Installed:** Ensure Metaflow is installed.
2.  **Nomad Cluster:** You need access to a running Nomad cluster. For local testing, you can run Nomad in development mode:
    ```bash
    nomad agent -dev -bind 0.0.0.0 -log-level INFO
    ```
    This will typically make Nomad available at `http://localhost:4646`.
3.  **`python-nomad` Library:** The Metaflow execution environment (and the Docker image used by Nomad tasks) needs the `python-nomad` library. Install it via pip:
    ```bash
    pip install python-nomad
    ```
4.  **Docker:** Nomad's default task driver for Metaflow is Docker, so Docker must be installed and running on the Nomad client nodes where jobs will execute.
5.  **Environment Variable (Optional but Recommended):** Set the `METAFLOW_NOMAD_ADDRESS` environment variable to your Nomad server's address:
    ```bash
    export METAFLOW_NOMAD_ADDRESS="http://localhost:4646"
    ```
    Alternatively, you can configure this in your `metaflow_config.json`.

## The Flow: `hello-nomad.py`

The flow `hello-nomad.py` is very simple:
- `start`: Initiates the flow.
- `nomad_step`: This step is decorated with `@nomad` and will be dispatched to your Nomad cluster for execution. It demonstrates requesting specific CPU, memory, and disk resources.
- `end`: Finishes the flow after the Nomad step completes.

```python
from metaflow import FlowSpec, step, nomad, resources, current
import os

class HelloNomadFlow(FlowSpec):
    # ... (flow code from hello-nomad.py) ...
```

## Running the Flow

1.  **Navigate to the tutorial directory:**
    ```bash
    cd metaflow/tutorials/09-hello-nomad
    ```

2.  **Run the flow:**
    ```bash
    python hello-nomad.py run
    ```
    If you haven't set `METAFLOW_NOMAD_ADDRESS` as an environment variable, you might need to configure it, or the client might default to a pre-configured address if available.

## Expected Output

You should see output similar to this (details like task IDs will vary):

```
Metaflow 2.x.x executing HelloNomadFlow for user:youruser
Workflow starting (run-id 167xxxxxxx):
 - View this run in UI: <UI_LINK_IF_AVAILABLE>
[167xxxxxxx][start] HelloNomadFlow starting.
[167xxxxxxx][start] Next, we will run a step on Nomad.
[167xxxxxxx][start] Task finished successfully.
[167xxxxxxx][nomad_step] Task is starting on Nomad... (details about job ID, eval ID)
[167xxxxxxx][nomad_step] Hello from a Nomad task!
[167xxxxxxx][nomad_step] This step is running as part of flow: HelloNomadFlow
[167xxxxxxx][nomad_step] Run ID: 167xxxxxxx, Step: nomad_step, Task ID: <task_id>
[167xxxxxxx][nomad_step] METAFLOW_NOMAD_WORKLOAD environment variable: 1
[167xxxxxxx][nomad_step] Successfully identified as a Nomad workload via environment variable.
[167xxxxxxx][nomad_step] Nomad step computation complete.
[167xxxxxxx][nomad_step] Task finished successfully.
[167xxxxxxx][end] Back from Nomad. Message: Successfully ran a step on Nomad!
[167xxxxxxx][end] HelloNomadFlow finished successfully!
Done!
```

If the Nomad step is launched successfully, you will also see corresponding job and allocation activity in your Nomad UI or via the Nomad CLI (`nomad job status`, `nomad alloc status`).

## Exploring Further

-   Modify the `@nomad` decorator in `hello-nomad.py` to request different resources (`cpu`, `memory`, `disk`).
-   If your Nomad cluster has specific constraints or datacenters, try using the `datacenters` and `constraints` arguments of the `@nomad` decorator.
-   Use the `metaflow nomad list` and `metaflow nomad kill` commands (from the directory containing `hello-nomad.py`) to manage jobs launched by this flow.

This tutorial provides a basic introduction. For more advanced use cases, refer to the main [Metaflow documentation on using Nomad](https://docs.metaflow.org/scaling/remote-tasks/nomad) (once available).
```

**Note:** The final link `https://docs.metaflow.org/scaling/remote-tasks/nomad` in the README is a placeholder for where the `docs/nomad.md` file (created in the previous step) would eventually be hosted.
