# Using Metaflow with HashiCorp Nomad

Metaflow allows yous to execute your workflow steps as jobs on a HashiCorp Nomad cluster. This enables scaling out your computations and managing them using Nomad's orchestration capabilities.

## Prerequisites

Before using the `@nomad` decorator, ensure that:
1.  You have a running Nomad cluster accessible from where you are launching your Metaflow flows.
2.  The `python-nomad` library is installed in your Metaflow execution environment (`pip install python-nomad`).
3.  Your Metaflow deployment is configured with the necessary Nomad parameters (e.g., `METAFLOW_NOMAD_ADDRESS`). See [Metaflow Configuration](https://docs.metaflow.org/metaflow-on-aws/metaflow-configuration) for details on how to set these.

## The `@nomad` Decorator

To execute a step on Nomad, apply the `@nomad` decorator to it:

```python
from metaflow import FlowSpec, step, nomad, resources

class NomadFlow(FlowSpec):

    @step
    def start(self):
        print("Starting the flow.")
        self.next(self.compute_on_nomad)

    @nomad(cpu=2, memory=1000, disk=500) # Request 2000 MHz CPU, 1000MB RAM, 500MB Disk
    @resources(cpu=1, memory=500) # General resource request
    @step
    def compute_on_nomad(self):
        print("This step is running on Nomad!")
        # Perform your computation here
        self.result = sum(i*i for i in range(1000))
        self.next(self.end)

    @step
    def end(self):
        print(f"Computation result: {self.result}")
        print("Flow finished.")

if __name__ == "__main__":
    NomadFlow()
```

When both `@nomad` and `@resources` are present, the resource values specified in `@nomad` will take precedence for that specific step if they are provided. Otherwise, it falls back to `@resources`, and then to Metaflow defaults for Nomad.

### Decorator Arguments

The `@nomad` decorator accepts the following arguments:

*   `image` (str, optional): The Docker image to use for the Nomad job. If not specified, it defaults to `METAFLOW_NOMAD_DEFAULT_IMAGE` from your Metaflow configuration, or a Python version-based image.
*   `region` (str, optional): The Nomad region where the job should be submitted. Defaults to `METAFLOW_NOMAD_REGION`.
*   `datacenters` (list[str], optional): A list of datacenters within the region where the job can run. Defaults to `METAFLOW_NOMAD_DEFAULT_DATACENTERS`. Example: `['dc1', 'east-2a']`.
*   `constraints` (list[dict], optional): A list of Nomad [constraints](https://developer.hashicorp.com/nomad/docs/job-specification/constraint) to apply to the job. This allows fine-grained control over where your tasks are placed. Example: `[{"attribute": "${attr.kernel.name}", "operator": "=", "value": "linux"}]`.
*   `cpu` (int, optional): CPU resources to request for the job, in MHz (e.g., `1000` for 1 vCPU). Overrides `@resources.cpu`.
*   `memory` (int, optional): Memory resources to request for the job, in MB (e.g., `4096` for 4GB). Overrides `@resources.memory`.
*   `disk` (int, optional): Disk resources to request for the job's ephemeral disk, in MB (e.g., `10240` for 10GB). Overrides `@resources.disk`.

You can also specify these options on the command line for a run:
```bash
python your_flow.py run --with nomad:image=your/custom-image,cpu=4000
```

## Resource Management

Metaflow translates the `cpu`, `memory`, and `disk` parameters into Nomad job resource stanzas.
*   `cpu`: Maps to `Resources.CPU` in the Nomad task (MHz).
*   `memory`: Maps to `Resources.MemoryMB` in the Nomad task (MB).
*   `disk`: Maps to `TaskGroup.EphemeralDisk.SizeMB` in the Nomad job (MB). Ensure your Nomad client nodes are configured to allow ephemeral disk usage for tasks.

If a step does not have `@nomad` specific resource values, it will use values from an `@resources` decorator if present, or fallback to defaults defined by `METAFLOW_NOMAD_CPU`, `METAFLOW_NOMAD_MEMORY`, and `METAFLOW_NOMAD_DISK` in your Metaflow configuration.

## GPU Support

To use GPUs with your Nomad tasks:
1.  Ensure your Nomad cluster has nodes with GPUs and the necessary drivers installed.
2.  Configure Nomad client nodes to advertise GPU resources. See Nomad's documentation on [Device Plugin](https://developer.hashicorp.com/nomad/docs/drivers/docker#device-plugin) for Docker tasks.
3.  Use the `constraints` argument in the `@nomad` decorator to target GPU-enabled nodes. For example, if your GPU nodes have a specific attribute or class:
    ```python
    @nomad(constraints=[{"attribute": "${attr.driver.nvidia.compute.capability}", "operator": ">=", "value": "6.0"}])
    ```
    Or, if you are using device plugins that expose resources like `nvidia/gpu`:
    ```python
    # This requires changes in how Nomad plugin generates jobspec to include device requests.
    # For now, use constraints to target nodes with GPUs.
    # A future update might allow specifying GPU count directly if python-nomad and jobspec allow easy mapping.
    ```
   You might also need to ensure your Docker `image` includes the necessary CUDA libraries.

## Accessing Logs

Metaflow automatically captures `stdout` and `stderr` from your Nomad tasks and stores them in your configured datastore (e.g., S3). You can access these logs using the standard Metaflow `logs` command:
```bash
python your_flow.py logs your_run_id/step_name
```
For live or raw logs directly from Nomad (useful for debugging the Nomad task itself), you would typically use the Nomad CLI:
```bash
nomad alloc logs <allocation_id> <task_name>
```
The Metaflow `nomad step` execution will also attempt to print the raw stdout/stderr from the Nomad task upon completion or failure for immediate feedback.

## Managing Nomad Jobs

Metaflow provides CLI commands to interact with jobs launched by the `@nomad` plugin.

### Listing Active Jobs

To list active Nomad jobs associated with your flows:
```bash
python your_flow.py nomad list --help
```
Options:
*   `--my-runs`: List all my unfinished tasks (based on local username).
*   `--user TEXT`: List unfinished tasks for a specific user.
*   `--run-id TEXT`: List unfinished tasks for a specific Metaflow run ID.
*   `--prefix TEXT`: Filter jobs by a Nomad job name prefix.

Example:
```bash
python your_flow.py nomad list --run-id 1678886400123456
```

### Terminating Jobs

To terminate (deregister) running Nomad jobs:
```bash
python your_flow.py nomad kill --help
```
Options:
*   `--job-id TEXT`: Specific Nomad job ID(s) to terminate. Can be used multiple times.
*   `--my-runs`: Kill all my unfinished tasks.
*   `--user TEXT`: Terminate unfinished tasks for the given user.
*   `--run-id TEXT`: Terminate unfinished tasks for a specific Metaflow run ID.
*   `--prefix TEXT`: Terminate jobs matching a Nomad job name prefix.
*   `--purge`: Purge the job from Nomad after stopping (GCs it immediately).

Example:
```bash
python your_flow.py nomad kill --job-id my-flow-1678886400123456-stepname-taskid-abcdef
```

## Troubleshooting

*   **Job stuck in PENDING:**
    *   Check if the requested resources (`cpu`, `memory`, `disk`) are available in your Nomad cluster.
    *   Verify that `datacenters` and `region` specified match your Nomad setup.
    *   Inspect `constraints` to ensure they are not preventing placement.
    *   Use `nomad job status <job_id>` and `nomad alloc status <alloc_id>` for details.
*   **Task Fails Immediately:**
    *   Check the Docker `image` can be pulled by Nomad clients.
    *   Ensure the command and arguments are correctly formed. Raw logs from `nomad alloc logs` can be helpful.
    *   Verify that the Metaflow package (code) can be downloaded from `code_package_url` by the Nomad client node.
*   **Authentication/Authorization Issues:**
    *   Ensure `METAFLOW_NOMAD_ADDRESS` is correct.
    *   If using ACLs, make sure `METAFLOW_NOMAD_TOKEN` is set and has the necessary permissions.
    *   For mTLS, verify `METAFLOW_NOMAD_CLIENT_CERT`, `METAFLOW_NOMAD_CLIENT_KEY`, and `METAFLOW_NOMAD_CACERT` paths and contents.

For further assistance, consult the Nomad documentation or reach out on the [Metaflow Slack channel](http://slack.outerbounds.co/).
