import os
import sys
import json
import traceback

from metaflow._vendor import click
from metaflow.exception import METAFLOW_EXIT_DISALLOW_RETRY, MetaflowException
from metaflow.metadata_provider.util import sync_local_metadata_from_datastore
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.mflog import TASK_LOG_SOURCE # For log saving location

from .nomad import NomadClient, NomadException

# Helper to parse CLI options, similar to Kubernetes CLI
def parse_nomad_cli_options(flow_name, run_id_opt, user_opt, my_runs_opt, echo_fn):
    if my_runs_opt:
        if user_opt:
            raise MetaflowException("--user is not supported with --my-runs.")
        user = os.environ.get("METAFLOW_USER", os.environ.get("USER"))
    elif user_opt:
        user = user_opt
    else:
        user = None # Default to all users if not specified

    if run_id_opt:
        run_id = run_id_opt
    else:
        run_id = None

    # Flow name is usually derived from the current flow context if not specified
    # For now, assume it's available or passed if needed for filtering.
    # The list/kill commands might need more sophisticated filtering based on Nomad job names/meta.
    return flow_name, run_id, user


@click.group()
def cli():
    pass

@cli.group(help="Commands related to HashiCorp Nomad.")
@click.pass_context
def nomad(ctx):
    # Make the Nomad client available to subcommands via ctx.obj if needed
    # For now, each command will instantiate its own client.
    pass

@nomad.command(
    help="Execute a single task on Nomad. This command is called internally by Metaflow."
)
@click.argument("step-name")
@click.argument("code-package-sha")
@click.argument("code-package-url")
# Options from NomadDecorator - these names should match command_options in nomad_decorator.py (with '-' instead of '_')
@click.option("--executable", help="Path to the python executable within the container.") # Added
@click.option("--image", help="Docker image to use for the Nomad job.")
@click.option("--region", help="Nomad region for the job.")
@click.option("--datacenters", type=str, help="JSON string of Nomad datacenters for the job (e.g., '[\"dc1\",\"dc2\"]').")
@click.option("--constraints", type=str, help="JSON string of Nomad constraints for the job.")
@click.option("--cpu", help="CPU resources in MHz.")
@click.option("--memory", help="Memory resources in MB.")
@click.option("--disk", help="Disk resources in MB.")
@click.option("--nomad-namespace-job", help="Nomad namespace for this specific job.") # Distinguish from client's default namespace
# Metaflow common task options
@click.option("--run-id", required=True, help="Current Metaflow run ID.")
@click.option("--task-id", required=True, help="Current Metaflow task ID.")
@click.option("--input-paths", help="Input paths for the task.") # Not directly used by Nomad client, but part of step CLI
@click.option("--split-index", help="Split index for the task.") # Same as above
@click.option("--clone-path", help="Clone path for the task.") # Same as above
@click.option("--clone-run-id", help="Clone run ID for the task.") # Same as above
@click.option("--tag", multiple=True, default=None, help="Metaflow tags for the run.")
@click.option("--namespace", default=None, help="Metaflow namespace, not Nomad namespace.") # Metaflow's own namespace
@click.option("--retry-count", default=0, type=int, help="Current retry count for the task.")
@click.option("--max-user-code-retries", default=0, type=int, help="Max user code retries.")
# TODO: Add other options if needed, e.g., from Kubernetes CLI like service_account, secrets, etc. if we support them.
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable,
    image,
    region,
    datacenters,
    constraints,
    cpu,
    memory,
    disk,
    nomad_namespace_job,
    run_id,
    task_id,
    input_paths,
    split_index,
    clone_path,
    clone_run_id,
    tag,
    namespace, # Metaflow namespace
    retry_count,
    max_user_code_retries,
    **kwargs # To catch any other options passed from decorator
):
    # `kwargs` will capture any other options defined in the decorator and passed via command_options
    # These can be passed to NomadClient.launch_job if it's designed to accept them.

    echo = ctx.obj.echo_always # For printing output

    # Reconstruct the original step CLI arguments that the Nomad task will execute
    # This is what the user's Python script will run inside the container.
    # The `executable` is python, `sys.argv[0]` is the flow file name.
    # `ctx.parent.parent.params` are top-level Metaflow args (e.g. --datastore, --metadata)
    # `kwargs` here are the step-specific command line args from the `step` call.

    # The step_cli_args should be the command that the user's code executes.
    # It's formed from: executable, flow_file_name, top-level_metaflow_args, "step", step_name, task_args...
    # The `nomad_decorator.runtime_step_cli` sets `cli_args.entrypoint` to be the command.
    # And `cli_args.command_options` contains all the decorator attributes.
    # The arguments to *this* `nomad step` command are those attributes.
    # The command to run *inside* Nomad is the original `python flow.py step <step_name> --run-id ...`

    # We need to get the flow_datastore and environment from ctx.obj
    flow_datastore = ctx.obj.flow_datastore
    environment = ctx.obj.environment
    flow_name = ctx.obj.flow.name
    user = ctx.obj.metadata.user() # Get current user from metadata

    # Construct the command that the Nomad task will execute (the user's Metaflow step code)
    # This is similar to how Kubernetes CLI constructs `step_cli`
    # The `executable` here is the one from the *decorator's* environment.

    # The `step_cli_args` passed to `NomadClient.launch_job` should be the
    # *original* step command, not the `metaflow nomad step` command.
    # This means we need to reconstruct it or have it passed through.
    # The `kubernetes_cli.py` uses `ctx.obj.environment.executable(step_name, executable_option)`
    # and then builds the full command string.

    # Let's assume `executable` is the python interpreter in the container.
    # The arguments to that python interpreter need to be assembled.
    # This is complex because we are inside `metaflow nomad step` which is ALREADY a step execution context.
    # The `kubernetes_cli.py` `step` command is what gets run *by* the K8s job.
    # Our `nomad_cli.py` `step` command is the one that *launches* the Nomad job.

    # The `step_cli` for `NomadClient.launch_job` should be:
    # [executable, flow_file_name, top_level_args..., "step", step_name, original_step_args...]
    # `original_step_args` are things like --run-id, --task-id, --input-paths etc. for the *user's* step code.

    # The `executable` passed to this function is the one from `NomadDecorator.runtime_step_cli` -> `cli_args.entrypoint[0]`
    # which is `self.environment.executable(self.step)`

    original_step_task_args = {
        "run_id": run_id,
        "task_id": task_id,
        "input_paths": input_paths,
        "split_index": split_index,
        "clone_path": clone_path,
        "clone_run_id": clone_run_id,
        "tag": list(tag), # Convert tuple to list
        "namespace": namespace, # Metaflow namespace
        "retry_count": retry_count,
        "max_user_code_retries": max_user_code_retries,
        # Add any other standard step arguments if needed
    }
    # Filter out None values, convert to CLI options
    filtered_step_task_args = {k: v for k, v in original_step_task_args.items() if v is not None}
    step_task_options_list = util.dict_to_cli_options(filtered_step_task_args)

    # Get top-level args (like --with, --datastore)
    # These are usually in ctx.parent.parent.params for a deeply nested command.
    # This depends on how `metaflow nomad step` is invoked by the runtime.
    # For now, assume they are not needed directly by the client, but by the command inside Nomad.
    # The `kubernetes_cli.py` seems to get them from `ctx.parent.parent.params`.
    # Let's assume `ctx.obj.top_level_options` holds these. (This needs verification)
    top_level_options_list = []
    if hasattr(ctx.obj, 'top_level_options'):
        top_level_options_list = util.dict_to_cli_options(ctx.obj.top_level_options)


    # sys.argv[0] is `metaflow` when run as `metaflow nomad step ...`
    # We need the original flow file name. This should be part of `ctx.obj` or passed.
    # Let's assume `ctx.obj.flow_name_path` or similar. For now, use a placeholder.
    # This is typically `os.path.basename(sys.argv[0])` in the *user's* execution context.
    # The Kubernetes CLI gets it from `os.path.basename(sys.argv[0])` because *it is* the user's context.
    # Here, we are one level deeper.
    # It's available in `ctx.obj.flow.script_name`
    flow_script_name = os.path.basename(ctx.obj.flow.script_name)

    # This is the command that will run *inside* the Nomad job container
    cmd_to_run_in_nomad = [executable, flow_script_name]
    cmd_to_run_in_nomad.extend(top_level_options_list) # Add top level options like --datastore
    cmd_to_run_in_nomad.append("step")
    cmd_to_run_in_nomad.append(step_name)
    cmd_to_run_in_nomad.extend(step_task_options_list)

    # Parse JSON string options
    parsed_datacenters = json.loads(datacenters) if datacenters else None
    parsed_constraints = json.loads(constraints) if constraints else None

    # Environment variables for the job (from @environment decorator)
    # This should be collected by the decorator and passed as an option.
    # For now, assume it's empty or passed via kwargs if we add an --env option.
    user_env_vars = {}
    if 'env' in kwargs and kwargs['env']:
        try:
            user_env_vars = json.loads(kwargs['env']) if isinstance(kwargs['env'], str) else kwargs['env']
        except json.JSONDecodeError:
            echo("Warning: Could not parse --env JSON string.", err=True)


    def _sync_metadata():
        if ctx.obj.metadata.TYPE == "local":
            sync_local_metadata_from_datastore(
                DATASTORE_LOCAL_DIR,
                flow_datastore.get_task_datastore(run_id, step_name, task_id)
            )

    nomad_client = None
    try:
        nomad_client = NomadClient(
            # Pass relevant client config from Metaflow config if needed,
            # or rely on env vars NOMAD_ADDR etc.
            # region, nomad_namespace_job are for the *job*, not client config here.
        )

        launched_job_info = nomad_client.launch_job(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            attempt=retry_count,
            user=user,
            code_package_sha=code_package_sha,
            code_package_url=code_package_url,
            code_package_ds=flow_datastore.TYPE,
            step_cli_args=cmd_to_run_in_nomad, # This is the command to run inside Nomad
            docker_image=image,
            datacenters=parsed_datacenters,
            region=region, # Job region
            nomad_namespace_job=nomad_namespace_job, # Job namespace
            cpu=cpu, memory=memory, disk=disk,
            constraints=parsed_constraints,
            environment_vars_decorator=user_env_vars,
            # Pass other relevant kwargs if NomadClient.launch_job supports them
        )

        echo(f"Nomad job launched: ID {launched_job_info['job_id']}, EvalID {launched_job_info['eval_id']}", fg="green")

        # Wait for completion
        # TODO: Echo logs during wait, similar to Kubernetes CLI
        # This requires NomadClient.wait_for_job_completion to potentially yield logs or have a callback.
        # For now, just wait and then print final status.
        job_succeeded, final_alloc_id = nomad_client.wait_for_job_completion(
            launched_job_info['job_id'],
            launched_job_info['eval_id']
            # TODO: Add timeout from decorator if implemented
        )

        if final_alloc_id:
            echo(f"Monitored allocation: {final_alloc_id}", fg="blue")
            # Try to get final logs (mflog should have saved them to S3)
            # This is more for debugging the raw output from Nomad if needed.
            # The primary log mechanism is via BASH_SAVE_LOGS to the datastore.
            try:
                # The task name used in _generate_job_spec was f"mf-task-{step_name}"
                task_name_in_nomad = f"mf-task-{step_name}"
                stdout_logs = nomad_client.get_logs(final_alloc_id, task_name_in_nomad, "stdout")
                stderr_logs = nomad_client.get_logs(final_alloc_id, task_name_in_nomad, "stderr")
                if stdout_logs:
                    echo(f"\n--- Nomad Task STDOUT (alloc: {final_alloc_id}, task: {task_name_in_nomad}) ---\n{stdout_logs}")
                if stderr_logs:
                    echo(f"\n--- Nomad Task STDERR (alloc: {final_alloc_id}, task: {task_name_in_nomad}) ---\n{stderr_logs}", err=True)
            except NomadException as log_ex:
                echo(f"Could not retrieve final raw logs from Nomad: {log_ex}", err=True)

        if not job_succeeded:
            echo(f"Nomad job {launched_job_info['job_id']} failed or timed out.", err=True, fg="red")
            # Sync metadata before exiting on failure
            _sync_metadata()
            sys.exit(1) # Indicate failure

        echo(f"Nomad job {launched_job_info['job_id']} completed successfully.", fg="green")

    except NomadException as e:
        echo(f"Nomad execution error: {e}", err=True, fg="red")
        traceback.print_exc()
        _sync_metadata() # Ensure metadata is synced on Nomad-specific errors too
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY) # Disallow Metaflow runtime retry for Nomad infrastructure issues
    except Exception as e:
        echo(f"Error during Nomad step execution: {e}", err=True, fg="red")
        traceback.print_exc()
        _sync_metadata()
        # For generic errors, let Metaflow decide on retries unless it's a MetaflowException
        if isinstance(e, MetaflowException):
            sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
        sys.exit(1)
    finally:
        # This will sync metadata like logs saved to S3, task status etc.
        _sync_metadata()


@nomad.command(help="List running Nomad jobs for this flow or user.")
@click.option("--my-runs", is_flag=True, help="List all my unfinished tasks.")
@click.option("--user", help="List unfinished tasks for the given user.")
@click.option("--run-id", help="List unfinished tasks for a specific run ID.")
@click.option("--prefix", help="Filter jobs by a name prefix (e.g., 'mf-myflowname-').")
@click.pass_context
def list(ctx, my_runs, user, run_id, prefix):
    echo = ctx.obj.echo
    flow_name_ctx = ctx.obj.flow.name if hasattr(ctx.obj, 'flow') else None

    # Use a default prefix if none is given, e.g., based on current flow
    if not prefix and flow_name_ctx:
        # A common job name pattern might be "mf-flowname-runid-..."
        # Listing might need to be more flexible or rely on Nomad metadata/tags if we set them.
        # For now, let's use a generic "mf-" prefix or allow user to specify.
        effective_prefix = f"mf-{flow_name_ctx}-"
        if run_id:
            effective_prefix += f"{run_id}-"
        # If user is specified, we can't directly filter by user via Nomad API prefix easily unless user is in job name.
        # This might require listing more jobs and then client-side filtering if user is in job Meta.
        echo(f"Listing jobs with effective prefix: {effective_prefix} (user filter '{user}' if any is client-side for now)")
    else:
        effective_prefix = prefix

    try:
        client = NomadClient()
        jobs = client.list_jobs(prefix=effective_prefix) # Nomad API prefix filter

        if not jobs:
            echo("No matching Nomad jobs found.")
            return

        for job_summary in jobs:
            # Client-side filter for user if applicable, assuming user is in job ID or Name or Meta.
            # This is a basic example; real filtering might need job details if user isn't in summary.
            # Job ID format: f"mf-{flow_name}-{run_id}-{step_name}-{task_id}-{uuid}"
            # Job Name format: f"{flow_name}-{run_id}-{step_name}-{task_id}"
            display_job = True
            if user:
                # This is a placeholder for actual user filtering logic.
                # Nomad job summaries don't typically include user directly.
                # We might need to embed user in job Meta or rely on naming conventions.
                # For now, if --user is passed, we just print a note.
                pass # echo(f"User filtering for '{user}' requires job metadata or naming convention.")

            if display_job:
                echo(
                    f"Job ID: *{job_summary['ID']}* Name: *{job_summary['Name']}* "
                    f"Status: *{job_summary['Status']}* Type: {job_summary['Type']} "
                    f"SubmitTime: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_summary['SubmitTime'] // 10**9))}"
                )
    except NomadException as e:
        echo(f"Error listing Nomad jobs: {e}", err=True)
    except Exception as e:
        echo(f"Generic error listing Nomad jobs: {e}", err=True)
        traceback.print_exc()


@nomad.command(help="Terminate specified Nomad jobs.")
@click.option("--my-runs", is_flag=True, help="Kill all my unfinished tasks (requires naming convention or metadata).")
@click.option("--user", help="Terminate unfinished tasks for the given user (requires naming convention or metadata).")
@click.option("--run-id", help="Terminate unfinished tasks for a specific run ID.")
@click.option("--job-id", multiple=True, help="Specific Nomad job ID(s) to terminate. Can be used multiple times.")
@click.option("--prefix", help="Terminate jobs matching a name prefix.")
@click.option("--purge", is_flag=True, default=False, help="Purge the job from Nomad after stopping (GCs it immediately).")
@click.pass_context
def kill(ctx, my_runs, user, run_id, job_id, prefix, purge):
    echo = ctx.obj.echo
    flow_name_ctx = ctx.obj.flow.name if hasattr(ctx.obj, 'flow') else None

    client = NomadClient()
    jobs_to_kill = list(job_id) # Start with explicitly provided job IDs

    if not jobs_to_kill and not prefix and not run_id and not my_runs and not user:
        echo("No jobs specified to kill. Use --job-id, --prefix, --run-id, --my-runs, or --user.", err=True)
        return

    # Logic to find jobs based on user, run_id, prefix (similar to list, but then add to jobs_to_kill)
    # This part needs careful implementation of job discovery.
    # For now, if prefix or run_id is given, we list and then kill.

    discovery_prefix = None
    if prefix:
        discovery_prefix = prefix
    elif run_id and flow_name_ctx:
        discovery_prefix = f"mf-{flow_name_ctx}-{run_id}-"
    # `my_runs` and `user` based discovery is complex without metadata and is omitted for now for kill.

    if discovery_prefix:
        echo(f"Discovering jobs with prefix '{discovery_prefix}' to kill...")
        try:
            discovered_jobs = client.list_jobs(prefix=discovery_prefix)
            for j_sum in discovered_jobs:
                if j_sum['Status'] not in ['dead', 'complete']: # Only kill running/pending jobs
                    if j_sum['ID'] not in jobs_to_kill:
                        jobs_to_kill.append(j_sum['ID'])
            echo(f"Found {len(discovered_jobs)} jobs with prefix, {len(jobs_to_kill)} unique jobs targeted.")
        except NomadException as e:
            echo(f"Error discovering jobs with prefix '{discovery_prefix}': {e}", err=True)
            # Continue if some jobs were specified by --job-id

    if not jobs_to_kill:
        echo("No matching jobs found to kill.", err=True)
        return

    for jid in jobs_to_kill:
        echo(f"Attempting to stop Nomad job: {jid} (Purge: {purge})")
        try:
            client.stop_job(jid, purge=purge)
            echo(f"Successfully requested stop for job {jid}.", fg="green")
        except NomadException as e:
            echo(f"Failed to stop job {jid}: {e}", err=True, fg="red")
        except Exception as e:
            echo(f"Generic error stopping job {jid}: {e}", err=True, fg="red")
            traceback.print_exc()

# Need to import util for dict_to_cli_options
from metaflow import util
import time # for list command formatting time
