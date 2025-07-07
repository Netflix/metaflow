import json
import os
import time
import uuid

try:
    import nomad
except ImportError:
    # This is to allow metaflow to parse the decorator an CLI even if nomad is not installed.
    # The actual client calls will fail later if nomad is not installed.
    pass

from metaflow.metaflow_config import (
    NOMAD_ADDRESS,        # e.g., "http://localhost:4646"
    NOMAD_REGION,         # Default Nomad region
    NOMAD_NAMESPACE,      # Default Nomad namespace
    NOMAD_TOKEN,          # Nomad ACL token, if any
    NOMAD_CLIENT_CERT,    # Path to client cert
    NOMAD_CLIENT_KEY,     # Path to client key
    NOMAD_CACERT,         # Path to CA cert for verifying server cert
    NOMAD_VERIFY_TLS,     # Whether to verify TLS (True/False)
    SERVICE_INTERNAL_URL, # For metadata service
    SERVICE_HEADERS,      # For metadata service
    DATASTORE_SYSROOT_S3,
    DATASTORE_SYSROOT_AZURE,
    DATASTORE_SYSROOT_GS,
    DEFAULT_METADATA,
    DEFAULT_DATASTORE,
    METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE,
    METAFLOW_CARD_S3ROOT, # Assuming cards could be supported
    # TODO: Add any other necessary configurations from metaflow_config
)
from metaflow.plugins.nomad.nomad_decorator import NomadException # Re-using the exception
from metaflow.mflog import bash_capture_logs, export_mflog_env_vars, BASH_SAVE_LOGS


# Directory for logs within the Nomad allocation's task directory
LOGS_DIR = "mflogs" # Changed from $PWD/.logs to avoid issues with current PWD in container
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class NomadClient(object):
    def __init__(
        self,
        nomad_address=None,
        region=None,
        namespace=None,
        token=None,
        client_cert=None,
        client_key=None,
        cacert=None,
        verify_tls=None,
        timeout=5,
    ):
        self.nomad_address = nomad_address or NOMAD_ADDRESS
        self.region = region or NOMAD_REGION
        self.namespace = namespace or NOMAD_NAMESPACE
        self.token = token or NOMAD_TOKEN
        self.client_cert = client_cert or NOMAD_CLIENT_CERT
        self.client_key = client_key or NOMAD_CLIENT_KEY
        self.cacert = cacert or NOMAD_CACERT
        self.verify_tls = verify_tls if verify_tls is not None else NOMAD_VERIFY_TLS
        self.timeout = timeout

        if not self.nomad_address:
            raise NomadException("Nomad address not specified. Set METAFLOW_NOMAD_ADDRESS or provide via API.")

        secure = self.nomad_address.startswith("https")

        cert_arg = None
        if self.client_cert and self.client_key:
            cert_arg = (self.client_cert, self.client_key)
        elif self.client_cert:
            cert_arg = self.client_cert

        try:
            self.client = nomad.Nomad(
                host=self.nomad_address.replace("http://", "").replace("https://", ""),
                port=4646, # Default, will be parsed from address if specified
                secure=secure,
                region=self.region,
                namespace=self.namespace,
                token=self.token,
                timeout=self.timeout,
                verify=self.cacert if self.cacert else self.verify_tls, # if cacert is given, requests uses it for verify
                cert=cert_arg,
            )
        except NameError:
            raise NomadException("python-nomad library is not installed. Please install it with: pip install python-nomad")
        except Exception as e:
            raise NomadException(f"Failed to initialize Nomad client: {e}")

    def _generate_job_spec(
        self,
        job_name, # Typically flow_name-run_id-step_name-task_id
        task_name, # Typically metaflow-step_name-task
        image,
        command_executable, # python, or from environment
        command_args, # list of arguments for the command
        cpu,
        memory,
        disk,
        datacenters,
        region,
        nomad_namespace, # This is the job's namespace, distinct from client's default
        constraints=None,
        env_vars=None,
        code_package_url=None,
        # TODO: Add parameters for secrets, ports, etc.
    ):
        job_id = f"mf-{job_name}-{uuid.uuid4().hex[:6]}" # Ensure some uniqueness

        task_resources = {
            "CPU": int(cpu), # MHz
            "MemoryMB": int(memory), # MB
        }
        if disk:
             task_resources["DiskMB"] = int(disk) # Requires preemption_config in Nomad and host volumes

        task_group = {
            "Name": f"tg-{task_name}",
            "Count": 1,
            "Tasks": [{
                "Name": task_name,
                "Driver": "docker",
                "Config": {
                    "image": image,
                    "command": command_executable,
                    "args": command_args,
                    # "ports": ["db"], # Example if we need port mapping
                },
                "Env": env_vars or {},
                "Resources": task_resources,
                "LogConfig": { # Basic log rotation
                    "MaxFiles": 3,
                    "MaxFileSizeMB": 10,
                },
                # "Services": [{...}] # If we need service discovery
            }],
            "EphemeralDisk": { # Request disk for the task group
                "SizeMB": int(disk) if disk else 1024, # Default to 1GB if not specified
                "Migrate": False, # Don't migrate ephemeral disk data on updates
            },
            # "RestartPolicy": {...} # Could configure restarts
        }

        if code_package_url:
            task_group["Tasks"][0]["Artifacts"] = [{
                "GetterSource": code_package_url,
                # Download to a directory named after the task_name within the ALLOC dir
                # The command will need to know to look for 'package.tgz' here
                "RelativeDest": f"local/{task_name}/",
            }]
            # The entrypoint script will need to extract this.
            # Example: command_args might start with a script that does:
            # cd local/task_name && tar xzf package.tgz && cd ../.. && python original_command ...

        nomad_job_spec = {
            "Job": {
                "ID": job_id,
                "Name": job_name,
                "Type": "batch", # Metaflow tasks are typically batch jobs
                "Datacenters": datacenters,
                "Region": region or self.region, # Job region can override client default
                "Namespace": nomad_namespace or self.namespace, # Job namespace can override
                "TaskGroups": [task_group],
                # "Meta": {"metaflow_run_id": run_id, ...} # For tagging/metadata
            }
        }

        if constraints:
            nomad_job_spec["Job"]["Constraints"] = constraints

        return nomad_job_spec

    def _get_mflog_command(self, flow_name, run_id, step_name, task_id, attempt, datastore_type, user_code_command):
        """
        Constructs the command to be run inside the Nomad task, including mflog setup.
        """
        mflog_expr = export_mflog_env_vars(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=attempt,
            datastore_type=datastore_type, # self._datastore.TYPE
            stdout_path=STDOUT_PATH, # Relative to ALLOCDIR/task_name/mflogs/
            stderr_path=STDERR_PATH, # Relative to ALLOCDIR/task_name/mflogs/
            # Note: LOGS_DIR here is relative to the task's working directory,
            # which by default is the root of the task's chroot / alloc directory.
        )

        # The user_code_command is expected to be a list: [executable, script_name, top_args..., "step", step_name, step_args...]
        # We need to make sure it's run with bash_capture_logs

        # Assuming user_code_command is a list that can be joined by spaces
        step_expr = bash_capture_logs(" ".join(user_code_command))

        # cmd_str will be executed by /bin/sh -c "cmd_str" by the docker driver by default if command is set and args is a list.
        # We create the log dir inside the task's allocated directory.
        # Nomad sets ALLOC_DIR and NOMAD_TASK_DIR env vars.
        # For Docker, the default workdir is often the root of the image or /
        # Let's assume logs are written to NOMAD_TASK_DIR / LOGS_DIR
        # The paths in mflog_expr (STDOUT_PATH, STDERR_PATH) should be relative to where the command is run,
        # or absolute if mflog_expr handles that.
        # Given mflog_expr sets MF_LOG_STDOUT_PATH & STDERR_PATH, bash_capture_logs should respect them.

        # Command to setup environment and run the Metaflow step
        # The final command is executed as /bin/sh -c "<returned_command_str>"
        # 1. Create the log directory (relative to task's working dir, often NOMAD_TASK_DIR)
        # 2. Export mflog environment variables
        # 3. Execute the actual step command, capturing its output
        # 4. Persist logs using BASH_SAVE_LOGS
        # 5. Exit with the step's exit code.

        # The actual step command (python myflow.py step ...) needs to be prefixed by
        # commands to download and extract the code package.
        # This is handled by the `Artifacts` stanza and the entrypoint logic in `kubernetes.py`
        # We need a similar pre-command here.

        # Let's assume the command_args passed to _generate_job_spec already includes
        # the full entrypoint logic (download, extract, then run).
        # For now, this method just wraps the final command with mflog.

        # The user_code_command is what `step_cli` (e.g. `python flow.py batch step ...`) would be.
        # It needs to be wrapped.

        # This part is tricky because the environment setup (package commands, bootstrap commands)
        # from Kubernetes.py needs to be replicated or adapted.
        # For now, let's assume the `command_args` for `_generate_job_spec` will handle this.
        # This function will just focus on the mflog wrapping.

        # Example from Kubernetes plugin:
        # init_cmds = self._environment.get_package_commands(code_package_url, self._datastore.TYPE)
        # init_expr = " && ".join(init_cmds)
        # step_expr = bash_capture_logs(" && ".join(self._environment.bootstrap_commands(step_name, self._datastore.TYPE) + step_cmds))
        # cmd_str = "true && mkdir -p %s && %s && %s && %s; " % (LOGS_DIR, mflog_expr, init_expr, step_expr)
        # cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # return shlex.split('bash -c "%s"' % cmd_str) -> this is for k8s spec
        # For Nomad, the "command" is the executable, "args" is the list.
        # The docker driver will effectively do: `Config.command Config.args[0] Config.args[1] ...`
        # Or if command is not set, args[0] is command, args[1:] are args.
        # If command is set and args is set, it's `command arg[0] arg[1] ...`
        # If command is `bash` and args is `['-c', 'full_script_string']`, then bash executes the string.

        # Let's assume `user_code_command` is the final python execution line.
        # We need to build a shell script string to pass to `bash -c`.

        # Placeholder for actual environment bootstrap commands
        # These would come from `self.environment.get_package_commands` and `self.environment.bootstrap_commands`
        # For Nomad, artifacts are downloaded to `NOMAD_ALLOC_DIR/local/<task_name>/package.tgz`
        # So, init_expr needs to cd there, extract, then cd back or adjust paths.
        # This logic should be part of the command_args passed to _generate_job_spec.
        # This function is more about the mflog wrapping of the *final* command.

        # This function is likely misnamed or needs to be part of a larger command construction.
        # The command passed to Nomad's "args" should be the full Metaflow CLI invocation.
        # The mflog setup should happen *inside* the container as part of that invocation.
        # The `kubernetes.py` _command method constructs the bash -c string. We need similar.

        # Let's assume `user_code_command` is a list like ["python", "flow.py", "step", "foo", ...]
        # The mflog part needs to wrap this.
        # The Kubernetes plugin does this:
        # mflog_expr = export_mflog_env_vars(...)
        # init_expr = "&&".join(self._environment.get_package_commands(...))
        # step_expr = bash_capture_logs("&&".join(self._environment.bootstrap_commands(...) + step_cmds))
        # cmd_str = "mkdir -p %s && %s && %s && %s" % (LOGS_DIR, mflog_expr, init_expr, step_expr)
        # cmd_str += "; c=$?; %s; exit $c" % BASH_SAVE_LOGS
        # The final command for Nomad task config: command="bash", args=["-c", cmd_str]

        # This function should probably just return the `mflog_expr` and `BASH_SAVE_LOGS`
        # and the caller (_generate_job_spec) integrates it.
        # For now, returning as is, assuming the caller builds the bash -c string.
        return mflog_expr, BASH_SAVE_LOGS


    def launch_job(
        self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        user, # For tagging or environment
        code_package_sha, # For environment
        code_package_url,
        code_package_ds, # Datastore type for package
        step_cli_args, # This is the full ["python", "flow.py", "step", ...]
        docker_image,
        datacenters, # From decorator
        region,      # From decorator
        nomad_namespace_job, # Job-specific namespace from decorator
        cpu, memory, disk,
        constraints=None,
        environment_vars_decorator=None, # User-defined env vars from @environment
        # TODO: Add other necessary params from Kubernetes.launch_job
    ):
        # 1. Construct environment variables for the Nomad task
        env = {}
        # Metaflow standard environment variables
        env["METAFLOW_USER"] = user
        env["METAFLOW_FLOW_NAME"] = flow_name
        env["METAFLOW_RUN_ID"] = run_id
        env["METAFLOW_STEP_NAME"] = step_name
        env["METAFLOW_TASK_ID"] = task_id
        env["METAFLOW_RETRY_COUNT"] = str(attempt)
        env["METAFLOW_CODE_SHA"] = code_package_sha
        env["METAFLOW_CODE_URL"] = code_package_url
        env["METAFLOW_CODE_DS"] = code_package_ds
        env["METAFLOW_SERVICE_URL"] = SERVICE_INTERNAL_URL
        env["METAFLOW_SERVICE_HEADERS"] = json.dumps(SERVICE_HEADERS)
        env["METAFLOW_DATASTORE_SYSROOT_S3"] = DATASTORE_SYSROOT_S3
        env["METAFLOW_DATASTORE_SYSROOT_AZURE"] = DATASTORE_SYSROOT_AZURE
        env["METAFLOW_DATASTORE_SYSROOT_GS"] = DATASTORE_SYSROOT_GS
        env["METAFLOW_DEFAULT_DATASTORE"] = DEFAULT_DATASTORE
        env["METAFLOW_DEFAULT_METADATA"] = DEFAULT_METADATA
        env["METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE"] = METAFLOW_DEFAULT_SECRETS_BACKEND_TYPE
        env["METAFLOW_CARD_S3ROOT"] = METAFLOW_CARD_S3ROOT # If cards are used

        env["METAFLOW_NOMAD_WORKLOAD"] = "1" # Marker for being inside a Nomad task

        # Add user-defined environment variables from @environment decorator
        if environment_vars_decorator:
            env.update(environment_vars_decorator)

        # Mflog variables will be set by the bash -c script using export_mflog_env_vars

        # 2. Construct the command string for bash -c "..."
        # This needs to replicate the logic in Kubernetes._command method.
        # - Create LOGS_DIR (e.g., mflogs in NOMAD_TASK_DIR)
        # - Setup mflog (export_mflog_env_vars)
        # - Setup package (cd to artifact dir, extract) -> This should be part of step_cli_args or entrypoint
        # - Bootstrap environment (bootstrap_commands) -> This should be part of step_cli_args or entrypoint
        # - Execute step_cli_args (wrapped with bash_capture_logs)
        # - Save logs (BASH_SAVE_LOGS)
        # - Exit with task's exit code

        # For now, let's assume step_cli_args (e.g. ["python", "flow.py", ...]) is the final command.
        # The actual environment setup (package download/extract, bootstrap) needs to be
        # part of an entrypoint script that Nomad's docker driver calls, or baked into the step_cli_args.
        # Let's assume step_cli_args[0] is the executable (e.g. python) and step_cli_args[1:] are its arguments.

        # Command structure:
        # bash -c "mkdir -p mflogs && \
        #            export MF_LOG_FOO=bar && \ (from export_mflog_env_vars)
        #            ( (python flow.py step ...) 2> >(tee mflogs/mflog_stderr >&2) 1> >(tee mflogs/mflog_stdout) ) && \ (from bash_capture_logs)
        #            c=$?; bash save_logs_script.sh mflogs/mflog_stdout mflogs/mflog_stderr; exit $c" (from BASH_SAVE_LOGS)

        mflog_expr = export_mflog_env_vars(
            flow_name=flow_name, run_id=run_id, step_name=step_name, task_id=task_id, retry_count=attempt,
            datastore_type=DEFAULT_DATASTORE, # TODO: Get this from flow_datastore more reliably
            stdout_path=STDOUT_PATH, # e.g., mflogs/mflog_stdout
            stderr_path=STDERR_PATH  # e.g., mflogs/mflog_stderr
        )

        # Construct the Metaflow step execution command string (what the user's code will run)
        # step_cli_args is like ['python', 'flow.py', 'batch', 'step', 'step_name', '--run-id', ...]
        # We need to ensure this is correctly quoted if it contains spaces or special characters.
        # For bash -c, it's safer to pass the command as a single string.
        executable = step_cli_args[0]
        args_string = " ".join(f"'{arg}'" for arg in step_cli_args[1:]) # Basic quoting
        full_metaflow_cmd = f"{executable} {args_string}"

        # This is where environment bootstrap (conda/pypi) and code package extraction would happen.
        # For now, assume the docker image has python and metaflow, and code is in via Artifacts.
        # A proper entrypoint script would handle this:
        # 1. cd $NOMAD_ALLOC_DIR/local/$NOMAD_TASK_NAME
        # 2. tar -xzf package.tgz (if package.tgz is the name)
        # 3. cd back or adjust python path
        # This entrypoint script would be the `command` in Nomad task spec.
        # For simplicity here, we assume `full_metaflow_cmd` can run directly after mflog setup.

        # We need to create the LOGS_DIR relative to NOMAD_TASK_DIR (usually the task's cwd)
        # And BASH_SAVE_LOGS needs to know where the S3/Azure/GS datastore is for logs.
        # The environment variables for datastore (e.g., METAFLOW_DATASTORE_SYSROOT_S3) are set in `env`.

        wrapped_step_cmd = bash_capture_logs(full_metaflow_cmd)

        # The command for Nomad task:
        # Using NOMAD_TASK_DIR as the base for logs.
        # Note: LOGS_DIR is relative path "mflogs"
        nomad_task_command_script = f"""
set -e
echo "Creating logs directory: {LOGS_DIR} (relative to $PWD, which should be $NOMAD_TASK_DIR)"
mkdir -p {LOGS_DIR}
echo "Exporting MFLOG variables..."
{mflog_expr}
echo "Executing Metaflow step command..."
{wrapped_step_cmd}
_exit_code=$?
echo "Saving logs..."
{BASH_SAVE_LOGS}
echo "Exiting with code $_exit_code"
exit $_exit_code
"""
        # For Nomad, the driver usually takes `command` (executable) and `args` (list).
        # To run a script string, we use `bash -c script_string`.
        job_spec_command_executable = "/bin/bash"
        job_spec_command_args = ["-c", nomad_task_command_script]

        job_name_for_nomad = f"{flow_name}-{run_id}-{step_name}-{task_id}"
        task_name_for_nomad = f"mf-task-{step_name}"

        job_spec = self._generate_job_spec(
            job_name=job_name_for_nomad,
            task_name=task_name_for_nomad,
            image=docker_image,
            command_executable=job_spec_command_executable,
            command_args=job_spec_command_args,
            cpu=cpu, memory=memory, disk=disk,
            datacenters=datacenters, region=region, nomad_namespace=nomad_namespace_job,
            constraints=constraints,
            env_vars=env,
            code_package_url=code_package_url,
        )

        try:
            # print("Registering Nomad job:", json.dumps(job_spec, indent=4)) # For debugging
            # The first argument to register_job is the job ID for idempotency,
            # but the library example uses a different string ("example") than the job_spec["Job"]["ID"].
            # Let's use the ID from the spec for consistency if the API allows.
            # The python-nomad docs for job.register_job(job_id, job_dict) suggest job_id is just an identifier.
            # Let's use the generated job_id from the spec.
            eval_info = self.client.job.register_job(job_id=job_spec["Job"]["ID"], job=job_spec)
            # Alternative: self.client.jobs.create_job(job_spec) - Need to check which is correct
            # The example `my_nomad.job.register_job("example", job)` suggests the first arg is a placeholder.
            # Let's try with the actual Job ID.
            # eval_info = self.client.jobs.create_job(job_spec) # This seems more standard for "create"

            # The example `my_nomad.job.register_job("example", job)` is likely correct based on docs.
            # The first arg is "job_id (str) – job_id." - this usually means the ID of the job to operate on.
            # For creation, it might be used for idempotency or just be the job name.
            # Let's try with the job_spec["Job"]["ID"].
            # response = self.client.job.register_job(job_spec["Job"]["ID"], job_spec)
            # The example uses "example" as the first arg, and job["Job"]["ID"] as "example" too.
            # Let's use job_spec["Job"]["Name"] as the first argument, which is what the example implies.
            # This first argument is the "job_id" parameter for the API endpoint, not necessarily the one in the payload.
            # Let's assume job_spec["Job"]["ID"] is the canonical ID.

            # Looking at python-nomad source:
            # job.py -> def register_job(self, job_id, job): return self.put(job_id, json=job ...)
            # So job_id is part of the URL. The payload is in `job`.
            # The Job ID in the payload (`job["Job"]["ID"]`) should match the `job_id` in the URL.

            response = self.client.job.register_job(job_spec["Job"]["ID"], job_spec)

            if "EvalID" not in response:
                raise NomadException(f"Failed to register Nomad job. Response: {response}")

            return {
                "job_id": job_spec["Job"]["ID"],
                "eval_id": response["EvalID"],
                "job_spec": job_spec # For potential later use or logging
            }

        except nomad.api.exceptions.NomadApiError as e:
            raise NomadException(f"Nomad API error launching job: {e.nomad_resp.reason} - {e.nomad_resp.text}")
        except Exception as e:
            raise NomadException(f"Error launching Nomad job: {e}")

    def wait_for_job_completion(self, job_id, eval_id, timeout_seconds=3600, poll_interval=10):
        """
        Waits for a Nomad job to complete or fail.
        This needs to monitor allocations associated with the job evaluation.
        Returns True if successful, False if failed or timed out.
        """
        start_time = time.time()
        last_alloc_status = None
        alloc_id_to_monitor = None

        print(f"Waiting for job {job_id} (EvalID: {eval_id}) to complete...")

        while time.time() - start_time < timeout_seconds:
            try:
                if not alloc_id_to_monitor:
                    # First, get allocations for the evaluation
                    # The evaluation tells us about the placement of allocations for the job.
                    evaluation = self.client.evaluation.get_evaluation(eval_id)
                    # evaluation might have "BlockedEval" or "FailedTgAllocs"
                    # A successful eval should lead to allocations.
                    # We need to find the allocation ID created by this evaluation for our job.

                    # Get all allocations for the job_id
                    job_allocs = self.client.job.get_allocations(job_id)
                    if not job_allocs:
                        # print(f"No allocations yet for job {job_id}. Current eval status: {evaluation.get('Status', 'N/A')}")
                        time.sleep(poll_interval)
                        continue

                    # Find the allocation that matches our evaluation or is the latest for the job.
                    # This logic might need refinement based on how evaluations map to allocs.
                    # For a simple batch job, there should be one allocation.
                    # Let's find an allocation that is not in a terminal state yet.
                    relevant_alloc = None
                    for alloc_summary in job_allocs:
                        alloc_detail = self.client.allocation.get_allocation(alloc_summary["ID"])
                        if alloc_detail["EvalID"] == eval_id: #This is the most direct link
                            alloc_id_to_monitor = alloc_detail["ID"]
                            break
                        # Fallback: if no direct match, take the first non-terminal one for this job
                        if alloc_detail["ClientStatus"] not in ["complete", "failed", "lost"]:
                            relevant_alloc = alloc_detail

                    if alloc_id_to_monitor:
                        print(f"Monitoring allocation {alloc_id_to_monitor} for job {job_id}.")
                    elif relevant_alloc:
                        alloc_id_to_monitor = relevant_alloc["ID"]
                        print(f"Monitoring allocation {alloc_id_to_monitor} (latest non-terminal) for job {job_id}.")
                    else:
                        # All allocations might be terminal (e.g. if job failed quickly)
                        # Or no allocs yet tied to this eval.
                        # print(f"Could not find a suitable allocation to monitor for job {job_id}, eval {eval_id}. Checking eval status: {evaluation.get('Status', 'N/A')}")
                        if evaluation.get("Status") == "failed":
                            print(f"Evaluation {eval_id} failed.")
                            return False, None # Failed
                        if evaluation.get("Status") == "complete" and not job_allocs : # Eval complete but no allocs
                            print(f"Evaluation {eval_id} complete but no allocations found for job {job_id}.")
                            # This might mean the job was valid but couldn't be placed.
                            return False, None # Failed
                        time.sleep(poll_interval)
                        continue

                # We have an alloc_id_to_monitor
                alloc = self.client.allocation.get_allocation(alloc_id_to_monitor)
                status = alloc["ClientStatus"]

                if status != last_alloc_status:
                    print(f"Allocation {alloc_id_to_monitor} status: {status}")
                    last_alloc_status = status

                if status == "complete":
                    # Check task states within the allocation
                    # For a single task job, if alloc is complete, task should be too.
                    # TaskStates: {"task_name": {"State": "dead", "Failed": False, "Restarts": 0, ...}}
                    task_state = alloc.get("TaskStates", {}).get(alloc["TaskGroup"], {}).get("State", "unknown") # This path is wrong
                    # The task name is what we defined, e.g., "mf-task-step_name"
                    # Let's find the task name from the job spec.
                    # For now, assume a single task in the group.
                    primary_task_name = None
                    if alloc.get("Job", {}).get("TaskGroups"):
                        primary_task_name = alloc["Job"]["TaskGroups"][0]["Tasks"][0]["Name"]

                    final_task_status = "unknown"
                    if primary_task_name and alloc.get("TaskStates", {}).get(primary_task_name):
                        final_task_status = alloc["TaskStates"][primary_task_name]["State"]
                        if alloc["TaskStates"][primary_task_name]["Failed"]:
                             print(f"Allocation {alloc_id_to_monitor} complete, but task {primary_task_name} failed.")
                             return False, alloc_id_to_monitor # Failed

                    print(f"Allocation {alloc_id_to_monitor} complete. Task '{primary_task_name}' state: {final_task_status}.")
                    return True, alloc_id_to_monitor # Success

                elif status in ["failed", "lost", "cancelled", "expired"]: # Terminal failure states
                    print(f"Allocation {alloc_id_to_monitor} ended with status: {status}.")
                    return False, alloc_id_to_monitor # Failed

            except nomad.api.exceptions.NomadApiError as e:
                if e.nomad_resp.status_code == 404: # Allocation might not exist yet or job failed before alloc
                    print(f"Allocation {alloc_id_to_monitor or eval_id} not found (404). Waiting...")
                    alloc_id_to_monitor = None # Reset to re-evaluate allocations
                else:
                    print(f"Nomad API error while waiting for job {job_id}: {e}")
                    # Don't exit loop immediately, maybe transient
            except Exception as e:
                print(f"Error waiting for job {job_id}: {e}")
                # Don't exit loop immediately

            time.sleep(poll_interval)

        print(f"Timeout waiting for job {job_id} (EvalID: {eval_id}) to complete.")
        return False, alloc_id_to_monitor # Timeout

    def get_logs(self, alloc_id, task_name, log_type="stdout", tail_lines=None, follow=False):
        """
        Retrieves logs for a specific task in an allocation.
        log_type can be "stdout" or "stderr".
        If follow is True, it streams logs. Otherwise, fetches current logs.
        tail_lines to get last N lines (if not following).

        The python-nomad library's client.fs.cat or client.logs.stream might be used.
        client.logs.stream(alloc_id, task_name, type="stdout", follow=True, offset=0, origin="end")
        client.fs.cat(alloc_id, path_to_log_file) where path is like "alloc/logs/task_name.stdout.0"
        """
        if not alloc_id or not task_name:
            raise NomadException("Allocation ID and task name are required to get logs.")

        # Standard log paths in Nomad allocs:
        # NOMAD_ALLOC_DIR/logs/<task_name>.stdout.0
        # NOMAD_ALLOC_DIR/logs/<task_name>.stderr.0
        # The number (0) is the restart index.

        # Using client.logs.stream seems more robust than fs.cat for potentially large/rotated logs.
        # However, mflog saves to specific files (STDOUT_PATH, STDERR_PATH) within LOGS_DIR relative to task dir.
        # These are mflogs/mflog_stdout and mflogs/mflog_stderr
        # So the path inside the alloc would be: alloc_dir/task_name_dir/mflogs/mflog_stdout
        # This is complex. Let's assume for now we want the raw task stdout/stderr from Nomad.

        log_file_name = f"{task_name}.{log_type}.0" # Standard Nomad log file for first run
        # If we want the mflog captured files:
        # mflog_filename = STDERR_FILE if log_type == "stderr" else STDOUT_FILE
        # log_file_path_in_alloc = f"{LOGS_DIR}/{mflog_filename}"
        # This path is relative to the task's working directory.
        # `client.fs.cat` takes path relative to alloc root. So it would be `task_name_dir_in_alloc/LOGS_DIR/mflog_filename`
        # This is getting complicated due to where mflog saves vs where Nomad saves raw stdout/stderr.

        # For now, let's try to get Nomad's raw stdout/stderr for the task.
        try:
            if follow:
                # This will yield log lines. The caller needs to handle iteration.
                return self.client.logs.stream(
                    alloc_id,
                    task_name,
                    type=log_type,
                    follow=True,
                    offset=0, # From start for now
                    origin="start",
                    plain=True # Get raw text
                )
            else:
                # Fetch current logs (non-streaming)
                # The `logs` method on the client object itself seems to be the one.
                # client.logs(alloc_id, task_name, type, plain, offset, job_id, follow, origin)
                log_data = self.client.logs.logs( # Confusingly named method
                    alloc_id,
                    task=task_name,
                    type=log_type,
                    plain=True,
                    # offset=0, # Get all for now
                    # tail is not directly supported by python-nomad's logs() call,
                    # but origin="end" with a negative offset might work if supported by API.
                    # For simplicity, fetch all and let caller tail if needed.
                )
                # log_data is a dict like {'Data': 'log content', 'File': ..., 'Offset': ...}
                # if plain=True, it should just be the string.
                # The documentation for python-nomad's client.logs.logs is sparse.
                # Let's assume it returns the log string directly when plain=True.
                if isinstance(log_data, dict) and "Data" in log_data: # Based on API spec for /client/fs/logs
                    logs_content = log_data["Data"]
                else: # Assuming plain=True returns string directly
                    logs_content = log_data

                if tail_lines and logs_content:
                    return "\n".join(logs_content.splitlines()[-tail_lines:])
                return logs_content

        except nomad.api.exceptions.NomadApiError as e:
            # If log file not found (e.g. task didn't start or write logs), it might 404.
            if e.nomad_resp.status_code == 404:
                return f"Log file for task {task_name} ({log_type}) not found in allocation {alloc_id}."
            raise NomadException(f"Nomad API error getting logs for task {task_name} in alloc {alloc_id}: {e}")
        except Exception as e:
            raise NomadException(f"Error getting logs for task {task_name} in alloc {alloc_id}: {e}")

    def stop_job(self, job_id, purge=False):
        """ Stops (deregisters) a Nomad job. """
        try:
            self.client.job.deregister_job(job_id, purge=purge)
            print(f"Successfully deregistered job {job_id}.")
            return True
        except nomad.api.exceptions.NomadApiError as e:
            if e.nomad_resp.status_code == 404:
                print(f"Job {job_id} not found, nothing to stop.")
                return True # Idempotent
            raise NomadException(f"Nomad API error stopping job {job_id}: {e}")
        except Exception as e:
            raise NomadException(f"Error stopping job {job_id}: {e}")

    def list_jobs(self, prefix=None):
        """ Lists Nomad jobs, optionally filtered by prefix. """
        try:
            # The library's n.jobs returns a Jobs object that can be iterated or accessed by ID.
            # n.jobs.get_jobs(prefix=...)
            job_list = self.client.jobs.get_jobs(prefix=prefix) # Returns a list of job summaries
            return job_list
        except Exception as e:
            raise NomadException(f"Error listing Nomad jobs: {e}")

# Example usage (for testing purposes, not part of the plugin runtime)
if __name__ == "__main__":
    # Configure these based on your Nomad setup
    # Ensure NOMAD_ADDRESS is set in env or pass it here.
    # client = NomadClient(nomad_address="http://localhost:4646", region="global", namespace="default")

    # This main block is for quick testing, real parameters would come from Metaflow runtime.
    print("This is a library file, not meant to be run directly for plugin operation.")
    print("To test, you would typically call these methods from the nomad_cli.py or within a flow.")

    # Example: How the CLI might use it
    # try:
    #     client = NomadClient() # Reads from env by default
    #     # Test listing jobs
    #     print("Listing jobs with prefix 'mf-':")
    #     jobs = client.list_jobs(prefix="mf-")
    #     for job_summary in jobs:
    #         print(f"  ID: {job_summary['ID']}, Name: {job_summary['Name']}, Status: {job_summary['Status']}")
    #
    #     # Placeholder for launching a test job (requires a lot of params)
    #     # launched_job_info = client.launch_job(...)
    #     # if launched_job_info:
    #     #     print(f"Launched job: {launched_job_info['job_id']}, Eval: {launched_job_info['eval_id']}")
    #     #     success, final_alloc_id = client.wait_for_job_completion(launched_job_info['job_id'], launched_job_info['eval_id'])
    #     #     if success:
    #     #         print(f"Job {launched_job_info['job_id']} completed successfully on allocation {final_alloc_id}.")
    #     #         logs = client.get_logs(final_alloc_id, "your_task_name_here", "stdout")
    #     #         print("Logs:\n", logs)
    #     #     else:
    #     #         print(f"Job {launched_job_info['job_id']} failed or timed out on allocation {final_alloc_id}.")
    #     #     client.stop_job(launched_job_info['job_id'])

    # except NomadException as e:
    #     print(f"Nomad related error: {e}")
    # except Exception as e:
    #     print(f"Generic error: {e}")

    pass
