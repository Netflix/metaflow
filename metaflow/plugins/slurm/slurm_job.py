import re
from .slurm_client import SlurmClient, _LOAD_SLURM_PREFIX
from .slurm_script import SlurmJobScript


class SlurmJob(object):
    def __init__(self, client: SlurmClient, name, command, loop, **kwargs) -> None:
        self.client = client
        self.name = name
        self.command = command
        self.loop = loop
        self.kwargs = kwargs

    def create_slurm_script(self):
        # TODO: probably add validation checks for some fields that are required..
        # and raise SlurmException
        sbatch_options = {
            "job-name": self.name,
            "output": f"{self.name}/stdout",
            "error": f"{self.name}/stderr",
            "partition": self.kwargs.get("partition"),
            "nodes": self.kwargs.get("nodes"),
            "ntasks": self.kwargs.get("ntasks"),
            "cpus-per-task": self.kwargs.get("cpus_per_task"),
            "mem": self.kwargs.get("memory"),
            "time": self.kwargs.get("run_time_limit"),
        }
        sbatch_options = {k: v for k, v in sbatch_options.items() if v is not None}

        self.slurm_job_script = SlurmJobScript(
            env=self.kwargs.get("environment_variables"), sbatch_options=sbatch_options
        )

        return self

    def environment_variable(self, name, value):
        # Never set to None
        if value is None:
            return self
        self.kwargs["environment_variables"] = dict(
            self.kwargs.get("environment_variables", {}), **{name: value}
        )
        return self

    def create(self):
        return self.create_slurm_script()

    def execute(self):
        cmd_str = self.command[-1].replace("'", '"')
        cmd_str = f"mkdir -p {self.name} && cd {self.name} && " + cmd_str
        cmd = f"bash -c '{cmd_str}'"

        slurm_job_id = self.loop.run_until_complete(
            self.client.submit(
                job_name=self.name,
                slurm_script_contents=self.slurm_job_script.generate_script(
                    command=cmd
                ),
            )
        )
        return RunningJob(
            client=self.client,
            name=self.name,
            loop=self.loop,
            slurm_job_id=slurm_job_id,
        )

    def partition(self, partition):
        self.kwargs["partition"] = partition
        return self

    def nodes(self, nodes):
        self.kwargs["nodes"] = nodes
        return self

    def ntasks(self, ntasks):
        self.kwargs["ntasks"] = ntasks
        return self

    def cpus_per_task(self, cpus_per_task):
        self.kwargs["cpus-per-task"] = cpus_per_task
        return self

    def memory(self, memory):
        self.kwargs["mem"] = memory
        return self

    def run_time_limit(self, run_time_limit):
        self.kwargs["time"] = run_time_limit
        return self


class RunningJob(object):
    def __init__(self, client, name, loop, slurm_job_id):
        self.client = client
        self.name = name
        self.loop = loop
        self.slurm_job_id = slurm_job_id

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self.slurm_job_id)

    @property
    def id(self):
        return self.slurm_job_id

    @property
    def job_name(self):
        return self.name

    async def status_obj(self):
        cmd_scontrol = f"scontrol show job {self.slurm_job_id}"
        proc_verify_scontrol = await self.client.conn.run(
            _LOAD_SLURM_PREFIX + "which scontrol"
        )
        if proc_verify_scontrol.returncode != 0:
            raise RuntimeError("'scontrol' could not be found on the remote machine.")
        cmd_scontrol = _LOAD_SLURM_PREFIX + cmd_scontrol

        proc = await self.client.conn.run(cmd_scontrol)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

        return proc.stdout.strip()

    @property
    def status(self):
        try:
            log_string = self.loop.run_until_complete(self.status_obj())
        except RuntimeError as e:
            if "command not found" in str(e):
                return None
            raise e

        pattern = r"JobState=(\S+)"
        match = re.search(pattern, log_string)
        if match:
            job_state = match.group(1)
            return job_state
        raise RuntimeError(
            f"Couldn't determine status of slurm job with ID: {self.slurm_job_id}"
        )

    @property
    def message(self):
        try:
            log_string = self.loop.run_until_complete(self.status_obj())
        except RuntimeError as e:
            if "command not found" in str(e):
                return None
            raise e

        pattern = r"Reason=(\S+)"
        match = re.search(pattern, log_string)
        if match:
            reason = match.group(1)
            return reason
        raise RuntimeError(
            f"Couldn't determine reason for slurm job with ID: {self.slurm_job_id}"
        )

    # TODO: confirm from full list here: https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES
    @property
    def is_waiting(self):
        return self.status in ["CONFIGURING", "PENDING"]

    @property
    def is_running(self):
        return self.status in ["CONFIGURING", "PENDING", "RUNNING"]

    @property
    def has_failed(self):
        return self.status in [
            "BOOT_FAIL",
            "FAILED",
            "DEADLINE",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
        ]

    @property
    def has_succeeded(self):
        return self.status in ["COMPLETING", "COMPLETED"]

    @property
    def has_finished(self):
        return self.has_succeeded or self.has_failed

    def kill(self):
        if not self.has_finished:
            self.loop.run_until_complete(self.client.terminate_job(self.slurm_job_id))
