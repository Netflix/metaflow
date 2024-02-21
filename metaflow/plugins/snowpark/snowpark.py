import json
import os
import time
from urllib.request import HTTPError, Request, URLError, urlopen

from metaflow import util
from metaflow.exception import MetaflowException
from metaflow.mflog import (
    BASH_SAVE_LOGS,
    bash_capture_logs,
    export_mflog_env_vars,
    tail_logs,
)
from metaflow.plugins.datatools.s3.s3tail import S3Tail


class NvcfException(MetaflowException):
    headline = "NVCF error"


class NvcfKilledException(MetaflowException):
    headline = "NVCF job killed"


# Redirect structured logs to $PWD/.logs/
LOGS_DIR = "$PWD/.logs"
STDOUT_FILE = "mflog_stdout"
STDERR_FILE = "mflog_stderr"
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)


class Nvcf(object):
    def __init__(self, metadata, environment):
        self.metadata = metadata
        self.environment = environment

    def launch_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        function_id,
        env={},
    ):
        mflog_expr = export_mflog_env_vars(
            datastore_type=code_package_ds,
            stdout_path=STDOUT_PATH,
            stderr_path=STDERR_PATH,
            **task_spec,
        )
        init_cmds = self.environment.get_package_commands(
            code_package_url, code_package_ds
        )
        init_expr = " && ".join(init_cmds)
        step_expr = bash_capture_logs(
            " && ".join(
                self.environment.bootstrap_commands(step_name, code_package_ds)
                + [step_cli]
            )
        )

        # construct an entry point that
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)

        cmd_str = "mkdir -p %s && %s && %s && %s; " % (
            LOGS_DIR,
            mflog_expr,
            init_expr,
            step_expr,
        )
        # after the task has finished, we save its exit code (fail/success)
        # and persist the final logs. The whole entrypoint should exit
        # with the exit code (c) of the task.
        #
        # Note that if step_expr OOMs, this tail expression is never executed.
        # We lose the last logs in this scenario.
        cmd_str += "c=$?; %s; exit $c" % BASH_SAVE_LOGS

        # TODO: remove this before committing code
        access_creds = {
            "AWS_ACCESS_KEY_ID": os.environ["NVCF_AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["NVCF_AWS_SECRET_ACCESS_KEY"],
        }
        env.update(access_creds)
        self.job = Job(function_id, 'bash -c "%s"' % cmd_str, env)
        self.job.submit()

    def wait(self, stdout_location, stderr_location, echo=None):
        def wait_for_launch(job):
            status = job.status
            echo(
                "Task status: %s..." % status,
                "stderr",
                _id=job.id,
            )

        prefix = b"[%s] " % util.to_bytes(self.job.id)
        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        # 1) Loop until the job has started
        wait_for_launch(self.job)

        # 2) Tail logs until the job has finished
        tail_logs(
            prefix=prefix,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            echo=echo,
            has_log_updates=lambda: self.job.is_running,
        )

        echo(
            "Task finished with exit code %s." % self.job.result.get("exit_code"),
            "stderr",
            _id=self.job.id,
        )
        if self.job.has_failed:
            raise NvcfException("This could be a transient error. Use @retry to retry.")


class JobStatus(object):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"




nvcf_url = "https://api.nvcf.nvidia.com"
submit_endpoint = f"{nvcf_url}/v2/nvcf/pexec/functions"
result_endpoint = f"{nvcf_url}/v2/nvcf/pexec/status"


class Job(object):
    def __init__(self, function_id, command, env):
        self._function_id = function_id
        self._payload = {"command": command, "env": env}
        self._result = {}

    def submit(self):
        try:
            bearer_token = os.environ["NVCF_BEARER_TOKEN"]
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            }
            request_data = json.dumps(self._payload).encode()
            request = Request(
                f"{submit_endpoint}/{self._function_id}",
                data=request_data,
                headers=headers,
            )
            response = urlopen(request)
            self._invocation_id = response.headers.get("NVCF-REQID")
            if response.getcode() == 200:
                data = json.loads(response.read())
                if data["status"].startswith("Oops"):
                    self._status = JobStatus.FAILED
                else:
                    self._status = JobStatus.SUCCESSFUL
                self._result = data
            elif response.getcode() == 202:
                self._status = JobStatus.SUBMITTED
            else:
                self._status = JobStatus.FAILED
            # TODO: Handle 404s nicely
        except (HTTPError, URLError) as e:
            self._state = JobStatus.FAILED
            raise e

    @property
    def status(self):
        if self._status not in [JobStatus.SUCCESSFUL, JobStatus.FAILED]:
            self._poll()
        return self._status

    @property
    def id(self):
        return self._invocation_id

    @property
    def is_running(self):
        return self.status == JobStatus.SUBMITTED

    @property
    def has_failed(self):
        return self.status == JobStatus.FAILED

    @property
    def result(self):
        return self._result

    def _poll(self):
        try:
            invocation_id = self._invocation_id
            bearer_token = os.environ["NVCF_BEARER_TOKEN"]
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            }
            request = Request(
                f"{result_endpoint}/{self._invocation_id}", headers=headers
            )
            response = urlopen(request)
            if response.getcode() == 200:
                data = json.loads(response.read())
                if data["status"].startswith("Oops"):
                    self._status = JobStatus.FAILED
                else:
                    self._status = JobStatus.SUCCESSFUL
                self._result = data
            elif response.getcode() in [400, 500]:
                self._status = JobStatus.FAILED
        except (HTTPError, URLError) as e:
            print(f"Error occurred while polling for result: {e}")