import os
import time
import json
import select
import atexit
import shlex
import time
import warnings

from requests.exceptions import HTTPError
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import BATCH_METADATA_SERVICE_URL, DATATOOLS_S3ROOT, \
    DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_S3, DEFAULT_METADATA, \
    BATCH_METADATA_SERVICE_HEADERS
from metaflow import util

from .batch_client import BatchClient

from metaflow.datastore.util.s3tail import S3Tail
from metaflow.mflog.mflog import refine, set_should_persist
from metaflow.mflog import export_mflog_env_vars,\
                           bash_capture_logs,\
                           update_delay,\
                           BASH_SAVE_LOGS

# Redirect structured logs to /logs/
LOGS_DIR = '/logs'
STDOUT_FILE = 'mflog_stdout'
STDERR_FILE = 'mflog_stderr'
STDOUT_PATH = os.path.join(LOGS_DIR, STDOUT_FILE)
STDERR_PATH = os.path.join(LOGS_DIR, STDERR_FILE)

class BatchException(MetaflowException):
    headline = 'AWS Batch error'


class BatchKilledException(MetaflowException):
    headline = 'AWS Batch task killed'


class Batch(object):
    def __init__(self, metadata, environment):
        self.metadata = metadata
        self.environment = environment
        self._client = BatchClient()
        atexit.register(lambda: self.job.kill() if hasattr(self, 'job') else None)

    def _command(self,
                 environment,
                 code_package_url,
                 step_name,
                 step_cmds,
                 task_spec):
        mflog_expr = export_mflog_env_vars(datastore_type='s3',
                                           stdout_path=STDOUT_PATH,
                                           stderr_path=STDERR_PATH,
                                           **task_spec)
        init_cmds = environment.get_package_commands(code_package_url)
        init_cmds.extend(environment.bootstrap_commands(step_name))
        init_expr = ' && '.join(init_cmds)
        step_expr = bash_capture_logs(' && '.join(step_cmds))

        # construct an entry point that
        # 1) initializes the mflog environment (mflog_expr)
        # 2) bootstraps a metaflow environment (init_expr)
        # 3) executes a task (step_expr)
        cmd_str = 'mkdir -p /logs && %s && %s && %s; ' % \
                        (mflog_expr, init_expr, step_expr)
        # after the task has finished, we save its exit code (fail/success)
        # and persist the final logs. The whole entrypoint should exit
        # with the exit code (c) of the task.
        #
        # Note that if step_expr OOMs, this tail expression is never executed.
        # We lose the last logs in this scenario (although they are visible 
        # still through AWS CloudWatch console).
        cmd_str += 'c=$?; %s; exit $c' % BASH_SAVE_LOGS
        return shlex.split('bash -c \"%s\"' % cmd_str)

    def _search_jobs(self, flow_name, run_id, user):
        if user is None:
            regex = '-{flow_name}-'.format(flow_name=flow_name)
        else:
            regex = '{user}-{flow_name}-'.format(
                user=user, flow_name=flow_name
            )
        jobs = []
        for job in self._client.unfinished_jobs():
            if regex in job['jobName']:
                jobs.append(job['jobId'])
        if run_id is not None:
            run_id = run_id[run_id.startswith('sfn-') and len('sfn-'):]
        for job in self._client.describe_jobs(jobs):
            parameters = job['parameters']
            match = (user is None or parameters['metaflow.user'] == user) and \
                    (parameters['metaflow.flow_name'] == flow_name) and \
                    (run_id is None or parameters['metaflow.run_id'] == run_id)
            if match:
                yield job

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count):
        return '{user}-{flow_name}-{run_id}-{step_name}-{task_id}-{retry_count}'.format(
            user=user,
            flow_name=flow_name,
            run_id=str(run_id) if run_id is not None else '',
            step_name=step_name,
            task_id=str(task_id) if task_id is not None else '',
            retry_count=str(retry_count) if retry_count is not None else ''
        )

    def list_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)
        found = False
        for job in jobs:
            found = True
            echo(
                '{name} [{id}] ({status})'.format(
                    name=job['jobName'], id=job['jobId'], status=job['status']
                )
            )
        if not found:
            echo('No running AWS Batch jobs found.')

    def kill_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)
        found = False
        for job in jobs:
            found = True
            try:
                self._client.attach_job(job['jobId']).kill()
                echo(
                    'Killing AWS Batch job: {name} [{id}] ({status})'.format(
                        name=job['jobName'], id=job['jobId'], status=job['status']
                    )
                )
            except Exception as e:
                echo(
                    'Failed to terminate AWS Batch job %s [%s]'
                    % (job['jobId'], repr(e))
                )
        if not found:
            echo('No running AWS Batch jobs found.')

    def create_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        execution_role=None,
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        env={},
        attrs={}
    ):
        job_name = self._job_name(
            attrs.get('metaflow.user'),
            attrs.get('metaflow.flow_name'),
            attrs.get('metaflow.run_id'),
            attrs.get('metaflow.step_name'),
            attrs.get('metaflow.task_id'),
            attrs.get('metaflow.retry_count')
        )
        job = self._client.job()
        job \
            .job_name(job_name) \
            .job_queue(queue) \
            .command(
                self._command(self.environment, code_package_url,
                              step_name, [step_cli], task_spec)) \
            .image(image) \
            .iam_role(iam_role) \
            .execution_role(execution_role) \
            .job_def(image, iam_role,
                queue, execution_role, shared_memory,
                max_swap, swappiness) \
            .cpu(cpu) \
            .gpu(gpu) \
            .memory(memory) \
            .shared_memory(shared_memory) \
            .max_swap(max_swap) \
            .swappiness(swappiness) \
            .timeout_in_secs(run_time_limit) \
            .environment_variable('AWS_DEFAULT_REGION', self._client.region()) \
            .environment_variable('METAFLOW_CODE_SHA', code_package_sha) \
            .environment_variable('METAFLOW_CODE_URL', code_package_url) \
            .environment_variable('METAFLOW_CODE_DS', code_package_ds) \
            .environment_variable('METAFLOW_USER', attrs['metaflow.user']) \
            .environment_variable('METAFLOW_SERVICE_URL', BATCH_METADATA_SERVICE_URL) \
            .environment_variable('METAFLOW_SERVICE_HEADERS', json.dumps(BATCH_METADATA_SERVICE_HEADERS)) \
            .environment_variable('METAFLOW_DATASTORE_SYSROOT_S3', DATASTORE_SYSROOT_S3) \
            .environment_variable('METAFLOW_DATATOOLS_S3ROOT', DATATOOLS_S3ROOT) \
            .environment_variable('METAFLOW_DEFAULT_DATASTORE', 's3') \
            .environment_variable('METAFLOW_DEFAULT_METADATA', DEFAULT_METADATA)
            # Skip setting METAFLOW_DATASTORE_SYSROOT_LOCAL because metadata sync between the local user 
            # instance and the remote AWS Batch instance assumes metadata is stored in DATASTORE_LOCAL_DIR 
            # on the remote AWS Batch instance; this happens when METAFLOW_DATASTORE_SYSROOT_LOCAL 
            # is NOT set (see get_datastore_root_from_config in datastore/local.py).
        for name, value in env.items():
            job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                job.parameter(key, value)
        return job

    def launch_job(
        self,
        step_name,
        step_cli,
        task_spec,
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        execution_role=None, # for FARGATE compatibility
        cpu=None,
        gpu=None,
        memory=None,
        platform=None,
        run_time_limit=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        env={},
        attrs={},
        ):
        if queue is None:
            queue = next(self._client.active_job_queues(), None)
            if queue is None:
                raise BatchException(
                    'Unable to launch AWS Batch job. No job queue '
                    ' specified and no valid & enabled queue found.'
                )
        job = self.create_job(
                        step_name,
                        step_cli,
                        task_spec,
                        code_package_sha,
                        code_package_url,
                        code_package_ds,
                        image,
                        queue,
                        iam_role,
                        execution_role,
                        cpu,
                        gpu,
                        memory,
                        run_time_limit,
                        shared_memory,
                        max_swap,
                        swappiness,
                        env,
                        attrs
        )
        self.job = job.execute()

    def wait(self, stdout_location, stderr_location, echo=None):
        
        def wait_for_launch(job):
            status = job.status
            echo('Task is starting (status %s)...' % status,
                 'stderr',
                 batch_id=job.id)
            t = time.time()
            while True:
                if status != job.status or (time.time()-t) > 30:
                    status = job.status
                    echo(
                        'Task is starting (status %s)...' % status,
                        'stderr',
                        batch_id=job.id
                    )
                    t = time.time()
                if job.is_running or job.is_done or job.is_crashed:
                    break
                select.poll().poll(200)

        prefix = b'[%s] ' % util.to_bytes(self.job.id)
        
        def _print_available(tail, stream, should_persist=False):
            # print the latest batch of lines from S3Tail
            try:
                for line in tail:
                    if should_persist:
                        line = set_should_persist(line)
                    else:
                        line = refine(line, prefix=prefix)
                    echo(line.strip().decode('utf-8', errors='replace'), stream)
            except Exception as ex:
                echo('[ temporary error in fetching logs: %s ]' % ex,
                     'stderr',
                     batch_id=self.job.id)
        stdout_tail = S3Tail(stdout_location)
        stderr_tail = S3Tail(stderr_location)

        # 1) Loop until the job has started
        wait_for_launch(self.job)

        # 2) Loop until the job has finished
        start_time = time.time()
        is_running = True
        next_log_update = start_time
        log_update_delay = 1

        while is_running:
            if time.time() > next_log_update:
                _print_available(stdout_tail, 'stdout')
                _print_available(stderr_tail, 'stderr')
                now = time.time()
                log_update_delay = update_delay(now - start_time)
                next_log_update = now + log_update_delay
                is_running = self.job.is_running

            # This sleep should never delay log updates. On the other hand,
            # we should exit this loop when the task has finished without
            # a long delay, regardless of the log tailing schedule
            d = min(log_update_delay, 5.0)
            select.poll().poll(d * 1000)
        
        # 3) Fetch remaining logs
        #
        # It is possible that we exit the loop above before all logs have been
        # shown.
        #
        # TODO if we notice AWS Batch failing to upload logs to S3, we can add a
        # HEAD request here to ensure that the file exists prior to calling
        # S3Tail and note the user about truncated logs if it doesn't
        _print_available(stdout_tail, 'stdout')
        _print_available(stderr_tail, 'stderr')
        # In case of hard crashes (OOM), the final save_logs won't happen.
        # We fetch the remaining logs from AWS CloudWatch and persist them to 
        # Amazon S3.
        #
        # TODO: AWS CloudWatch fetch logs

        if self.job.is_crashed:
            msg = next(msg for msg in 
                [self.job.reason, self.job.status_reason, 'Task crashed.']
                 if msg is not None)
            raise BatchException(
                '%s '
                'This could be a transient error. '
                'Use @retry to retry.' % msg
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise BatchException("Task failed!")
            echo(
                'Task finished with exit code %s.' % self.job.status_code,
                'stderr',
                batch_id=self.job.id
            )
