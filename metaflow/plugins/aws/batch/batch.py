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

    def _command(self, code_package_url, environment, step_name, step_cli):
        cmds = environment.get_package_commands(code_package_url)
        cmds.extend(environment.bootstrap_commands(step_name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend(step_cli)
        return shlex.split('/bin/sh -c "%s"' % " && ".join(cmds))

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
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
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
                self._command(code_package_url,
                              self.environment, step_name, [step_cli])) \
            .image(image) \
            .iam_role(iam_role) \
            .job_def(image, iam_role) \
            .cpu(cpu) \
            .gpu(gpu) \
            .memory(memory) \
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
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        queue,
        iam_role=None,
        cpu=None,
        gpu=None,
        memory=None,
        run_time_limit=None,
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
                        code_package_sha,
                        code_package_url,
                        code_package_ds,
                        image,
                        queue,
                        iam_role,
                        cpu,
                        gpu,
                        memory,
                        run_time_limit,
                        env,
                        attrs
        )
        self.job = job.execute()

    def wait(self, echo=None):
        def wait_for_launch(job):
            status = job.status
            echo(job.id, 'Task is starting (status %s)...' % status)
            t = time.time()
            while True:
                if status != job.status or (time.time()-t) > 30:
                    status = job.status
                    echo(
                        job.id,
                        'Task is starting (status %s)...' % status
                    )
                    t = time.time()
                if job.is_running or job.is_done or job.is_crashed:
                    break
                select.poll().poll(200)

        def print_all(tail):
            for line in tail:
                if line:
                    echo(self.job.id, util.to_unicode(line))
                else:
                    return tail, False
            return tail, True

        wait_for_launch(self.job)
        logs = self.job.logs()
        while True:
            logs, finished, = print_all(logs)
            if finished:
                break
            else:
                select.poll().poll(500)

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
                self.job.id,
                'Task finished with exit code %s.' % self.job.status_code
            )
