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
    headline = 'Batch error'


class BatchKilledException(MetaflowException):
    headline = 'Batch task killed'


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
            regex = '-{flow_name}-{run_id}-'.format(flow_name=flow_name, run_id=run_id)
        else:
            regex = '{user}-{flow_name}-{run_id}-'.format(
                user=user, flow_name=flow_name, run_id=run_id
            )
        jobs = []
        for job in self._client.unfinished_jobs():
            if regex in job['jobName']:
                jobs.append(job)
        return jobs

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count):
        return '{user}-{flow_name}-{run_id}-{step_name}-{task_id}-{retry_count}'.format(
            user=user,
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            retry_count=retry_count,
        )

    def list_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)
        if jobs:
            for job in jobs:
                echo(
                    '{name} [{id}] ({status})'.format(
                        name=job['jobName'], id=job['jobId'], status=job['status']
                    )
                )
        else:
            echo('No running Batch jobs found.')

    def kill_jobs(self, flow_name, run_id, user, echo):
        jobs = self._search_jobs(flow_name, run_id, user)

        if jobs:
            for job in jobs:
                try:
                    self._client.attach_job(job['jobId']).kill()
                    echo(
                        'Killing Batch job: {name} [{id}] ({status})'.format(
                            name=job['jobName'], id=job['jobId'], status=job['status']
                        )
                    )
                except Exception as e:
                    echo(
                        'Failed to terminate Batch job %s [%s]'
                        % (job['jobId'], repr(e))
                    )
        else:
            echo('No running Batch jobs found.')

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
        job_name = self._job_name(
            attrs['metaflow.user'],
            attrs['metaflow.flow_name'],
            attrs['metaflow.run_id'],
            attrs['metaflow.step_name'],
            attrs['metaflow.task_id'],
            attrs['metaflow.retry_count'],
        )
        if queue is None:
            queue = next(self._client.active_job_queues(), None)
            if queue is None:
                raise BatchException(
                    'Unable to launch Batch job. No job queue '
                    ' specified and no valid & enabled queue found.'
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
            .cpu(cpu) \
            .gpu(gpu) \
            .memory(memory) \
            .timeout_in_secs(run_time_limit) \
            .environment_variable('METAFLOW_CODE_SHA', code_package_sha) \
            .environment_variable('METAFLOW_CODE_URL', code_package_url) \
            .environment_variable('METAFLOW_CODE_DS', code_package_ds) \
            .environment_variable('METAFLOW_USER', attrs['metaflow.user']) \
            .environment_variable('METAFLOW_SERVICE_URL', BATCH_METADATA_SERVICE_URL) \
            .environment_variable('METAFLOW_SERVICE_HEADERS', json.dumps(BATCH_METADATA_SERVICE_HEADERS)) \
            .environment_variable('METAFLOW_DATASTORE_SYSROOT_LOCAL', DATASTORE_LOCAL_DIR) \
            .environment_variable('METAFLOW_DATASTORE_SYSROOT_S3', DATASTORE_SYSROOT_S3) \
            .environment_variable('METAFLOW_DATATOOLS_S3ROOT', DATATOOLS_S3ROOT) \
            .environment_variable('METAFLOW_DEFAULT_DATASTORE', 's3') \
            .environment_variable('METAFLOW_DEFAULT_METADATA', DEFAULT_METADATA)
        for name, value in env.items():
            job.environment_variable(name, value)
        for name, value in self.metadata.get_runtime_environment('batch').items():
            job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                job.parameter(key, value)
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
                        self.job.id,
                        'Task is starting (status %s)...' % status
                    )
                    t = time.time()
                if self.job.is_running or self.job.is_done or self.job.is_crashed:
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
            if self.job.reason:
                raise BatchException(
                    'Task crashed due to %s .'
                    'This could be a transient error. '
                    'Use @retry to retry.' % self.job.reason
                )
            raise BatchException(
                'Task crashed. '
                'This could be a transient error. '
                'Use @retry to retry.'
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise BatchException("Task failed!")
            echo(
                self.job.id,
                'Task finished with exit code %s.' % self.job.status_code
            )
