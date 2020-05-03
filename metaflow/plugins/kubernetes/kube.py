import os
import time
import json
import select
import atexit
import shlex
import time
import warnings
import hashlib



from requests.exceptions import HTTPError
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import BATCH_METADATA_SERVICE_URL, DATATOOLS_S3ROOT, \
    DATASTORE_LOCAL_DIR, DATASTORE_SYSROOT_S3, DEFAULT_METADATA, \
    BATCH_METADATA_SERVICE_HEADERS, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_DEFAULT_REGION
from metaflow import util

from .kube_client import KubeClient


class KubeException(MetaflowException):
    headline = 'Kube error'


class KubeKilledException(MetaflowException):
    headline = 'Kube task killed'


class Kube(object):

    job_type = 'step_execution'

    def __init__(self, metadata, environment,datastore):
        self.metadata = metadata
        self.environment = environment
        self._client = KubeClient()
        self.datastore = datastore
        atexit.register(lambda: self.job.kill()
                        if hasattr(self, 'job') else None)

    # $ This will Generate the Packaged Environment to Run on Kubernetes
    def _command(self, code_package_url, environment, step_name, step_cli):
        cmds = environment.get_package_commands(code_package_url)
        # $ Added this line because its not present in the batch. 
        cmds.extend(["%s -m pip install kubernetes \
                    --user -qqq" % environment._python()])
        cmds.extend(environment.bootstrap_commands(step_name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend(step_cli)
        return shlex.split('/bin/sh -c "%s"' % " && ".join(cmds))

    def _search_jobs(self, flow_name, run_id, user): # $ The Function Works.
        """_search_jobs [Searches the jobs on Kube]
        :rtype: [List[KubeJobSpec]]
        """
        # todo : throw error if there is no flow name
        search_object = {'flow_name': flow_name,'job_type':'step_execution'}
        if run_id is not None:
            search_object['run_id'] = run_id
        if user is not None:
            search_object['user'] = user
        jobs = []
        for job in self._client.unfinished_jobs():
            # $ Use Labels to indentify jobs and thier executions. 
            job_labels = dict(job.labels)
            if set(job_labels.items()).intersection(set(search_object.items())) == set(search_object.items()):
                jobs.append(job)
        return jobs

    def _name_str(self, user, flow_name, run_id, step_name, task_id):
        return '{user}-{flow_name}-{run_id}-{step_name}-{task_id}'.format(
            user=str.lower(user),
            flow_name=str.lower(flow_name),
            run_id=run_id,
            step_name=str.lower(step_name),
            task_id=task_id
            ) 

    def _job_name(self, user, flow_name, run_id, step_name, task_id, retry_count): # $ Name Generated using MD5 Hash because of Length problems. Labels are used instead. 
        # $ Name can only be as Long as 65 Chars. :: https://stackoverflow.com/questions/50412837/kubernetes-label-name-63-character-limit
        curr_name = self._name_str(user, flow_name, run_id, step_name, task_id)
        # $ SHA the CURR Name and 
        curr_name = hashlib.sha224(curr_name.encode()).hexdigest()+'-'+retry_count

        return curr_name

    def list_jobs(self, flow_name, run_id, user, echo): # $ THIS FUNCTION Works
        jobs = self._search_jobs(flow_name, run_id, user)
        if jobs:
            for job in jobs:
                job_name = self._name_str(job.labels['user'],job.labels['flow_name'],job.labels['run_id'],job.labels['step_name'],job.labels['task_id'])+'-'+job.labels['retry_count']
                echo(
                    '{name} [{id}] ({status})'.format(
                        name=job_name, id=job.id, status=job.status
                    )
                )
        else:
            echo('No running Kube jobs found.')

    def kill_jobs(self, flow_name, run_id, user, echo): # $ THIS FUNCTION Works : Effects are unintended. Need to check the WRT the client API. 
        jobs = self._search_jobs(flow_name, run_id, user)
        if jobs:
            for job in jobs:
                try:
                    running_job = self._client.attach_job(job.job_name,job.namespace,dont_update=True)
                    running_job._apply(job._data)
                    job_name = self._name_str(job.labels['user'],job.labels['flow_name'],job.labels['run_id'],job.labels['step_name'],job.labels['task_id'])+'-'+job.labels['retry_count']
                    echo(
                        'Killing Kube job: {name} [{id}]'.format(
                            name=job_name, id=job.id
                        )
                    )
                    running_job.kill()
                except Exception as e:
                    echo(
                        'Failed to terminate Kube job %s %s [%s]'
                        % (job.job_name, job.id, repr(e))
                    )
        else:
            echo('No running Kube jobs found.')

    def launch_job(
        self,
        step_name,
        step_cli,
        code_package_sha,
        code_package_url,
        code_package_ds,
        image,
        cpu=None,
        gpu=None,
        memory=None,
        kube_namespace=None,
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
        job = self._client.job()
        job \
            .job_name(job_name) \
            .command(
                self._command(code_package_url,
                              self.environment, step_name, [step_cli])) \
            .image(image) \
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
            .environment_variable('METAFLOW_DATASTORE_SYSROOT_S3', DATASTORE_SYSROOT_S3) \
            .environment_variable('METAFLOW_DATATOOLS_S3ROOT', DATATOOLS_S3ROOT) \
            .environment_variable('METAFLOW_DEFAULT_DATASTORE', 's3') \
            .environment_variable('AWS_ACCESS_KEY_ID', AWS_ACCESS_KEY_ID) \
            .environment_variable('AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY) \
            .environment_variable('AWS_SESSION_TOKEN', AWS_SESSION_TOKEN) \
            .environment_variable('AWS_DEFAULT_REGION', AWS_DEFAULT_REGION) \
            .meta_data_label('run_id', attrs['metaflow.run_id']) \
            .meta_data_label('step_name', attrs['metaflow.step_name']) \
            .meta_data_label('user', attrs['metaflow.user']) \
            .meta_data_label('flow_name', attrs['metaflow.flow_name']) \
            .meta_data_label('task_id', attrs['metaflow.task_id']) \
            .meta_data_label('retry_count', attrs['metaflow.retry_count']) \
            .meta_data_label('job_type',self.job_type) \
            .namespace(kube_namespace) 
            # $ (TODO) : Set the AWS Keys based Kube Secret references here.
        for name, value in env.items():
            job.environment_variable(name, value)
        for name, value in self.metadata.get_runtime_environment('kube').items():
            job.environment_variable(name, value)
        if attrs:
            for key, value in attrs.items():
                job.parameter(key, value)
        executing_job = job.execute()
        if executing_job is None:
            raise KubeException('Exception Creating Kubenetes Job')
        self.job = executing_job

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
        
        # $ (TODO) : This may have issues. Check this During Time of Execution.
        logs = self.job.logs()

        while True:
            logs, finished, = print_all(logs)
            if finished:
                break
            else:
                select.poll().poll(500)

        while True:
            if self.job.is_done or self.job.is_crashed:
                select.poll().poll(500)
                break

        if self.job.is_crashed:
            if self.job.reason:
                raise KubeException(
                    'Task crashed due to %s .'
                    'This could be a transient error. '
                    'Use @retry to retry.' % self.job.reason
                )
            raise KubeException(
                'Task crashed. '
                'This could be a transient error. '
                'Use @retry to retry.'
            )
        else:
            if self.job.is_running:
                # Kill the job if it is still running by throwing an exception.
                raise KubeException("Task failed!")
            echo(
                self.job.id,
                'Task finished with status %s.' % self.job.status
            )
