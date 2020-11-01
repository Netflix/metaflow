import os
import sys
import platform
import re
import tarfile

from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.datastore import TransformableObject
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata import MetaDatum

from metaflow import util
from metaflow import R

from .batch import Batch, BatchException
from metaflow.metaflow_config import ECS_S3_ACCESS_IAM_ROLE, BATCH_JOB_QUEUE, \
                    BATCH_CONTAINER_IMAGE, BATCH_CONTAINER_REGISTRY

try:
    # python2
    from urlparse import urlparse
except:  # noqa E722
    # python3
    from urllib.parse import urlparse


class ResourcesDecorator(StepDecorator):
    """
    Step decorator to specify the resources needed when executing this step.
    This decorator passes this information along to Batch when requesting resources
    to execute this step.
    This decorator is ignored if the execution of the step does not happen on Batch.
    To use, annotate your step as follows:
    ```
    @resources(cpu=32)
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1
    gpu : int
        Number of GPUs required for this step. Defaults to 0
    memory : int
        Memory size (in MB) required for this step. Defaults to 4000
    """
    name = 'resources'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
    }

class BatchDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute on Batch.
    This decorator indicates that your step should execute on Batch. Note that you can
    apply this decorator automatically to all steps using the ```--with batch``` argument
    when calling run. Step level decorators are overrides and will force a step to execute
    on Batch regardless of the ```--with``` specification.
    To use, annotate your step as follows:
    ```
    @batch
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1. If @resources is also
        present, the maximum value from all decorators is used
    gpu : int
        Number of GPUs required for this step. Defaults to 0. If @resources is also
        present, the maximum value from all decorators is used
    memory : int
        Memory size (in MB) required for this step. Defaults to 4000. If @resources is
        also present, the maximum value from all decorators is used
    image : string
        Image to use when launching on Batch. If not specified, a default image mapping to
        the current version of Python is used
    queue : string
        Queue to submit the job to. Defaults to the one determined by the environment variable
        METAFLOW_BATCH_JOB_QUEUE
    iam_role : string
        IAM role that Batch can use to access S3. Defaults to the one determined by the environment
        variable METAFLOW_ECS_S3_ACCESS_IAM_ROLE
    """
    name = 'batch'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
        'image': None,
        'queue': BATCH_JOB_QUEUE,
        'iam_role': ECS_S3_ACCESS_IAM_ROLE
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(BatchDecorator, self).__init__(attributes, statically_defined)

        if not self.attributes['image']:
            if BATCH_CONTAINER_IMAGE:
                self.attributes['image'] = BATCH_CONTAINER_IMAGE
            else:
                if R.use_r():
                    self.attributes['image'] = R.container_image()
                else:
                    self.attributes['image'] = 'python:%s.%s' % (platform.python_version_tuple()[0],
                        platform.python_version_tuple()[1])
        if not BatchDecorator._get_registry(self.attributes['image']):
            if BATCH_CONTAINER_REGISTRY:
                self.attributes['image'] = '%s/%s' % (BATCH_CONTAINER_REGISTRY.rstrip('/'), 
                    self.attributes['image'])

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise BatchException('The *@batch* decorator requires --datastore=s3.')

        self.logger = logger
        self.environment = environment
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # we use the larger of @resources and @batch attributes
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = str(max(int(my_val or 0), int(v or 0)))
        self.run_time_limit = get_run_time_limit_for_task(decos)
        if self.run_time_limit < 60:
            raise BatchException('The timeout for step *{step}* should be at '
                'least 60 seconds for execution on AWS Batch'.format(step=step))

    def runtime_init(self, flow, graph, package, run_id):
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(
        self, datastore, task_id, split_index, input_paths, is_cloned):
        if not is_cloned:
            self._save_package_once(datastore, self.package)

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries):
        if retry_count <= max_user_code_retries:
            # after all attempts to run the user code have failed, we don't need
            # Batch anymore. We can execute possible fallback code locally.
            cli_args.commands = ['batch', 'step']
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options['run-time-limit'] = self.run_time_limit
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

    def task_pre_step(self,
                      step_name,
                      ds,
                      metadata,
                      run_id,
                      task_id,
                      flow,
                      graph,
                      retry_count,
                      max_retries):
        if metadata.TYPE == 'local':
            self.ds_root = ds.root
        else:
            self.ds_root = None
        meta = {}
        meta['aws-batch-job-id'] = os.environ['AWS_BATCH_JOB_ID']
        meta['aws-batch-job-attempt'] = os.environ['AWS_BATCH_JOB_ATTEMPT']
        meta['aws-batch-ce-name'] = os.environ['AWS_BATCH_CE_NAME']
        meta['aws-batch-jq-name'] = os.environ['AWS_BATCH_JQ_NAME']    
        entries = [MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_retries):
        if self.ds_root:
            # We have a local metadata service so we need to persist it to the datastore.
            # Note that the datastore is *always* s3 (see runtime_task_created function)
            with util.TempDir() as td:
                tar_file_path = os.path.join(td, 'metadata.tgz')
                with tarfile.open(tar_file_path, 'w:gz') as tar:
                    # The local metadata is stored in the local datastore
                    # which, for batch jobs, is always the DATASTORE_LOCAL_DIR
                    tar.add(DATASTORE_LOCAL_DIR)
                # At this point we upload what need to s3
                s3, _ = get_s3_client()
                with open(tar_file_path, 'rb') as f:
                    path = os.path.join(
                        self.ds_root,
                        MetaflowDataStore.filename_with_attempt_prefix(
                            'metadata.tgz', retry_count))
                    url = urlparse(path)
                    s3.upload_fileobj(f, url.netloc, url.path.lstrip('/'))

    @classmethod
    def _save_package_once(cls, datastore, package):
        if cls.package_url is None:
            cls.package_url = datastore.save_data(package.sha, TransformableObject(package.blob))
            cls.package_sha = package.sha

    @classmethod
    def _get_registry(cls, image):
        pattern = re.compile('^(?:([^\/]+)\/)?(?:([^\/]+)\/)?([^@:\/]+)(?:[@:](.+))?$')
        groups = pattern.match(image).groups()
        registry = groups[0]
        namespace = groups[1]
        if not namespace and registry and not re.search(r'[:.]', registry):
            return None
        return registry