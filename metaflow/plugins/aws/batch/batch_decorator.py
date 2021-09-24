import os
import sys
import platform
import re
import tarfile
import requests

from metaflow.decorators import StepDecorator
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata import MetaDatum
from metaflow.metadata.util import sync_local_metadata_to_datastore

from metaflow import util
from metaflow import R

from .batch import BatchException
from metaflow.metaflow_config import ECS_S3_ACCESS_IAM_ROLE, BATCH_JOB_QUEUE, \
                    BATCH_CONTAINER_IMAGE, BATCH_CONTAINER_REGISTRY, \
                    ECS_FARGATE_EXECUTION_ROLE
from metaflow.sidecar import SidecarSubProcess


class BatchDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute on AWS Batch.
    
    This decorator indicates that your step should execute on AWS Batch. Note 
    that you can apply this decorator automatically to all steps using the 
    ```--with batch``` argument when calling run/resume. Step level decorators 
    within the code are overrides and will force a step to execute on AWS Batch
    regardless of the ```--with``` specification.
    
    To use, annotate your step as follows:
    ```
    @batch
    @step
    def my_step(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1. If @resources is
        also present, the maximum value from all decorators is used
    gpu : int
        Number of GPUs required for this step. Defaults to 0. If @resources is
        also present, the maximum value from all decorators is used
    memory : int
        Memory size (in MB) required for this step. Defaults to 4096. If 
        @resources is also present, the maximum value from all decorators is
        used
    image : string
        Docker image to use when launching on AWS Batch. If not specified, a 
        default docker image mapping to the current version of Python is used
    queue : string
        AWS Batch Job Queue to submit the job to. Defaults to the one 
        specified by the environment variable METAFLOW_BATCH_JOB_QUEUE
    iam_role : string
        AWS IAM role that AWS Batch container uses to access AWS cloud resources
        (Amazon S3, Amazon DynamoDb, etc). Defaults to the one specified by the
        environment variable METAFLOW_ECS_S3_ACCESS_IAM_ROLE
    execution_role : string
        AWS IAM role that AWS Batch can use to trigger AWS Fargate tasks. 
        Defaults to the one determined by the environment variable 
        METAFLOW_ECS_FARGATE_EXECUTION_ROLE https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html
    shared_memory : int
        The value for the size (in MiB) of the /dev/shm volume for this step.
        This parameter maps to the --shm-size option to docker run.
    max_swap : int
        The total amount of swap memory (in MiB) a container can use for this 
        step. This parameter is translated to the --memory-swap option to 
        docker run where the value is the sum of the container memory plus the 
        max_swap value.
    swappiness : int
        This allows you to tune memory swappiness behavior for this step.
        A swappiness value of 0 causes swapping not to happen unless absolutely
        necessary. A swappiness value of 100 causes pages to be swapped very
        aggressively. Accepted values are whole numbers between 0 and 100.
    """
    name = 'batch'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4096',
        'image': None,
        'queue': BATCH_JOB_QUEUE,
        'iam_role': ECS_S3_ACCESS_IAM_ROLE,
        'execution_role': ECS_FARGATE_EXECUTION_ROLE,
        'shared_memory': None,
        'max_swap': None,
        'swappiness': None,
        'host_volumes': None,
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

    def step_init(self,
                  flow,
                  graph,
                  step,
                  decos,
                  environment,
                  flow_datastore,
                  logger):
        if flow_datastore.TYPE != 's3':
            raise BatchException('The *@batch* decorator requires --datastore=s3.')

        self.logger = logger
        self.environment = environment
        self.step = step
        self.flow_datastore = flow_datastore
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

    def runtime_task_created(self,
                             task_datastore,
                             task_id,
                             split_index,
                             input_paths,
                             is_cloned,
                             ubf_context):
        if not is_cloned:
            self._save_package_once(self.flow_datastore, self.package)

    def runtime_step_cli(self,
                         cli_args,
                         retry_count,
                         max_user_code_retries,
                         ubf_context):
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
                      task_datastore,
                      metadata,
                      run_id,
                      task_id,
                      flow,
                      graph,
                      retry_count,
                      max_retries,
                      ubf_context,
                      inputs):
        if metadata.TYPE == 'local':
            self.task_datastore = task_datastore
        else:
            self.task_datastore = None
        meta = {}
        meta['aws-batch-job-id'] = os.environ['AWS_BATCH_JOB_ID']
        meta['aws-batch-job-attempt'] = os.environ['AWS_BATCH_JOB_ATTEMPT']
        meta['aws-batch-ce-name'] = os.environ['AWS_BATCH_CE_NAME']
        meta['aws-batch-jq-name'] = os.environ['AWS_BATCH_JQ_NAME']
        meta['aws-batch-execution-env'] = os.environ['AWS_EXECUTION_ENV']

        # Capture AWS Logs metadata. This is best effort only since
        # only V4 of the metadata uri for the ECS container hosts this
        # information and it is quite likely that not all consumers of 
        # Metaflow would be running the container agent compatible with
        # version V4.
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html
        try:
            logs_meta = requests.get(
                            url=os.environ['ECS_CONTAINER_METADATA_URI_V4']) \
                                .json() \
                                .get('LogOptions', {})
            meta['aws-batch-awslogs-group'] = logs_meta.get('awslogs-group')
            meta['aws-batch-awslogs-region'] = logs_meta.get('awslogs-region')
            meta['aws-batch-awslogs-stream'] = logs_meta.get('awslogs-stream')
        except:
            pass

        entries = [MetaDatum(
            field=k, value=v, type=k, tags=["attempt_id:{0}".format(retry_count)])
            for k, v in meta.items()]
        # Register book-keeping metadata for debugging.
        metadata.register_metadata(run_id, step_name, task_id, entries)
        self._save_logs_sidecar = SidecarSubProcess('save_logs_periodically')

    def task_post_step(self,
                       step_name,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):
        if self.task_datastore:
            sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, 
                self.task_datastore)

    def task_exception(self,
                       exception,
                       step_name,
                       flow,
                       graph,
                       retry_count,
                       max_user_code_retries):
        if self.task_datastore:
            sync_local_metadata_to_datastore(DATASTORE_LOCAL_DIR, 
                self.task_datastore)

    def task_finished(self,
                      step_name,
                      flow,
                      graph,
                      is_task_ok,
                      retry_count,
                      max_retries):
        try:
            self._save_logs_sidecar.kill()
        except:
            pass

    @classmethod
    def _save_package_once(cls, flow_datastore, package):
        if cls.package_url is None:
            cls.package_url, cls.package_sha = flow_datastore.save_data(
                [package.blob], len_hint=1)[0]

    @classmethod
    def _get_registry(cls, image):
        """
        Explanation:

            (.+?(?:[:.].+?)\/)? - [GROUP 0] REGISTRY
                .+?                 - A registry must start with at least one character
                (?:[:.].+?)\/       - A registry must have ":" or "." and end with "/"
                ?                   - Make a registry optional
            (.*?)               - [GROUP 1] REPOSITORY
                .*?                 - Get repository name until separator
            (?:[@:])?           - SEPARATOR
                ?:                  - Don't capture separator
                [@:]                - The separator must be either "@" or ":"
                ?                   - The separator is optional
            ((?<=[@:]).*)?      - [GROUP 2] TAG / DIGEST
                (?<=[@:])           - A tag / digest must be preceded by "@" or ":"
                .*                  - Capture rest of tag / digest
                ?                   - A tag / digest is optional

        Examples:

            image
                - None
                - image
                - None
            example/image
                - None
                - example/image
                - None
            example/image:tag
                - None
                - example/image
                - tag
            example.domain.com/example/image:tag
                - example.domain.com/
                - example/image
                - tag
            123.123.123.123:123/example/image:tag
                - 123.123.123.123:123/
                - example/image
                - tag
            example.domain.com/example/image@sha256:45b23dee0
                - example.domain.com/
                - example/image
                - sha256:45b23dee0
        """

        pattern = re.compile(r"^(.+?(?:[:.].+?)\/)?(.*?)(?:[@:])?((?<=[@:]).*)?$")
        registry, repository, tag = pattern.match(image).groups()
        if registry is not None:
            registry = registry.rstrip("/")
        return registry