import os
import sys
import platform
import requests

from metaflow import R
from metaflow import util
from metaflow.decorators import StepDecorator
from metaflow.datastore.datastore import TransformableObject
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import ECS_S3_ACCESS_IAM_ROLE, \
                                        BATCH_JOB_QUEUE, \
                                        BATCH_CONTAINER_IMAGE, \
                                        BATCH_CONTAINER_REGISTRY, \
                                        ECS_FARGATE_EXECUTION_ROLE, \
                                        DATASTORE_LOCAL_DIR
from metaflow.plugins import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.sidecar import SidecarSubProcess

from .batch import BatchException
from ..aws_utils import get_docker_registry, sync_metadata_to_S3

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
        'swappiness': None
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(BatchDecorator, self).__init__(attributes, statically_defined)

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes['image']:
            # If metaflow-config specifies a docker image, just use that.
            if BATCH_CONTAINER_IMAGE:
                self.attributes['image'] = BATCH_CONTAINER_IMAGE
            # If metaflow-config doesn't specify a docker image, assign a 
            # default docker image.
            else:
                # Metaflow-R has it's own default docker image (rocker family)
                if R.use_r():
                    self.attributes['image'] = R.container_image()
                # Default to vanilla Python image corresponding to major.minor
                # version of the Python interpreter launching the flow.
                else:
                    self.attributes['image'] = \
                        'python:%s.%s' % (platform.python_version_tuple()[0],
                                            platform.python_version_tuple()[1])
        # Assign docker registry URL for the image.
        if not get_docker_registry(self.attributes['image']):
            if BATCH_CONTAINER_REGISTRY:
                self.attributes['image'] = \
                    '%s/%s' % (BATCH_CONTAINER_REGISTRY.rstrip('/'),
                                    self.attributes['image'])

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self,
                  flow,
                  graph,
                  step,
                  decos,
                  environment,
                  datastore,
                  logger):
        # Executing AWS Batch jobs requires a non-local datastore.
        if datastore.TYPE != 's3':
            raise KubernetesException(
                'The *@kubernetes* decorator requires --datastore=s3.')

        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # We use the larger of @resources and @k8s attributes
                    # TODO: Fix https://github.com/Netflix/metaflow/issues/467
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = \
                                        str(max(int(my_val or 0), int(v or 0)))
        
        # Set run time limit for the AWS Batch job.
        self.run_time_limit = get_run_time_limit_for_task(decos)
        if self.run_time_limit < 60:
            raise BatchException('The timeout for step *{step}* should be at '
                'least 60 seconds for execution on AWS Batch.'.format(step=step))

    def runtime_init(self,
                     flow,
                     graph,
                     package,
                     run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(self,
                             datastore,
                             task_id,
                             split_index,
                             input_paths,
                             is_cloned,
                             ubf_context):
        # To execute the AWS Batch job, the job container needs to have
        # access to the code package. We store the package in the datastore
        # which the pod is able to download as part of it's entrypoint. 
        if not is_cloned and self.package_url is None:
            self.package_url = datastore.save_data(
                                    self.package.sha,
                                    TransformableObject(self.package.blob))
            self.package_sha = self.package.sha

    def runtime_step_cli(self,
                         cli_args,
                         retry_count,
                         max_user_code_retries,
                         ubf_context):
        if retry_count <= max_user_code_retries:
            # after all attempts to run the user code have failed, we don't need
            # to execute on AWS Batch anymore. We can execute possible fallback 
            # code locally.
            cli_args.commands = ['batch', 'step']
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options['run-time-limit'] = self.run_time_limit
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

    def task_pre_step(self,
                      step_name,
                      datastore,
                      metadata,
                      run_id,
                      task_id,
                      flow,
                      graph,
                      retry_count,
                      max_retries,
                      ubf_context):
        # If `local` metadata is configured, we would need to copy task
        # execution metadata from the AWS Batch container to user's
        # local file system after the user code has finished execution. This
        # happens via datastore as a communication bridge.
        self.sync_metadata = metadata.TYPE == 'local'

        self.datastore = datastore

        # task_pre_step may run locally if fallback is activated for @catch 
        # decorator. In that scenario, we skip collecting AWS Batch execution
        # metadata. A rudimentary way to detect non-local execution is to
        # check for the existence of AWS_BATCH_JOB_ID environment variable.

        if 'AWS_BATCH_JOB_ID' in os.environ:
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

            entries = \
                [MetaDatum(field=k, value=v, type=k, tags=[]) 
                    for k, v in meta.items()]
            # Register book-keeping metadata for debugging.
            metadata.register_metadata(run_id, step_name, task_id, entries)
        
            # Start MFLog sidecar to collect task logs.
            self._save_logs_sidecar = SidecarSubProcess('save_logs_periodically')

    def task_finished(self,
                      step_name,
                      flow,
                      graph,
                      is_task_ok,
                      retry_count,
                      max_retries):
        if self.sync_metadata:
            # Note that the datastore is *always* Amazon S3 (see 
            # runtime_task_created function).
            sync_metadata_to_S3(DATASTORE_LOCAL_DIR,
                                self.datastore.root,
                                retry_count)

        try:
            self._save_logs_sidecar.kill()
        except:
            # Best effort kill
            pass