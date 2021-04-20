from collections import defaultdict, deque
import random
import select
import sys
import time
import hashlib

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import AWS_SANDBOX_ENABLED

class BatchClient(object):
    def __init__(self):
        from ..aws_client import get_aws_client
        self._client = get_aws_client('batch')

    def active_job_queues(self):
        paginator = self._client.get_paginator('describe_job_queues')
        return (
            queue['jobQueueName']
            for page in paginator.paginate()
            for queue in page['jobQueues']
            if queue['state'] == 'ENABLED' and queue['status'] == 'VALID'
        )

    def unfinished_jobs(self):
        queues = self.active_job_queues()
        return (
            job
            for queue in queues
            for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']
            for page in self._client.get_paginator('list_jobs').paginate(
                jobQueue=queue, jobStatus=status
            )
            for job in page['jobSummaryList']
        )

    def describe_jobs(self, job_ids):
        for jobIds in [job_ids[i:i+100] for i in range(0, len(job_ids), 100)]:
            for jobs in self._client.describe_jobs(jobs=jobIds)['jobs']:
                yield jobs

    def describe_job_queue(self, job_queue):
        paginator = self._client.get_paginator('describe_job_queues').paginate(
            jobQueues=[job_queue], maxResults=1)
        return paginator.paginate()['jobQueues'][0] 

    def job(self):
        return BatchJob(self._client)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()

    def region(self):
        return self._client._client_config.region_name


class BatchJobException(MetaflowException):
    headline = 'AWS Batch job error'


class BatchJob(object):
    def __init__(self, client):
        self._client = client
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def execute(self):
        if self._image is None:
            raise BatchJobException(
                'Unable to launch AWS Batch job. No docker image specified.'
            )
        if self._iam_role is None:
            raise BatchJobException(
                'Unable to launch AWS Batch job. No IAM role specified.'
            )
        if 'jobDefinition' not in self.payload:
            self.payload['jobDefinition'] = \
                self._register_job_definition(self._image,
                                              self._iam_role,
                                              self.payload['job_queue'],
                                              self._execution_role,
                                              self._shared_memory,
                                              self._max_swap,
                                              self._swappiness)
        response = self._client.submit_job(**self.payload)
        job = RunningJob(response['jobId'], self._client)
        return job.update()

    def _register_job_definition(self,
                                 image,
                                 job_role,
                                 job_queue,
                                 execution_role,
                                 shared_memory,
                                 max_swap,
                                 swappiness):
        # identify platform from any compute environment associated with the
        # queue
        if AWS_SANDBOX_ENABLED:
            # within the Metaflow sandbox, we can't execute the
            # describe_job_queues directive for AWS Batch to detect compute
            # environment platform, so let's just default to EC2 for now.
            platform = "EC2"
        else:
            response = self._client.describe_job_queues(jobQueues=[job_queue])
            if len(response['jobQueues']) == 0:
                raise BatchJobException(
                    'AWS Batch Job Queue %s not found.' % job_queue)
            compute_environment = response['jobQueues'][0] \
                                    ['computeEnvironmentOrder'][0] \
                                    ['computeEnvironment']
            response = self._client.describe_compute_environments(
                computeEnvironments=[compute_environment])
            platform = response['computeEnvironments'][0] \
                            ['computeResources']['type']

        # compose job definition
        job_definition = {
            'type': 'container',
            'containerProperties': {
                'image': image,
                'jobRoleArn': job_role,
                'command': ['echo', 'hello world'],
                'resourceRequirements': [
                    {
                        'value': '1',
                        'type': 'VCPU'
                    },
                    {
                        'value': '4096',
                        'type': 'MEMORY'
                    }
                ]
            }
        }

        if platform == 'FARGATE' or platform == 'FARGATE_SPOT':
            if execution_role is None:
                raise BatchJobException(
                    'No AWS Fargate task execution IAM role found. Please see '
                    'https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html '
                    'and set the role as METAFLOW_ECS_FARGATE_EXECUTION_ROLE '
                    'environment variable.')
            job_definition['containerProperties']['executionRoleArn'] = \
                execution_role
            job_definition['platformCapabilities'] = ['FARGATE']
            job_definition['containerProperties']['networkConfiguration'] = \
                {'assignPublicIp': 'ENABLED'}
        
        if platform == 'EC2' or platform == 'SPOT':
            if 'linuxParameters' not in job_definition['containerProperties']:
                job_definition['containerProperties']['linuxParameters'] = {}
            if shared_memory is not None:
                if not (isinstance(shared_memory, (int, unicode, basestring)) and 
                    int(shared_memory) > 0):
                    raise BatchJobException(
                        'Invalid shared memory size value ({}); '
                        'it should be greater than 0'.format(shared_memory))
                else:
                    job_definition['containerProperties'] \
                        ['linuxParameters']['sharedMemorySize'] = int(shared_memory)
            if swappiness is not None: 
                if not (isinstance(swappiness, (int, unicode, basestring)) and 
                    int(swappiness) >= 0 and int(swappiness) < 100):
                    raise BatchJobException(
                        'Invalid swappiness value ({}); '
                        '(should be 0 or greater and less than 100)'.format(swappiness))
                else:
                    job_definition['containerProperties'] \
                        ['linuxParameters']['swappiness'] = int(swappiness)
            if max_swap is not None: 
                if not (isinstance(max_swap, (int, unicode, basestring)) and 
                    int(max_swap) >= 0):
                    raise BatchJobException(
                        'Invalid swappiness value ({}); '
                        '(should be 0 or greater)'.format(max_swap))
                else:
                    job_definition['containerProperties'] \
                        ['linuxParameters']['maxSwap'] = int(max_swap)

        # check if job definition already exists
        def_name = 'metaflow_%s' % \
            hashlib.sha224(str(job_definition).encode('utf-8')).hexdigest()
        payload = {'jobDefinitionName': def_name, 'status': 'ACTIVE'}
        response = self._client.describe_job_definitions(**payload)
        if len(response['jobDefinitions']) > 0:
            return response['jobDefinitions'][0]['jobDefinitionArn']

        # else create a job definition
        job_definition['jobDefinitionName'] = def_name
        try:
            response = self._client.register_job_definition(**job_definition)
        except Exception as ex:
            if type(ex).__name__ == 'ParamValidationError' and \
                    (platform == 'FARGATE' or platform == 'FARGATE_SPOT'):
                raise BatchJobException(
                    '%s \nPlease ensure you have installed boto3>=1.16.29 if '
                    'you intend to launch AWS Batch jobs on AWS Fargate '
                    'compute platform.' % ex)
            else:
                raise ex
        return response['jobDefinitionArn']

    def job_def(self,
                image,
                iam_role,
                job_queue,
                execution_role,
                shared_memory,
                max_swap,
                swappiness):
        self.payload['jobDefinition'] = \
            self._register_job_definition(image,
                                          iam_role,
                                          job_queue,
                                          execution_role,
                                          shared_memory,
                                          max_swap,
                                          swappiness)
        return self

    def job_name(self, job_name):
        self.payload['jobName'] = job_name
        return self

    def job_queue(self, job_queue):
        self.payload['jobQueue'] = job_queue
        return self

    def image(self, image):
        self._image = image
        return self

    def iam_role(self, iam_role):
        self._iam_role = iam_role
        return self

    def execution_role(self, execution_role):
        self._execution_role = execution_role
        return self

    def shared_memory(self, shared_memory):
        self._shared_memory = shared_memory
        return self

    def max_swap(self, max_swap):
        self._max_swap = max_swap
        return self

    def swappiness(self, swappiness):
        self._swappiness = swappiness
        return self

    def command(self, command):
        if 'command' not in self.payload['containerOverrides']:
            self.payload['containerOverrides']['command'] = []
        self.payload['containerOverrides']['command'].extend(command)
        return self

    def cpu(self, cpu):
        if not (isinstance(cpu, (int, unicode, basestring, float)) and float(cpu) > 0):
            raise BatchJobException(
                'Invalid CPU value ({}); it should be greater than 0'.format(cpu))
        if 'resourceRequirements' not in self.payload['containerOverrides']:
            self.payload['containerOverrides']['resourceRequirements'] = []
        self.payload['containerOverrides']['resourceRequirements'].append(
            {'value' : str(cpu), 'type': 'VCPU'}
        )
        return self

    def memory(self, mem):
        if not (isinstance(mem, (int, unicode, basestring)) and int(mem) > 0):
            raise BatchJobException(
                'Invalid memory value ({}); it should be greater than 0'.format(mem))
        if 'resourceRequirements' not in self.payload['containerOverrides']:
            self.payload['containerOverrides']['resourceRequirements'] = []
        self.payload['containerOverrides']['resourceRequirements'].append(
            {'value' : str(mem), 'type': 'MEMORY'}
        )
        return self

    def gpu(self, gpu):
        if not (isinstance(gpu, (int, unicode, basestring))):
            raise BatchJobException(
                'invalid gpu value: ({}) (should be 0 or greater)'.format(gpu))
        if int(gpu) > 0:
            if 'resourceRequirements' not in self.payload['containerOverrides']:
                self.payload['containerOverrides']['resourceRequirements'] = []
            self.payload['containerOverrides']['resourceRequirements'].append(
                {'type': 'GPU', 'value': str(gpu)}
            )
        return self

    def environment_variable(self, name, value):
        if 'environment' not in self.payload['containerOverrides']:
            self.payload['containerOverrides']['environment'] = []
        value = str(value)
        if value.startswith("$$.") or value.startswith("$."):
            # Context Object substitution for AWS Step Functions
            # https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html
            self.payload['containerOverrides']['environment'].append(
                {'name': name, 'value.$': value}
            )
        else:
            self.payload['containerOverrides']['environment'].append(
                {'name': name, 'value': value}
            )
        return self

    def timeout_in_secs(self, timeout_in_secs):
        self.payload['timeout']['attemptDurationSeconds'] = timeout_in_secs
        return self

    def parameter(self, key, value):
        self.payload['parameters'][key] = str(value)
        return self

    def attempts(self, attempts):
        self.payload['retryStrategy']['attempts'] = attempts
        return self

class Throttle(object):
    def __init__(self, delta_in_secs=1, num_tries=20):
        self.delta_in_secs = delta_in_secs
        self.num_tries = num_tries
        self._now = None
        self._reset()

    def _reset(self):
        self._tries_left = self.num_tries
        self._wait = self.delta_in_secs

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            now = time.time()
            if self._now is None or (now - self._now > self._wait):
                self._now = now
                try:
                    func(*args, **kwargs)
                    self._reset()
                except TriableException as ex:
                    self._tries_left -= 1
                    if self._tries_left == 0:
                        raise ex.ex
                    self._wait = (self.delta_in_secs*1.2)**(self.num_tries-self._tries_left) + \
                        random.randint(0, 3*self.delta_in_secs)
        return wrapped

class TriableException(Exception):
    def __init__(self, ex):
        self.ex = ex

class RunningJob(object):

    NUM_RETRIES = 8

    def __init__(self, id, client):
        self._id = id
        self._client = client
        self._data = {}

    def __repr__(self):
        return '{}(\'{}\')'.format(self.__class__.__name__, self._id)

    def _apply(self, data):
        self._data = data

    @Throttle()
    def _update(self):
        try:
            data = self._client.describe_jobs(jobs=[self._id])
        except self._client.exceptions.ClientError as err:
            code = err.response['ResponseMetadata']['HTTPStatusCode']
            if code == 429 or code >= 500:
                raise TriableException(err)
            raise err
        self._apply(data['jobs'][0])

    def update(self):
        self._update()
        while not self._data:
            self._update()
        return self

    @property
    def id(self):
        return self._id

    @property
    def info(self):
        if not self._data:
            self.update()
        return self._data

    @property
    def job_name(self):
        return self.info['jobName']

    @property
    def job_queue(self):
        return self.info['jobQueue']

    @property
    def status(self):
        if not self.is_done:
            self.update()
        return self.info['status']

    @property
    def status_reason(self):
        return self.info.get('statusReason')

    @property
    def created_at(self):
        return self.info['createdAt']

    @property
    def stopped_at(self):
        return self.info.get('stoppedAt', 0)

    @property
    def is_done(self):
        if self.stopped_at == 0:
            self.update()
        return self.stopped_at > 0

    @property
    def is_running(self):
        return self.status == 'RUNNING'

    @property
    def is_successful(self):
        return self.status == 'SUCCEEDED'

    @property
    def is_crashed(self):
        # TODO: Check statusmessage to find if the job crashed instead of failing
        return self.status == 'FAILED'

    @property
    def reason(self):
        return self.info['container'].get('reason')

    @property
    def status_code(self):
        if not self.is_done:
            self.update()
        return self.info['container'].get('exitCode')

    def wait_for_running(self):
        if not self.is_running and not self.is_done:
            BatchWaiter(self._client).wait_for_running(self.id)

    @property
    def log_stream_name(self):
        return self.info['container'].get('logStreamName')

    def logs(self):
        def get_log_stream(job):
            log_stream_name = job.log_stream_name
            if log_stream_name:
                return BatchLogs('/aws/batch/job', log_stream_name, sleep_on_no_data=1)
            else:
                return None

        log_stream = None
        while True:
            if self.is_running or self.is_done or self.is_crashed:
                log_stream = get_log_stream(self)
                break
            elif not self.is_done:
                self.wait_for_running()

        if log_stream is None:
            return 
        exception = None
        for i in range(self.NUM_RETRIES + 1):
            try:
                check_after_done = 0
                for line in log_stream:
                    if not line:
                        if self.is_done:
                            if check_after_done > 1:
                                return
                            check_after_done += 1
                        else:
                            pass
                    else:
                        i = 0
                        yield line
                return
            except Exception as ex:
                exception = ex
                if self.is_crashed:
                    break
                #sys.stderr.write(repr(ex) + '\n')
                if i < self.NUM_RETRIES:
                    time.sleep(2 ** i + random.randint(0, 5))
        raise BatchJobException(repr(exception))

    def kill(self):
        if not self.is_done:
            self._client.terminate_job(
                jobId=self._id, reason='Metaflow initiated job termination.')
        return self.update()


class BatchWaiter(object):
    def __init__(self, client):
        try:
            from botocore import waiter
        except:
            raise BatchJobException(
                'Could not import module \'botocore\' which '
                'is required for Batch jobs. Install botocore '
                'first.'
            )
        self._client = client
        self._waiter = waiter

    def wait_for_running(self, job_id):
        model = self._waiter.WaiterModel(
            {
                'version': 2,
                'waiters': {
                    'JobRunning': {
                        'delay': 1,
                        'operation': 'DescribeJobs',
                        'description': 'Wait until job starts running',
                        'maxAttempts': 1000000,
                        'acceptors': [
                            {
                                'argument': 'jobs[].status',
                                'expected': 'SUCCEEDED',
                                'matcher': 'pathAll',
                                'state': 'success',
                            },
                            {
                                'argument': 'jobs[].status',
                                'expected': 'FAILED',
                                'matcher': 'pathAny',
                                'state': 'success',
                            },
                            {
                                'argument': 'jobs[].status',
                                'expected': 'RUNNING',
                                'matcher': 'pathAny',
                                'state': 'success',
                            },
                        ],
                    }
                },
            }
        )
        self._waiter.create_waiter_with_client('JobRunning', model, self._client).wait(
            jobs=[job_id]
        )

class BatchLogs(object):
    def __init__(self, group, stream, pos=0, sleep_on_no_data=0):
        from ..aws_client import get_aws_client
        self._client = get_aws_client('logs')
        self._group = group
        self._stream = stream
        self._pos = pos
        self._sleep_on_no_data = sleep_on_no_data
        self._buf = deque()
        self._token = None

    def _get_events(self):
        try:
            if self._token:
                response = self._client.get_log_events(
                    logGroupName=self._group,
                    logStreamName=self._stream,
                    startTime=self._pos,
                    nextToken=self._token,
                    startFromHead=True,
                )
            else:
                response = self._client.get_log_events(
                    logGroupName=self._group,
                    logStreamName=self._stream,
                    startTime=self._pos,
                    startFromHead=True,
                )
            self._token = response['nextForwardToken']
            return response['events']
        except self._client.exceptions.ResourceNotFoundException as e:
            # The logs might be delayed by a bit, so we can simply try
            # again next time.
            return []

    def __iter__(self):
        while True:
            self._fill_buf()
            if len(self._buf) == 0:
                yield ''
                if self._sleep_on_no_data > 0:
                    select.poll().poll(self._sleep_on_no_data * 1000)
            else:
                while self._buf:
                    yield self._buf.popleft()

    def _fill_buf(self):
        events = self._get_events()
        for event in events:
            self._buf.append(event['message'])
            self._pos = event['timestamp']