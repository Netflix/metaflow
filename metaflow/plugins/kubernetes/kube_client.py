from collections import defaultdict, deque
import select
import sys
import time
import hashlib
import datetime

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import get_kubernetes_client,KUBE_NAMESPACE,KUBE_SERVICE_ACCOUNT


MAX_MEMORY = 64*1000
MAX_CPU = 16

    
class KubeClient(object):
    def __init__(self):
        # todo : set the 
        self._client,self._kube_client = get_kubernetes_client()

    def unfinished_jobs(self):
        """unfinished_jobs [Gets the Kube jobs which are unfinished.]
        
        :return: [List with KubeJobSpec Objects]
        :rtype: [List[KubeJobSpec]]
        """
        # $ NAMESPACE Comes FROM ENV VAR. 
        # $ Get the Jobs.
        jobs = self._kube_client.BatchV1Api(self._client).list_namespaced_job(KUBE_NAMESPACE,timeout_seconds=60)
        kube_specs = []
        for job in jobs.items:
            if job.status.active is not None:
                ks = KubeJobSpec(api_client=self._client,kube_client=self._kube_client,job_name=job.metadata.name,namespace=job.metadata.namespace,dont_update=True)
                ks._apply(job)
                kube_specs.append(ks)
        return kube_specs

    def job(self):
        return KubeJob(api_client=self._client,kube_client=self._kube_client)

    def attach_job(self, job_name,namespace,**kwargs):
        job = RunningKubeJob(api_client=self._client,kube_client=self._kube_client,job_name=job_name,namespace=namespace,**kwargs)
        return job.update()


class KubeJobException(MetaflowException):
    headline = 'Kube job error'


class KubeJobSpecException(MetaflowException):
    headline = 'Kube job Specification  Exception'



class KubeJob(object):
    """KubeJob 
        Job Object Created to Set Job Properties like ENV Vars,Etc.
        Consists of the `execute` method which provides `RunningKubeJob` Object. 
        `RunningKubeJob` will help provide the logs and jobs for the current 
        Native runtime. 
    

    :raises KubeJobException: [Upon Failure of `execute` method]
    """
    def __init__(self, api_client=None,kube_client=None):
        try:
            from kubernetes.client.rest import ApiException
        except:
            raise KubeJobException(
                'Could not import module \'kubernetes\' which '
                'is required for Kubernetes batch jobs. Install kubernetes '
                'first.'
            )
        self.API_EXCEPTION = ApiException
        self.gpu_enabled = False
        self._client = api_client
        self._kube_client = kube_client
        self._api_client = self._kube_client.BatchV1Api(self._client)
        self.payload = self._kube_client.V1Job(api_version="batch/v1", kind="Job")
        self.payload.metadata = self._kube_client.V1ObjectMeta()
        self.payload.metadata.labels = dict()
        self.payload.status = self._kube_client.V1JobStatus()
        self.namespace_name = None
        self.name = None
        # self.template = self._kube_client.V1PodTemplate()
        self.template = self._kube_client.V1PodTemplateSpec()
        self.env_list = []
        self.params = []
        self._image = None
        self.container = self._kube_client.V1Container(name='metaflow-job') 
        self.command_value = None # $ Need to figure how to structure this properly. 
        self.container.resources = self._kube_client.V1ResourceRequirements(limits={'cpu':str(MAX_CPU*1000)+"m",'memory':str(MAX_MEMORY)+"Mi"},requests={}) # $ NOTE: Currently Setting Hard Limits. Will Change Later


    def execute(self):
        """execute [Runs the Job and yields a RunningKubeJob object]
        :raises KubeJobException: [Upon failure of submitting Job for Execution]
        :return: RunningKubeJob or None
        :rtype: [RunningKubeJob]
        """
        if self._image is None:
            raise KubeJobException(
                'Unable to launch Kubernetes Job job. No docker image specified.'
            )
        if self.namespace_name is None:
            raise KubeJobException("Unable to launch Kubernetes Job Without Namespace.")

        self.container.image = self._image
        self.container.command = [self.command_value[0]]
        self.container.args = self.command_value[1:]
        self.container.env = self.env_list
        pod_spec = self._kube_client.V1PodSpec(containers=[self.container],restart_policy='Never',service_account_name=KUBE_SERVICE_ACCOUNT)

        if self.gpu_enabled: # When running with GPU's Adding Shared Memory Support.
            pod_spec.tolerations = [self._kube_client.V1Toleration(operator='Exists')]
            # Shared Volumed Based on https://www.ibm.com/blogs/research/2018/12/training-cloud-small-files/
            pod_spec.volumes = [
                self._kube_client.V1Volume(name='shm',empty_dir=self._kube_client.V1EmptyDirVolumeSource(medium='Memory'))   
            ]
            self.container.volume_mounts = [self._kube_client.V1VolumeMount(name='shm',mount_path='/dev/shm')]
        # pod_spec.tolerations = [ self._kube_client.operator ]
        self.template.spec = pod_spec
        self.payload.spec = self._kube_client.V1JobSpec(ttl_seconds_after_finished=100, template=self.template)
        try: 
            api_response = self._api_client.create_namespaced_job(self.namespace_name,body=self.payload)
            # $ Returning from within try to ensure There was correct Response
            job = RunningKubeJob(api_client=self._client,kube_client=self._kube_client,job_name=self.name,namespace=self.namespace_name)
            return job.update()
        except self.API_EXCEPTION as e:
            # $ (TODO) : TEST AND CHECK IF THE EXCEPTION BEING RAISED IS APPROPRIATEDLY CAUGHT
            print(e)
            raise KubeJobException("Exception when calling API: %s\n" % e)
            return None


    def parameter(self,key, value):
        self.params.append({key:value})
        return self

    # $ (TODO) : Need to handle really long Job Names
    def job_name(self, job_name):
        self.payload.metadata.name = job_name
        self.name = job_name 

        return self

    def meta_data_label(self,key,value):
        self.payload.metadata.labels[key] = value
        return self

    def namespace(self,namespace_name):
        self.namespace_name = namespace_name
        return self

    def image(self, image):
        self._image = image
        return self

    def args(self,args):
        if not isinstance(args,list) :
            raise KubeJobException("Invalid Args Type. Needs to be Of Type List but got {}".format(type(args)))
        self.container.args = args
        return self

    def command(self, command):
        if not isinstance(command,list) :
            raise KubeJobException("Invalid Command Type. Needs to be Of Type List but got {}".format(type(command)))
        # self.container.command = command
        self.command_value = command
        return self
    
    def _validate_cpu(self,cpu):
        # $ Allow floating point values for CPU.
        if not (isinstance(cpu, (float, unicode, basestring,int)) and float(cpu) > 0):
            raise KubeJobException(
                'Invalid CPU value ({}); it should be greater than 0'.format(cpu))

    def max_cpu(self,cpu):
        self._validate_cpu(cpu)
        self.container.resources.limits['cpu'] = str(float(cpu)*1000)+"m" 
        return self

    def cpu(self, cpu):
        self._validate_cpu(cpu)
        self.container.resources.requests['cpu'] = str(float(cpu)*1000)+"m" 
        return self

    def _validate_memory(self,mem):
        if not (isinstance(mem, (int, unicode, basestring)) and int(mem) > 0):
            raise KubeJobException(
                'Invalid memory value ({}); it should be greater than 0'.format(mem))

    def max_memory(self,mem):
        self._validate_memory(mem)
        self.container.resources.limits['memory'] = str(mem)+"Mi"
        return self

    def memory(self, mem):
        self._validate_memory(mem)
        self.container.resources.requests['memory'] = str(mem)+"Mi"
        return self

    # $ (TODO) : CONFIGURE GPU RELATED STUFF HERE
    def gpu(self, gpu):
        if not (isinstance(gpu, (int, unicode, basestring))):
            raise KubeJobException(
                'invalid gpu value: ({}) (should be 0 or greater)'.format(gpu))
        if int(gpu) > 0: # This is when the cluster is configured with NVIDIA Plugin. 
            self.container.resources.limits['nvidia.com/gpu'] = int(gpu)
            self.gpu_enabled = True
        return self

    def environment_variable(self, name, value):
        self.env_list.append(self._kube_client.V1EnvVar(name=name,value=value))
        return self

    # $ (TODO) : CHECK JOB CONFIGS TO SEE HOW LONG TO PERSIST A JOB AFTER COMPLETION/FAILURE
    def timeout_in_secs(self, timeout_in_secs):
        # self.
        # self.payload['timeout']['attemptDurationSeconds'] = timeout_in_secs
        return self

class limit(object):
    def __init__(self, delta_in_secs):
        self.delta_in_secs = delta_in_secs
        self._now = None

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            now = time.time()
            if self._now is None or (now - self._now > self.delta_in_secs):
                func(*args, **kwargs)
                self._now = now
        return wrapped


class KubeJobSpec(object):
    """KubeJobSpec 
        The purpose if this class is to bind with Job Related highlevel object will API's of Kubernetes.
        This binds the object to job name and namespace. running the KubeJobSpec().update() will update the object 
        with the latest observations from Kubernetes. The updates are stored in the _data property. We use properties of a 
        job such as 'id','job_name','status','created_at', 'is_done' etc as high level abstractions to what the Object that kubernetes
        api returns. We achieve this using @property decorator.
        
        :parameter api_client [kubernetes.client.ApiClient]
        :parameter kube_client [kubernetes.client]
        :parameter job_name [str],
        :parameter namespace [str] : Kubernetes Namespace comes here. ,
        :parameter dont_update [bool]
            If True: 
                Will not allow any update to object ever via update API. Hence will never hit the bound API. 
                Used In cases where list of objects are fetched in a List API and then this class is Used to wrap a High level hooks into the kubernetes response. 

            If False:
                It Object is bound to Kube API. If there are functions calling the properties which will involve checking the Kube cluster Like is_done, is_running etc. then this is very useful. 
                This is leveraged by the RunningKubeJob Object sets don't update as false. 
        
        :raises KubeJobSpecException: If bound API Fails it will raise Exception. 
    """
    def __init__(self,api_client=None,kube_client=None,job_name=None,namespace=None,dont_update=False):
        super().__init__()
        try:
            from kubernetes.client.rest import ApiException
        except:
            raise KubeJobException(
                'Could not import module \'kubernetes\' which '
                'is required for Kubernetes batch jobs. Install kubernetes '
                'first.'
            )
        self.API_EXCEPTION = ApiException
        self._kube_client = kube_client
        self._client = api_client
        self._batch_api_client = self._kube_client.BatchV1Api(self._client)
        self.name = job_name
        self.updated = False
        self.dont_update = dont_update
        self.job_deleted = False
        self.namespace = namespace
        self._data = {}
        self.update()

    def __repr__(self):
        return '{}(\'{}\')'.format(self.__class__.__name__, self.name)

    def _apply(self, data):
        self._data = data
        self.updated = True

    @limit(1)
    def _update(self):
        if self.dont_update or self.job_deleted:
            return 
        try:
            # $ https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/BatchV1Api.md#read_namespaced_job
            data = self._batch_api_client.read_namespaced_job(self.name,self.namespace)
        except self.API_EXCEPTION as e :
            if self.updated: # $ This is to handle the case when jobs killed via CLI and The runtime is stuck in execution
                if e.status == 404:
                    self.job_deleted = True
                    raise KubeJobSpecException("Job Has been Deleted from the Cluster. Exiting Gracefully.")
                    return 
                raise KubeJobSpecException('Error in read_namespaced_job API %s'%str(e))
            return 
        self._apply(data)

    def update(self):
        self._update()
        return self
    
    @property
    def id(self): 
        return self.info.metadata.uid

    @property
    def info(self):
        return self._data

    @property
    def job_name(self):
        return self.info.metadata.name
    
    @property
    def labels(self):
        return self.info.metadata.labels

    @property
    def status(self):
        if self.is_running:
            return 'RUNNING'
        elif self.is_successful:
            return 'COMPLETED'
        elif self.is_crashed:
            return 'FAILED'
        else:
            return 'UNKNOWN_STATUS' 

    @property
    def status_reason(self):
        return self.reason

    @property
    def created_at(self):
        return self.info.status.start_time

    @property
    def is_done(self):
        if self.info.status.completion_time is None:
            self.update()
        return self.info.status.completion_time is not None

    @property
    def is_running(self):
        if self.info.status.active == 1:
            self.update()
        return self.info.status.active == 1

    @property
    def is_successful(self):
        return self.info.status.succeeded is not None

    @property
    def is_crashed(self):
        # TODO: Check statusmessage to find if the job crashed instead of failing
        return self.info.status.failed is not None

    @property
    def reason(self):
        reason = []
        if self.info.status.conditions is not None:
            for obj in self.info.status.conditions:
                if obj.reason is not None:
                    reason.append(obj.reason)

        return '\n'.join(reason)


class RunningKubeJob(KubeJobSpec):
    """RunningKubeJob 
    Inherits the KubeJobSpec class which provides hooks to the job running on Kubernetes. Created by subproccess/main process via CLI for following:
        - Monitoring the Running job created during exection of each step. 
            - Helps monitor logs and perform post job cleanup tasks. 
        - Listing currently running jobs for a flow from the CLI 
        - Killing currently running job from CLI. 

    :type KubeJobSpec:
    
    Options
    -----
    :dont_update : Inherited from parent 
    """
    NUM_RETRIES = 5

    def __init__(self, api_client=None, kube_client=None, job_name=None, namespace=None, dont_update=False):
        super().__init__(api_client=api_client, kube_client=kube_client, job_name=job_name, namespace=namespace, dont_update=dont_update)

    # $ https://stackoverflow.com/questions/56124320/how-to-get-log-and-describe-of-pods-in-kubernetes-by-python-client
    def logs(self):
        pod_label_selector = "controller-uid=" + self.info.spec.template.metadata.labels.get('controller-uid')
        pods_list = self._kube_client.CoreV1Api(self._client).list_namespaced_pod(self.namespace,label_selector=pod_label_selector, timeout_seconds=10)
        pod_name = pods_list.items[0].metadata.name
        # There is no Need to check if the job is in runnable state as the Job will be runnnig on Kube
        try:
            from kubernetes import watch # $ Done this for soft dependency scoping. 
        except:
            raise KubeJobException(
                'Could not import module \'kubernetes\' which '
                'is required for Kubernetes batch jobs. Install kubernetes '
                'first.'
            )
        watcher = watch.Watch()
        for i in range(self.NUM_RETRIES):
            if self.is_done:
                break
            try:
                check_after_done = 0
                # last_call = time()
                for line in watcher.stream(self._kube_client.CoreV1Api(self._client).read_namespaced_pod_log, name=pod_name, namespace=self.namespace):
                    # start_time = datetime.datetime.now()
                    if not line:
                        if self.is_done:
                            if check_after_done > 1:
                                return
                            check_after_done += 1
                        else:
                            pass
                    else:
                        yield line
                break # Because this is a generator, we want to break out here because this means that we are done printing all logs. 
            except Exception as ex:
                if self.is_crashed:
                    break
                # sys.stderr.write('Except : '+str(i))
                time.sleep(2 ** i)        

    def kill(self):
        if not self.is_done:
            self.delete()
        return self.update()
    
    def delete(self):
        if not self.job_deleted: 
            # referenced from : https://github.com/rundeck-plugins/kubernetes/issues/8#issuecomment-399520970
            self._batch_api_client.delete_namespaced_job(\
                    name=self.name,\
                    namespace=self.namespace,\
                    body=self._kube_client.V1DeleteOptions(api_version='v1', kind="DeleteOptions", propagation_policy="Foreground")
                )