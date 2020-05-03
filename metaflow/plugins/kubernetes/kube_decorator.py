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

from metaflow import util

from .kube import Kube, KubeException
from metaflow.metaflow_config import BATCH_CONTAINER_IMAGE, BATCH_CONTAINER_REGISTRY, KUBE_NAMESPACE
                    

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

class KubeDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute on Kubernetes.

    This decorator indicates that your step should execute on Kubenetes. Note that you can
    apply this decorator automatically to all steps using the ```--with kube``` argument
    when calling run. Step level decorators are overrides and will force a step to execute
    on Batch regardless of the ```--with``` specification.

    To use, annotate your step as follows:
    ```
    @kube
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
    kube_namespace : string 
        Namespace on Kubernetes to execute on. This is different than the metaflow namespace. 
        Namespaces in kubernetes create a way through which cluster level resource limits can be allocated. 
    """
    name = 'kube'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
        'image': None,
        'kube_namespace':None
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(KubeDecorator, self).__init__(attributes, statically_defined)
        if not self.attributes['image']:
            if BATCH_CONTAINER_IMAGE:
                self.attributes['image'] = BATCH_CONTAINER_IMAGE
            else:
                self.attributes['image'] = 'python:%s.%s' % (platform.python_version_tuple()[0],
                    platform.python_version_tuple()[1])
        
        if not self.attributes['kube_namespace']:
            self.attributes['kube_namespace'] = KUBE_NAMESPACE

        if not KubeDecorator._get_registry(self.attributes['image']):
            if BATCH_CONTAINER_REGISTRY:
                self.attributes['image'] = '%s/%s' % (BATCH_CONTAINER_REGISTRY.rstrip('/'), 
                    self.attributes['image'])
        
    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise KubeException('The *@kube* decorator requires --datastore=s3.')

        self.logger = logger
        self.environment = environment
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # we use the larger of @resources and @kube attributes
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = str(max(int(my_val or 0), int(v or 0)))
        self.run_time_limit = get_run_time_limit_for_task(decos)

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
            # Kube anymore. We can execute possible fallback code locally.
            cli_args.commands = ['kube', 'step']
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options['run-time-limit'] = self.run_time_limit
            cli_args.entrypoint[0] = sys.executable

    def task_pre_step(
            self, step_name, ds, meta, run_id, task_id, flow, graph, retry_count, max_retries):
        if meta.TYPE == 'local':
            self.ds_root = ds.root
        else:
            self.ds_root = None

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
