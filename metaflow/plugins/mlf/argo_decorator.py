import platform
import re
from metaflow.datastore.datastore import TransformableObject
from metaflow.decorators import StepDecorator
from .argo_workflow import ArgoException


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


class ArgoDecorator(StepDecorator):
    """
    Decorator for argo workflows
    ```
    @argo
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
        Docker image to use for argo template. If not specified, a default image mapping to
        a base Python/ML container is used
    """
    name = 'argo'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4000',
        'image': None
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(ArgoDecorator, self).__init__(attributes, statically_defined)

        if not self.attributes['image']:
            self.attributes['image'] = 'python:%s.%s-alpine' % (platform.python_version_tuple()[0],
                                                              platform.python_version_tuple()[1])

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if datastore.TYPE != 's3':
            raise ArgoException('The *@argo* decorator requires --datastore=s3.')

        self.logger = logger
        self.environment = environment
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # we use the larger of @resources and @argo attributes  TODO: do we need Resources?
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = str(max(int(my_val or 0), int(v or 0)))

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