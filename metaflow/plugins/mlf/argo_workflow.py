import io
import os
import sys
from metaflow.util import get_username
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3
from metaflow.exception import MetaflowException
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator


class ArgoException(MetaflowException):
    headline = 'Argo error'


def create_template(name, node, cmds, env, docker_image, node_selector, resources):
    """
    Creates a template to be executed through the DAG task.
    Foreach step is implemented as the 'steps' template which
    require its own 'container' template to execute.
    """
    t = {
        'name': name,
        'inputs': {
            'parameters': [
                {'name': 'input-paths'},
            ]
        },
        'outputs': {
            'parameters': [
                {
                    'name': 'task-id',
                    'value': '{{pod.name}}'
                },
            ]
        },
        'container': {
            'image': docker_image,
            'command': ['/bin/sh'],
            'args': ['-c', cmds],
            'env': env,
        }
    }

    if node_selector:
        t['nodeSelector'] = node_selector
    if resources:
        t['container']['resources'] = {
            'requests': resources,
            'limits': resources.copy()  # prevent creating yaml anchor and link
        }

    if node.is_inside_foreach:
        # main steps template should be named by 'name'
        t['name'] = f'{name}-template'
        steps = {
            'name': name,
            'steps': [
                [{'name': name, 'template': t['name']}]
            ]
        }
        return [t, steps]

    return [t]


def create_resources(decorators):
    resources = {}

    for deco in decorators:
        if isinstance(deco, ResourcesDecorator):
            for key, val in deco.attributes.items():
                if key == 'cpu':
                    val = int(val)

                # argo cluster treats memory as kb
                if key == 'memory':
                    val = str(val) + 'Mi'

                elif key == 'gpu':
                    key = 'nvidia.com/gpu'
                    val = int(val)
                    if val <= 0:
                        continue

                resources[key] = val

            break

    return resources


def create_node_selector(decorators):
    for deco in decorators:
        if 'nodeSelector' in deco.attributes and deco.attributes['nodeSelector']:
            return deco.attributes['nodeSelector']

    return None


def create_dag_task(name, node):
    task = {
        'name': name,
        'template': name,
        'dependencies': [mangle_step_name(d) for d in node.in_funcs],
    }

    paths = ['%s/{{tasks.%s.outputs.parameters.task-id}}' % (p, mangle_step_name(p)) for p in node.in_funcs]
    if paths:
        input_paths = '{{workflow.name}}/'
        if len(paths) > 1:
            input_paths += ':'
        input_paths += ','.join(paths)

        task['arguments'] = {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': input_paths,
                },
            ]
        }
    else:
        task['arguments'] = {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': '{{workflow.name}}/_parameters/0',
                },
            ]
        }

    return task


def mangle_step_name(name):
    "Must consist of alpha-numeric characters or '-'"
    return name.replace('_', '-')


def get_step_docker_image(base_image, flow_decorators, step):
    """
    docker image is inherited from: cmdline -> flow -> step
    Parameters
    ----------
    base_image: specified in cmdline or default
    flow_decorators: argo_base decorator
    step: current step

    Returns
    -------
    name of resulting docker_image
    """
    if 'argo_base' in flow_decorators:
        if flow_decorators['argo_base'].attributes['image']:
            base_image = flow_decorators['argo_base'].attributes['image']

    for step_decorator in step.decorators:
        if 'image' in step_decorator.attributes and step_decorator.attributes['image']:
            base_image = step_decorator.attributes['image']

    return base_image


class ArgoWorkflow:
    def __init__(self,
                 name,
                 flow,
                 graph,
                 code_package,
                 code_package_url,
                 metadata,
                 datastore,
                 environment,
                 event_logger,
                 monitor,
                 image):
        self.name = name
        self.flow = flow
        self.graph = graph
        self.code_package = code_package
        self.code_package_url = code_package_url
        self.metadata = metadata
        self.datastore = datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.image = image
        self._workflow = self._compile()

    def to_yaml(self):
        from ruamel.yaml import YAML
        s = io.StringIO()
        YAML().dump(self._workflow, s)
        return s.getvalue()

    def _compile(self):
        templates = []
        tasks = []
        for name, node in self.graph.nodes.items():
            name = mangle_step_name(name)
            docker_image = get_step_docker_image(self.image, self.flow._flow_decorators, node)
            resources = create_resources(node.decorators)
            node_selector = create_node_selector(node.decorators)
            templates.extend(
                create_template(name, node, self._command(node), self._env(), docker_image, node_selector, resources))
            tasks.append(create_dag_task(name, node))

        templates.append({'name': 'entry', 'dag': {'tasks': tasks}})

        return {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'Workflow',
            'metadata': {
                'generateName': self.name + '-',
                'labels': {
                    'workflows.argoproj.io/archive-strategy': 'false',
                }
            },
            'spec': {
                'entrypoint': 'entry',
                'templates': templates
            }
        }

    def _command(self, node):
        cmds = self.environment.get_package_commands(self.code_package_url)
        cmds.extend(self.environment.bootstrap_commands(node.name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend([self._step_cli(node)])
        return " && ".join(cmds)

    def _step_cli(self, node):
        cmds = []
        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)
        entrypoint = [executable, script_name]

        if node.name == 'start':
            params = entrypoint + [
                '--quiet',
                '--metadata=%s' % self.metadata.TYPE,
                '--environment=%s' % self.environment.TYPE,
                '--datastore=%s' % self.datastore.TYPE,
                '--event-logger=%s' % self.event_logger.logger_type,
                '--monitor=%s' % self.monitor.monitor_type,
                '--no-pylint',
                'init',
                '--run-id {{workflow.name}}',
                '--task-id 0'
            ]
            cmds.append(' '.join(params))

        top_level = [
            '--quiet',
            '--metadata=%s' % self.metadata.TYPE,
            '--environment=%s' % self.environment.TYPE,
            '--datastore=%s' % self.datastore.TYPE,
            '--datastore-root=%s' % self.datastore.datastore_root,
            '--event-logger=%s' % self.event_logger.logger_type,
            '--monitor=%s' % self.monitor.monitor_type,
            '--no-pylint'
        ]

        step = [
            'step',
            node.name,
            '--run-id {{workflow.name}}',
            '--task-id {{pod.name}}',
            '--input-paths {{inputs.parameters.input-paths}}',
        ]

        cmds.append(' '.join(entrypoint + top_level + step))
        return ' && '.join(cmds)

    def _env(self):
        env = {
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'METAFLOW_USER': get_username(),
            'METAFLOW_DATASTORE_SYSROOT_S3': DATASTORE_SYSROOT_S3,
        }
        return [{'name': k, 'value': v} for k, v in env.items()]
