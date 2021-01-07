import os
import sys
import json

from metaflow.util import get_username
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException
from .argo_client import ArgoClient


def create_template(name, node, cmds, env, docker_image, node_selector, resources):
    """
    Creates a template to be executed through the DAG task.
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

    if node.is_inside_foreach:
        t['inputs']['parameters'].append({'name': 'split-index'})

    if node.type == 'foreach':
        t['outputs']['parameters'].append(
            {
                'name': 'num-splits',
                'valueFrom': {'path': ArgoInternalStepDecorator.splits_file_path}

            }
        )

    if node_selector:
        t['nodeSelector'] = node_selector

    if resources:
        t['container']['resources'] = {
            'requests': resources,
            'limits': resources.copy()  # prevent creating yaml anchor and link
        }

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
        if isinstance(deco, ArgoStepDecorator):
            if 'nodeSelector' in deco.attributes and deco.attributes['nodeSelector']:
                return deco.attributes['nodeSelector']

    return None


def create_dag_task(graph, name, node):
    task = {
        'name': name,
        'template': name,
        'dependencies': [mangle_step_name(d) for d in node.in_funcs],
        'arguments': {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': input_paths(graph, node)
                }
            ]
        }
    }

    if node.is_inside_foreach:
        task['arguments']['parameters'].append({'name': 'split-index', 'value': '{{item}}'})
        task['withParam'] = \
            '{{tasks.%s.outputs.parameters.num-splits}}' % mangle_step_name(node.in_funcs[0])

    return task


def input_paths(graph, node):
    if node.name == 'start':
        return '{{workflow.name}}/_parameters/0'

    elif node.type == 'join':
        if graph[node.split_parents[-1]].type == 'foreach':
            parent_step = node.in_funcs[0]
            parent_dag_task = mangle_step_name(parent_step)
            # {{tasks.TASK.outputs.parameters}} contains a "json" with all TASK's children params (task-ids)
            return '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters}}' % (parent_step, parent_dag_task)
        else:
            parents = ['%s/{{tasks.%s.outputs.parameters.task-id}}' % (p, mangle_step_name(p)) for p in node.in_funcs]
            return '{{workflow.name}}/:%s' % ','.join(parents)

    parent_step = node.in_funcs[0]
    parent_dag_task = mangle_step_name(parent_step)
    return '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}' % (parent_step, parent_dag_task)


def mangle_step_name(name):
    """ Must consist of alpha-numeric characters or '-' """
    return name.replace('_', '-')


def get_step_docker_image(node, base_image, flow_decorators):
    """
    docker image is inherited from: cmdline -> flow -> step
    Parameters
    ----------
    base_image: specified in cmdline or default
    flow_decorators: argo_base decorator
    node: current step

    Returns
    -------
    name of resulting docker_image
    """
    docker_image_name = base_image
    if 'argo_base' in flow_decorators:
        if flow_decorators['argo_base'].attributes['image']:
            docker_image_name = flow_decorators['argo_base'].attributes['image']

    for step_decorator in node.decorators:
        if isinstance(step_decorator, ArgoStepDecorator):  # prevent Batch parameters from overwriting argo parameters
            if 'image' in step_decorator.attributes and step_decorator.attributes['image']:
                docker_image_name = step_decorator.attributes['image']

    return docker_image_name


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

    def to_json(self):
        return json.dumps(self._workflow)

    def deploy(self, auth, namespace):
        client = ArgoClient(auth, namespace)
        try:
            client.create_template(self.name, self._workflow)
        except Exception as e:
            raise ArgoException(str(e))

    @classmethod
    def trigger(cls, auth, namespace, name, parameters):
        workflow = {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'Workflow',
            'metadata': {
                'generateName': name + '-'
            },
            'spec': {
                'arguments': {
                    'parameters': [
                        {'name': n, 'value': json.dumps(v)} for n, v in parameters.items()
                    ]
                },
                'workflowTemplateRef': {
                    'name': name
                }
            }
        }
        client = ArgoClient(auth, namespace)
        try:
            template = client.get_template(name)
        except Exception as e:
            raise ArgoException(str(e))
        if template is None:
            raise ArgoException("The WorkflowTemplate *%s* doesn't exist on "
                                "Argo Workflows. Please deploy your flow first." % name)
        try:
            return client.submit(workflow)
        except Exception as e:
            raise ArgoException(str(e))

    @classmethod
    def list(cls, auth, namespace, name, phases):
        client = ArgoClient(auth, namespace)
        try:
            tmpl = client.get_template(name)
        except Exception as e:
            raise ArgoException(str(e))
        if tmpl is None:
            raise ArgoException("The WorkflowTemplate *%s* doesn't exist "
                                "on Argo Workflows." % name)
        try:
            return client.list_workflows(name + '-', phases)
        except Exception as e:
            raise ArgoException(str(e))

    def _compile(self):
        parameters = self._process_parameters()
        return {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'WorkflowTemplate',
            'metadata': {
                'name': self.name,
                'labels': {
                    'workflows.argoproj.io/archive-strategy': 'false',
                }
            },
            'spec': {
                'entrypoint': 'entry',
                'arguments': {
                    'parameters': parameters
                },
                'templates': self._prepare_templates(parameters),
            }
        }

    def _prepare_templates(self, parameters):
        templates = []
        tasks = []
        for name, node in self.graph.nodes.items():
            name = mangle_step_name(name)
            templates.extend(create_template(name,
                                             node,
                                             self._command(node, parameters),
                                             self._env(),
                                             get_step_docker_image(node, self.image, self.flow._flow_decorators),
                                             create_node_selector(node.decorators),
                                             create_resources(node.decorators)))

            tasks.append(create_dag_task(self.graph, name, node))

        templates.append({'name': 'entry', 'dag': {'tasks': tasks}})
        return templates

    def _command(self, node, parameters):
        cmds = self.environment.get_package_commands(self.code_package_url)
        cmds.extend(self.environment.bootstrap_commands(node.name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend([self._step_cli(node, parameters)])
        return " && ".join(cmds)

    def _process_parameters(self):
        parameters = []
        for var, param in self.flow._get_parameters():
            p = {'name': param.name}
            if 'default' in param.kwargs:
                v = deploy_time_eval(param.kwargs.get('default'))
                p['value'] = json.dumps(v)
            parameters.append(p)
        return parameters

    def _step_cli(self, node, parameters):
        cmds = []
        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)
        entrypoint = [executable, script_name]

        run_id = '{{workflow.name}}'
        task_id = '{{pod.name}}'
        paths = '{{inputs.parameters.input-paths}}'

        if node.name == 'start':
            # We need a separate unique ID for the special _parameters task
            task_id_params = '0'

            params = entrypoint + [
                '--quiet',
                '--metadata=%s' % self.metadata.TYPE,
                '--environment=%s' % self.environment.TYPE,
                '--datastore=%s' % self.datastore.TYPE,
                '--event-logger=%s' % self.event_logger.logger_type,
                '--monitor=%s' % self.monitor.monitor_type,
                '--no-pylint',
                'init',
                '--run-id %s' % run_id,
                '--task-id %s' % task_id_params,
            ]
            params.extend(['--%s={{workflow.parameters.%s}}' %
                           (p['name'], p['name']) for p in parameters])
            cmds.append(' '.join(params))
            paths = '%s/_parameters/%s' % (run_id, task_id_params)

        if node.type == 'join' and self.graph[node.split_parents[-1]].type == 'foreach':
            # convert input-paths from argo aggregation json and save into $METAFLOW_PARENT_PATHS
            # which will be used by --input-paths $METAFLOW_PARENT_PATHS
            export_parent_tasks = 'METAFLOW_PARENT_PATHS=' \
                                  '$(python -m metaflow.plugins.argo.convert_argo_aggregation %s)' % paths
            paths = '$METAFLOW_PARENT_PATHS'

            cmds.append(export_parent_tasks)

        top_level = [
            '--quiet',
            '--metadata=%s' % self.metadata.TYPE,
            '--environment=%s' % self.environment.TYPE,
            '--datastore=%s' % self.datastore.TYPE,
            '--datastore-root=%s' % self.datastore.datastore_root,
            '--event-logger=%s' % self.event_logger.logger_type,
            '--monitor=%s' % self.monitor.monitor_type,
            '--no-pylint',
            '--with=argo_internal'
        ]

        step = [
            'step',
            node.name,
            '--run-id %s' % run_id,
            '--task-id %s' % task_id,
            '--input-paths %s' % paths,
        ]

        if any(self.graph[n].type == 'foreach' for n in node.in_funcs):
            step.append('--split-index {{inputs.parameters.split-index}}')

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
