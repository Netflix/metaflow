import json
import os
import platform
import sys

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, METADATA_SERVICE_URL, DEFAULT_METADATA
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from metaflow.plugins.environment_decorator import EnvironmentDecorator
from metaflow.util import get_username, compress_list
from .argo_client import ArgoClient
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException


def dns_name(name):
    """
    Most k8s resource types require a name to be used as
    DNS subdomain name as defined in RFC 1123.
    Hence template names couldn't have '_' (underscore).
    """
    return name.replace('_', '-')


ENTRYPOINT = 'entry'
DAG_PREFIX = 'foreach-'


def parse_env_param(param, err_msg):
    try:
        return json.loads(param) if param else []
    except json.JSONDecodeError as ex:
        raise ArgoException('{}. {}'.format(err_msg, str(ex)))


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
                 image,
                 env,
                 env_from):
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
        self.env = parse_env_param(env, '"env" must be a valid JSON')
        self.env_from = parse_env_param(env_from, '"env-from" must be a valid JSON')
        self.image = image
        self._flow_attributes = self._parse_flow_decorator()
        self._workflow = self._compile()

    def to_json(self):
        return json.dumps(self._workflow, indent=4)

    def deploy(self, auth, namespace):
        client = ArgoClient(auth, namespace)
        try:
            client.create_template(self.name, self._workflow)
        except Exception as e:
            raise ArgoException(str(e))

    @classmethod
    def trigger(cls, auth, namespace, name, parameters):
        name = dns_name(name)
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
        name = dns_name(name)
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

    def _parse_flow_decorator(self):
        if 'argo_base' in self.flow._flow_decorators:
            return self.flow._flow_decorators['argo_base'].attributes
        return {}

    def _compile(self):
        self.parameters = self._parameters()
        return {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'WorkflowTemplate',
            'metadata': {
                'name': dns_name(self.name),
                'labels': self._flow_attributes.get('labels'),
                'annotations': self._flow_attributes.get('annotations'),
            },
            'spec': {
                'entrypoint': ENTRYPOINT,
                'arguments': {
                    'parameters': self.parameters
                },
                'imagePullSecrets': self._flow_attributes.get('imagePullSecrets'),
                'templates': self._generate_templates(),
            }
        }

    def _parameters(self):
        parameters = []
        for _, param in self.flow._get_parameters():
            p = {'name': param.name}
            if 'default' in param.kwargs:
                v = deploy_time_eval(param.kwargs.get('default'))
                p['value'] = json.dumps(v)
            parameters.append(p)
        return parameters

    def _default_image(self):
        if self.image:
            return self.image
        image = self._flow_attributes.get('image')
        if image:
            return image
        return 'python:%s.%s' % platform.python_version_tuple()[:2]

    def _generate_templates(self):
        tasks, nested_dags = self._visit(self.graph['start'])
        dag = {
            'name': ENTRYPOINT,
            'dag': {
                'tasks': tasks
            }
        }

        return [dag] + nested_dags + [self.container_template(self.graph[node]) for node in self.graph.nodes]

    def _visit(self, node, tasks=[], nested_dags=[], exit_node=None):
        """
        Traverse linear nodes.
        Special treatment of split and foreach subgraphs
        Nested Dag is inserted for first child of foreach
        """

        def _linear_or_start_task(node):
            if node.name == 'start':
                return start_task()
            else:
                return linear_task(node)

        if node.type == 'end':
            tasks.append(linear_task(node))

        elif node == exit_node:
            pass  # end recursion

        elif self._is_foreach_first_child(node):
            tasks.append(foreach_task(node))
            join = self.graph[self.graph[node.split_parents[-1]].matching_join]
            # create nested dag and add tasks for foreach block
            nested = nested_dag(node, join)
            if node.type == 'split-and':
                nested_tasks, nested_dags = self._visit_split(node, tasks=[dag_first_task(node)],
                                                              nested_dags=nested_dags, exit_node=join)
            else:
                nested_tasks, nested_dags = self._visit(self.graph[node.out_funcs[0]], tasks=[dag_first_task(node)],
                                                        nested_dags=nested_dags, exit_node=join)

            nested['dag']['tasks'] = nested_tasks
            nested_dags.append(nested)
            # join ends the foreach block
            tasks.append(join_foreach_task(join, node.name))
            # continue with node after join
            tasks, nested_dags = self._visit(self.graph[join.out_funcs[0]], tasks, nested_dags, exit_node)

        elif node.type in ('linear', 'join', 'foreach'):
            tasks.append(_linear_or_start_task(node))
            tasks, nested_dags = self._visit(self.graph[node.out_funcs[0]], tasks, nested_dags, exit_node)

        elif node.type == 'split-and':
            tasks.append(_linear_or_start_task(node))
            tasks, nested_dags = self._visit_split(node, tasks, nested_dags, exit_node)

        else:
            raise MetaflowException('Undefined node type: {} in step: {}'.format(node.type, node.name))

        return tasks, nested_dags

    def _visit_split(self, node, tasks, nested_dags, exit_node):
        join = self.graph[node.matching_join]
        # traverse branches
        for out in node.out_funcs:
            tasks, nested_dags = self._visit(self.graph[out], tasks, nested_dags, exit_node=join)
        # finally continue with join node
        return self._visit(join, tasks, nested_dags, exit_node)

    def _commands(self, node):
        cmds = self.environment.get_package_commands(self.code_package_url)
        cmds.extend(self.environment.bootstrap_commands(node.name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend(self._step_commands(node))
        return cmds

    def _step_commands(self, node):
        cmds = []
        script_name = os.path.basename(sys.argv[0])
        executable = self.environment.executable(node.name)
        entrypoint = [executable, script_name]

        run_id = '{{workflow.name}}'
        task_id = '{{pod.name}}'
        paths = '{{inputs.parameters.input-paths}}'

        if node.name == 'start':
            # We need a separate unique ID for the special _parameters task
            task_id_params = '%s-params' % task_id

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
                           (p['name'], p['name']) for p in self.parameters])
            cmds.append(' '.join(params))
            paths = '%s/_parameters/%s' % (run_id, task_id_params)

        if node.type == 'join' and self.graph[node.split_parents[-1]].type == 'foreach':
            # convert input-paths from argo aggregation json and save into $METAFLOW_PARENT_PATHS
            # which will be used by --input-paths $METAFLOW_PARENT_PATHS
            module = 'metaflow.plugins.argo.convert_argo_aggregation'
            export_parent_tasks = 'METAFLOW_PARENT_PATHS=$(python -m %s %s)' \
                                  % (module, paths)
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
        return cmds

    def _is_foreach_first_child(self, node):
        if node.in_funcs:
            parent = self.graph[node.in_funcs[-1]]
            return node.is_inside_foreach and parent.type == 'foreach'
        return False

    def container_template(self, node):
        """
        Returns an argo container template spec. to execute a step
        """
        attr = parse_step_decorator(node, ArgoStepDecorator)
        env_decorator = parse_step_decorator(node, EnvironmentDecorator)
        res_decorator = parse_step_decorator(node, ResourcesDecorator)

        image = self._default_image()
        if attr.get('image'):
            image = attr['image']
        env, env_from = self._prepare_environment(attr, env_decorator)
        res = self._resources(res_decorator)

        template = {
            'name': dns_name(node.name),
            'metadata': {
                'labels': attr.get('labels'),
                'annotations': attr.get('annotations')
            },
            'inputs': {
                'parameters': [{
                    'name': 'input-paths'
                }],
                'artifacts': attr.get('input_artifacts'),
            },
            'outputs': {
                'parameters': [{
                    'name': 'task-id',
                    'value': '{{pod.name}}'
                }],
                'artifacts': attr.get('output_artifacts')
            },
            'nodeSelector': attr.get('nodeSelector'),
            'container': {
                'image': image,
                'command': ['/bin/sh'],
                'args': ['-c', ' && '.join(self._commands(node))],
                'env': env,
                'envFrom': env_from,
                'resources': {
                    'requests': res,
                    'limits': res
                }
            },
        }

        if self._is_foreach_first_child(node):
            template['inputs']['parameters'].append({
                'name': 'split-index'
            })

        if node.type == 'foreach':
            template['outputs']['parameters'].append({
                'name': 'num-splits',
                'valueFrom': {'path': ArgoInternalStepDecorator.splits_file_path}
            })

        return template

    def _resources(self, attr):
        res = {}
        cpu = attr.get('cpu')
        if cpu:
            res['cpu'] = int(cpu)
        mem = attr.get('memory')
        if mem:
            # argo cluster treats memory as kb
            res['memory'] = str(mem) + 'Mi'
        gpu = attr.get('gpu')
        if gpu:
            res['nvidia.com/gpu'] = int(gpu)
        return res

    def _prepare_environment(self, attr, env_decorator):
        default = {
            'METAFLOW_USER': get_username(),
            'METAFLOW_DATASTORE_SYSROOT_S3': DATASTORE_SYSROOT_S3,
        }
        if METADATA_SERVICE_URL:
            default['METAFLOW_SERVICE_URL'] = METADATA_SERVICE_URL
        if DEFAULT_METADATA:
            default['METAFLOW_DEFAULT_METADATA'] = DEFAULT_METADATA
        # add env vars from @environment decorator if exist
        default.update(env_decorator.get('vars', {}))
        default_env = [{'name': k, 'value': v} for k, v in default.items()]
        env = default_env + self._flow_attributes.get('env', []) + self.env + attr.get('env', [])
        env_from = self._flow_attributes.get('envFrom', []) + self.env_from + attr.get('envFrom', [])
        return env, env_from


def parse_step_decorator(node, deco_type):
    deco = [d for d in node.decorators if isinstance(d, deco_type)]
    return deco[0].attributes if deco else {}


def start_task():
    return {
        'name': 'start',
        'template': 'start',
        'arguments': {
            'parameters': [{
                'name': 'input-paths',
                'value': '{{workflow.name}}/_parameters/0'
            }]
        }
    }


def dag_first_task(node):
    name = dns_name(node.name)
    return {
        'name': name,
        'template': name,
        'arguments': {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': '{{inputs.parameters.input-paths}}'
                },
                {
                    'name': 'split-index',
                    'value': '{{inputs.parameters.split-index}}'
                }
            ]
        }
    }


def foreach_task(node):
    """
    the block, which encapsulates the inner steps of the foreach
    """
    name = dns_name(node.name)
    parent_node = node.in_funcs[-1]
    parent_name = dns_name(parent_node)
    return {
        'name': name,  # this name is displayed in argo UI
        'template': dns_name(DAG_PREFIX + name),
        'dependencies': [parent_name],
        'arguments': {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}' % (parent_node, parent_name)
                },
                {
                    'name': 'split-index',
                    'value': '{{item}}'
                }
            ]
        },
        'withParam': '{{tasks.%s.outputs.parameters.num-splits}}' % parent_name
    }


def join_foreach_task(node, parent_task):
    name = dns_name(node.name)
    parent_step = node.in_funcs[-1]
    parent_task_name = dns_name(parent_task)
    return {
        'name': name,
        'template': name,
        'dependencies': [parent_task_name],
        'arguments': {
            'parameters': [{
                'name': 'input-paths',
                'value': '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters}}' % (parent_step, parent_task_name)
            }]
        }
    }


def linear_task(node):
    name = dns_name(node.name)
    paths = ['{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}' % (n, dns_name(n)) for n in node.in_funcs]
    return {
        'name': name,
        'template': name,
        'dependencies': [dns_name(n) for n in node.in_funcs],
        'arguments': {
            'parameters': [{
                'name': 'input-paths',
                'value': compress_list(paths)
            }]
        }
    }


def nested_dag(node, join):
    last_dag_task = dns_name(join.in_funcs[0])
    return {
        'name': dns_name(DAG_PREFIX + node.name),
        'inputs': {
            'parameters': [
                {
                    'name': 'input-paths'
                },
                {
                    'name': 'split-index'
                }
            ]
        },
        'outputs': {
            'parameters': [
                {
                    'name': 'task-id',
                    'valueFrom': {
                        'parameter': '{{tasks.%s.outputs.parameters.task-id}}' % last_dag_task
                    }
                }
            ]
        },
        'dag': {}
    }
