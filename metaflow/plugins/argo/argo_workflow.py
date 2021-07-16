import json
import os
import platform
import shlex
import sys

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3
from metaflow.metaflow_config import DEFAULT_METADATA, METADATA_SERVICE_URL, METADATA_SERVICE_HEADERS
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from metaflow.plugins.environment_decorator import EnvironmentDecorator
from metaflow.util import get_username, compress_list
from metaflow.mflog import export_mflog_env_vars, bash_capture_logs, BASH_SAVE_LOGS
from metaflow.metaflow_environment import metaflow_version
from .argo_client import ArgoClient
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException

ENTRYPOINT = 'entry'


def dns_name(name):
    """
    Most k8s resource types require a name to be used as
    DNS subdomain name as defined in RFC 1123.
    Hence template names couldn't have '_' (underscore).
    """
    return name.replace('_', '-').lower()


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
                 image_pull_secrets,
                 env,
                 env_from,
                 labels,
                 annotations,
                 max_workers):
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
        self.image_pull_secrets = image_pull_secrets
        self.env = env
        self.env_from = env_from
        self.labels = labels
        self.annotations = annotations
        self.attributes = {
            'labels': {
                'metaflow.workflow_template': name,
            },
            'annotations': {
                'metaflow.owner': get_username(),
                'metaflow.version': metaflow_version.get_version(),
                'metaflow.flow_name': flow.name,
            }
        }
        self.max_workers = max_workers
        self._flow_attributes = self._parse_flow_decorator()
        self._workflow = remove_empty_elements(self._compile())

    def to_json(self):
        return json.dumps(self._workflow, indent=4)

    def deploy(self, auth, k8s_namespace):
        client = ArgoClient(auth, k8s_namespace)
        try:
            client.create_template(self.name, self._workflow)
        except Exception as e:
            raise ArgoException(str(e))

    @classmethod
    def trigger(cls, auth, k8s_namespace, name, parameters):
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
        client = ArgoClient(auth, k8s_namespace)
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
    def list(cls, auth, k8s_namespace, name, phases):
        client = ArgoClient(auth, k8s_namespace)
        try:
            tmpl = client.get_template(name)
        except Exception as e:
            raise ArgoException(str(e))
        if tmpl is None:
            raise ArgoException("The WorkflowTemplate *%s* doesn't exist "
                                "on Argo Workflows." % name)
        try:
            return client.list_workflows(name, phases)

        except Exception as e:
            raise ArgoException(str(e))

    def _parse_flow_decorator(self):
        if 'argo_base' in self.flow._flow_decorators:
            return self.flow._flow_decorators['argo_base'].attributes
        return {}

    def _compile(self):
        self.parameters = self._parameters()
        labels = {
            **self._flow_attributes.get('labels', {}),
            **self.labels,
            **self.attributes['labels'],
        }
        annotations = {
            **self._flow_attributes.get('annotations', {}),
            **self.annotations,
            **self.attributes['annotations'],
        }
        return {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'WorkflowTemplate',
            'metadata': {
                'name': self.name,
                'labels': labels,
                'annotations': annotations,
            },
            'spec': {
                'entrypoint': ENTRYPOINT,
                'workflowMetadata': self.attributes,
                'arguments': {
                    'parameters': self.parameters,
                },
                'imagePullSecrets': self.image_pull_secrets if self.image_pull_secrets \
                                    else self._flow_attributes.get('imagePullSecrets'),
                'parallelism': self.max_workers,
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
        container_templates = [self.container_template(self.graph[node]) for node in self.graph.nodes]
        return [dag] + nested_dags + container_templates

    def _visit(self, node, tasks=[], nested_dags=[], exit_node=None):
        """
        Traverse graph nodes.
        Special treatment of split and foreach subgraphs
        """

        def _linear_or_first_dag_task(node):
            if self._is_foreach_first_child(node):
                return dag_first_task(node)
            elif node.name == 'start':
                return start_task()
            else:
                return linear_task(node)

        if node.type == 'end':
            tasks.append(linear_task(node))

        elif node == exit_node:
            pass  # end recursion

        elif node.type in ('linear', 'join'):
            tasks.append(_linear_or_first_dag_task(node))
            tasks, nested_dags = self._visit(self.graph[node.out_funcs[0]], tasks, nested_dags, exit_node)

        elif node.type == 'split-and':
            tasks.append(_linear_or_first_dag_task(node))
            join = self.graph[node.matching_join]
            # traverse branches
            for out in node.out_funcs:
                tasks, nested_dags = self._visit(self.graph[out], tasks, nested_dags, exit_node=join)
            # finally continue with join node
            tasks, nested_dags = self._visit(join, tasks, nested_dags, exit_node)

        elif node.type == 'foreach':
            tasks.append(_linear_or_first_dag_task(node))
            for_each = foreach_task(node)
            tasks.append(for_each)
            join = self.graph[node.matching_join]
            # create nested dag and add tasks for foreach block
            nested_tasks, nested_dags = self._visit(self.graph[node.out_funcs[0]], tasks=[], nested_dags=nested_dags,
                                                    exit_node=join)
            nested_dags.append(nested_dag(for_each['name'], nested_tasks))
            # join ends the foreach block
            tasks.append(join_foreach_task(join, parent_task=for_each))
            # continue with node after join
            tasks, nested_dags = self._visit(self.graph[join.out_funcs[0]], tasks, nested_dags, exit_node)

        else:
            raise MetaflowException('Undefined node type: {} in step: {}'.format(node.type, node.name))

        return tasks, nested_dags

    def _commands(self, node):
        mflog_expr = export_mflog_env_vars(datastore_type='s3',
                                           stdout_path='/tmp/mflog_stdout',
                                           stderr_path='/tmp/mflog_stderr',
                                           flow_name=self.flow.name,
                                           run_id='{{workflow.name}}',
                                           step_name=node.name,
                                           task_id='{{pod.name}}',
                                           retry_count=0)
        init_cmds = []
        if self.code_package_url:
            init_cmds.extend(self.environment.get_package_commands(self.code_package_url))
        init_cmds.extend(self.environment.bootstrap_commands(node.name))
        init_expr = ' && '.join(init_cmds)
        step_expr = bash_capture_logs(' && '.join(self._step_commands(node)))
        cmd = ['true', mflog_expr, init_expr, step_expr]
        cmd_str =  '%s; c=$?; %s; exit $c' % (' && '.join(c for c in cmd if c), BASH_SAVE_LOGS)
        return shlex.split('bash -c \"%s\"' % cmd_str)

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
        return node.is_inside_foreach and self.graph[node.in_funcs[0]].type == 'foreach'

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
        cmd = self._commands(node)

        metadata = {
            'labels': {
                **attr.get('labels', {}),
                **self.attributes['labels'],
            },
            'annotations': {
                **attr.get('annotations', {}),
                **self.attributes['annotations'],
                **{'metaflow.step_name': node.name},
            },
        }

        template = {
            'name': dns_name(node.name),
            'metadata': metadata,
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
                'command': [cmd[0]],
                'args': cmd[1:],
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
        if DEFAULT_METADATA:
            default['METAFLOW_DEFAULT_METADATA'] = DEFAULT_METADATA
        if METADATA_SERVICE_URL:
            default['METAFLOW_SERVICE_URL'] = METADATA_SERVICE_URL
        if METADATA_SERVICE_HEADERS:
            default['METADATA_SERVICE_HEADERS'] = METADATA_SERVICE_HEADERS
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
    The inner steps of foreach are encapsulated in a separate template (dag)
    """
    name = dns_name('%s-foreach-%s' % (node.name, node.foreach_param))  # displayed in argo graph
    parent_name = dns_name(node.name)
    return {
        'name': name,
        'template': name,
        'dependencies': [parent_name],
        'arguments': {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}' % (node.name, parent_name)
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
    parent_task_name = parent_task['name']
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


def nested_dag(name, tasks):
    return {
        'name': dns_name(name),
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
                        'parameter': '{{tasks.%s.outputs.parameters.task-id}}' % dns_name(tasks[-1]['name'])
                    }
                }
            ]
        },
        'dag': {
            'tasks': tasks
        }
    }


def remove_empty_elements(spec):
    """
    Removes empty elements from the dictionary and all sub-dictionaries.
    """
    if isinstance(spec, dict):
        kids = {k: remove_empty_elements(v) for k, v in spec.items() if v}
        return {k: v for k, v in kids.items() if v}
    if isinstance(spec, list):
        elems = [remove_empty_elements(v) for v in spec]
        return [v for v in elems if v]
    return spec
