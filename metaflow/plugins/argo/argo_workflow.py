import os
import sys
import json
import platform

from metaflow.util import get_username, compress_list
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, METADATA_SERVICE_URL, DEFAULT_METADATA
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from metaflow.plugins.environment_decorator import EnvironmentDecorator
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException
from .argo_client import ArgoClient


ENTRYPOINT = 'entry'


def dns_name(name):
    """
    Most k8s resource types require a name to be used as
    DNS subdomain name as defined in RFC 1123.
    Hence template names couldn't have '_' (underscore).
    """
    return name.replace('_', '-')


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
        self._flow_attributes = self._parse_flow_docorator()
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

    def _parse_flow_docorator(self):
        if 'argo_base' in self.flow._flow_decorators:
            return self.flow._flow_decorators['argo_base'].attributes
        return {}

    def _compile(self):
        return {
            'apiVersion': 'argoproj.io/v1alpha1',
            'kind': 'WorkflowTemplate',
            'metadata': self._metadata(),
            'spec': self._spec()
        }

    def _metadata(self):
        meta = {k: v for k, v in self._flow_attributes.items()
                if k in ('labels', 'annotations') and v}
        meta['name'] = dns_name(self.name)
        return meta

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

    def _spec(self):
        parameters = self._parameters()
        spec = {'entrypoint': ENTRYPOINT}
        if parameters:
            spec['arguments'] = {'parameters': parameters}

        image_pull_secret = self._flow_attributes.get('imagePullSecrets')
        if image_pull_secret:
            spec['imagePullSecrets'] = image_pull_secret

        spec['templates'] = self._generate_templates()
        return spec

    def _generate_steps(self):
        # we apply depth-first-search to the graph to sort it and generate the steps
        # at the same time. The reason for sorting is if both nested foreach and nested
        # branch happens in the nested foreach. We should be sure that the branches and
        # their parent must be add in the same DAG, or if user writes his/her script in
        # any order, sort it before passing to nested DAG.
        stack = []
        visited = []
        steps = []
        nested_foreach_stack = []
        stack.append(self.graph['start'])
        while stack:
            node = stack.pop()
            if node.name not in visited:
                visited.append(node.name)
                steps.append(
                    Step(node,
                         self.graph,
                         self._default_image(),
                         self._flow_attributes.get('env', []) + self.env,
                         self._flow_attributes.get('envFrom', []) + self.env_from,
                         self._commands(node, self._parameters())
                    )
                )
                for child in node.out_funcs:
                    # the join node can be added to stack only if all its predecessors (parents)
                    # are visited. otherwise, skip to add join node till the time that all
                    # its in_funcs are visited
                    if self.graph[child].type == 'join' and not \
                            all(predecessor in visited for predecessor in self.graph[child].in_funcs):
                        continue
                    # store the nodes which is nested foreach in stack
                    if self.graph[child].is_inside_foreach and node.type == 'foreach':
                        nested_foreach_stack.append(self.graph[child])
                    stack.append(self.graph[child])
        return steps, nested_foreach_stack

    def matching_join_of_split_parent(self, node):
        return self.graph[self.graph[node.split_parents[-1]].matching_join]

    def _generate_templates(self):
        steps, nested_foreach_stack = self._generate_steps()
        templates = [s.template() for s in steps]
        nested_dag_template = [self._generate_dag_template(steps, nested_node)
                               for nested_node in nested_foreach_stack[::-1]]
        main_dag_template = self._generate_dag_template(steps)

        return [main_dag_template] + templates + nested_dag_template

    def _generate_dag_template(self, steps, nested_node=None):
        map_node_to_step = {step.node: step for step in steps}
        tasks = []
        exit_condition = None
        nested_dag = {}

        if nested_node:
            step = map_node_to_step[nested_node]
            join = self.matching_join_of_split_parent(step.node)
            nested_dag = nested_dag_template(step.node, join)
            tasks = nested_dag['dag']['tasks']
            # This is the exit condition for the current dag
            exit_condition = map_node_to_step[join]
            steps = steps[steps.index(step) + 1:steps.index(exit_condition) + 1]

        for step in steps:
            if exit_condition and step == exit_condition:
                nested_dag['dag']['tasks'] = tasks
                return nested_dag

            if not step.visited:
                step.visited = True
                tasks.append(step.task())
                if step.is_nested_foreach():
                    # the join_after_nested_dag step is belonged to the upper dag if the nested dag occurred
                    join_after_nested_dag = self.matching_join_of_split_parent(step.node)
                    task = join_foreach_task(join_after_nested_dag, ptask=step.node.name)
                    tasks.append(task)
                    map_node_to_step[join_after_nested_dag].visited = True
                    continue

        main_dag_template = {
            'name': ENTRYPOINT,
            'dag': {
                'tasks': tasks
            }
        }

        return main_dag_template

    def _commands(self, node, parameters):
        cmds = self.environment.get_package_commands(self.code_package_url)
        cmds.extend(self.environment.bootstrap_commands(node.name))
        cmds.append("echo 'Task is starting.'")
        cmds.extend(self._step_commands(node, parameters))
        return cmds

    def _step_commands(self, node, parameters):
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
                           (p['name'], p['name']) for p in parameters])
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


class Step:
    def __init__(self,
                 node,
                 graph,
                 default_image,
                 env,
                 env_from,
                 commands):
        self.name = dns_name(node.name)
        self.node = node
        self.graph = graph
        self.default_image = default_image
        self.flow_env = env
        self.flow_env_from = env_from
        self.cmds = commands
        self.visited = False
        self._attr = self._parse_step_decorator(ArgoStepDecorator)
        self._attr.update(self._parse_step_decorator(ResourcesDecorator))
        self._attr.update(self._parse_step_decorator(EnvironmentDecorator))

    def _parse_step_decorator(self, deco_type):
        deco = [d for d in self.node.decorators if isinstance(d, deco_type)]
        return deco[0].attributes if deco else {}

    def _parent(self):
        return self.node.in_funcs[0]

    def is_nested_foreach(self):
        return self.node.is_inside_foreach and \
            self.graph[self._parent()].type == 'foreach'

    def task(self):
        if self.name == 'start':
            return start_task()
        if self.is_nested_foreach():
            return child_foreach_task(self.node)
        return regular_task(self.node)

    def template(self):
        tmpl = {
            'name': self.name,
            'inputs': self._inputs(),
            'outputs': self._outputs()
        }
        metadata = {k: v for k, v in self._attr.items()
                    if k in ('labels', 'annotations') and v}
        if metadata:
            tmpl['metadata'] = metadata
        if self._attr.get('nodeSelector'):
            tmpl['nodeSelector'] = self._attr['nodeSelector']
        tmpl['container'] = self._container()
        return tmpl

    def _inputs(self):
        params = ['input-paths']
        if self.is_nested_foreach():
            params.append('split-index')
        inputs = {
            'parameters': [{'name': ip} for ip in params]
        }
        artifacts = self._attr.get('input_artifacts')
        if artifacts:
            inputs['artifacts'] = artifacts
        return inputs

    def _outputs(self):
        params = [{
            'name': 'task-id',
            'value': '{{pod.name}}'
        }]
        if self.node.type == 'foreach':
            params.append({
                'name': 'num-splits',
                'valueFrom': {'path': ArgoInternalStepDecorator.splits_file_path}
            })
        outputs = {'parameters': params}
        artifacts = self._attr.get('output_artifacts')
        if artifacts:
            outputs['artifacts'] = artifacts
        return outputs

    def _container(self):
        image = self.default_image
        if self._attr.get('image'):
            image = self._attr['image']
        env, env_from = self._prepare_environment()
        container = {
            'image': image,
            'command': ['/bin/sh'],
            'args': ['-c', ' && '.join(self.cmds)],
            'env': env,
        }
        if env_from:
            container['envFrom'] = env_from
        res = self._resources()
        if res:
            container['resources'] = {
                'requests': res,
                'limits': res
            }
        return container

    def _resources(self):
        res = {}
        cpu = self._attr.get('cpu')
        if cpu:
            res['cpu'] = int(cpu)
        mem = self._attr.get('memory')
        if mem:
            # argo cluster treats memory as kb
            res['memory'] = str(mem) + 'Mi'
        gpu = self._attr.get('gpu')
        if gpu:
            res['nvidia.com/gpu'] = int(gpu)
        return res

    def _prepare_environment(self):
        default = {
            'METAFLOW_USER': get_username(),
            'METAFLOW_DATASTORE_SYSROOT_S3': DATASTORE_SYSROOT_S3,
        }
        if METADATA_SERVICE_URL:
            default['METAFLOW_SERVICE_URL'] = METADATA_SERVICE_URL
        if DEFAULT_METADATA:
            default['METAFLOW_DEFAULT_METADATA'] = DEFAULT_METADATA
        # add env vars from @environment decorator if exist
        default.update(self._attr.get('vars', {}))
        default_env = [{'name': k, 'value': v} for k, v in default.items()]
        env = default_env + self.flow_env + self._attr.get('env', [])
        env_from = self.flow_env_from + self._attr.get('envFrom', [])
        return env, env_from


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


def child_foreach_task(node):
    name = dns_name(node.name)
    pstep = node.in_funcs[-1]
    ptask = dns_name(pstep)
    return {
        'name': name,
        'template': 'nested-' + name,
        'dependencies': [ptask],
        'arguments': {
            'parameters': [
                {
                    'name': 'input-paths',
                    'value': '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters.task-id}}' % (pstep, ptask)
                },
                {
                    'name': 'split-index',
                    'value': '{{item}}'
                }
            ]
        },
        'withParam': '{{tasks.%s.outputs.parameters.num-splits}}' % ptask,
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


def join_foreach_task(node, ptask):
    name = dns_name(node.name)
    pstep = node.in_funcs[-1]
    ptask = dns_name(ptask)
    return {
        'name': name,
        'template': name,
        'dependencies': [ptask],
        'arguments': {
            'parameters': [{
                'name': 'input-paths',
                'value': '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters}}' % (pstep, ptask)
            }]
        }
    }


def regular_task(node):
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


def nested_dag_template(node, join):
    name = dns_name(node.name)
    last_dag_task = dns_name(join.in_funcs[0])
    return {
        'name': 'nested-%s' % name,
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
        'dag': {
            'tasks': [dag_first_task(node)]
        }
    }
