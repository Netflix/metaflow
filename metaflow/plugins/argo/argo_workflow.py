import os
import sys
import json
import platform

from metaflow.util import get_username, compress_list
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, METADATA_SERVICE_URL, DEFAULT_METADATA
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException
from .argo_client import ArgoClient

"""
Nested Foreach
In order to design the nested foreach in Argo, we need to use nested DAG. Each node in metaflow
that has these two characteristics, counts as a nested foreach. First, the is_inside_foreach for
this node is True. Second, its parent has type foreach. Therefore, this node uses as the entry
node for nested DAG. Moreover, we need to specify the exit condition(node) from this nested DAG.
The exit condition is when we reach to the matching join of the entry node's parent.
But before building nested dag, we need to do topological sort on graph and passed
the ordered queue to build the nested DAG template. The reason for sorting is if both nested foreach
and nested branch happens in this nested foreach. We should be sure that the branches and their
parent must be add in the same DAG. Consider is_inside_foreach for node Y is True and its parent X
has a foreach type. First, we sort the steps from Y to maching_join of X (join_x). Then, define the
nested template which includes inputs,outputs and DAG and add Y as the entry node for this DAG and
iterate over ordered queue till we reach the join_x. Here, the iteration stops and return the output
of the latest node, which is added to this nested DAG,to the upper DAG. Because in Argo the nested
DAG must pass the output to the upper DAG (the DAG that this nested DAG calls inside it).
The next node after each nested dag is always a join node (exit_node). This node doesn't follow the
default rule for input-path and dependencies. Its input-path as follows:
'{{workflow.name}}/(join.in_funcs)>/{{tasks.<(split_parent(join).out_func)>.outputs.parameters}}'
and its dependencies is split_parent(join).out_func.
"""

def dns_name(name):
    """
    Most k8s resource types require a name to be used as
    DNS subdomain name as defined in RFC 1123.
    Hence template names couldn't have '_' (underscore).
    """
    return name.replace('_', '-')


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
        entrypoint = 'entry'
        spec = {'entrypoint': entrypoint}
        if parameters:
            spec['arguments'] = {'parameters': parameters}

        image_pull_secret = self._flow_attributes.get('imagePullSecrets')
        if image_pull_secret:
            spec['imagePullSecrets'] = image_pull_secret

        spec['templates'] = self._generate_templates(entrypoint)

        return spec

    def _generate_step(self):
        steps = [Step(node,
                      self.graph,
                      self._default_image(),
                      self._flow_attributes.get('env', []),
                      self._flow_attributes.get('envFrom', []),
                      self._commands(node, self._parameters()),
                      visited=False)
                 for node in self.graph.nodes.values()]

        map_node_to_step = {step.node: step for step in steps}
        start_step = steps[0]
        sorted_steps = self._sort_steps(start_step, map_node_to_step, stack=[])
        # need to set value of visited to False again for generate template and nested-dag
        for step in sorted_steps:
            step.visited = False
        return sorted_steps, map_node_to_step

    def _sort_steps(self, step, map_node_to_step, stack):
        """
        The aim is to do the topological sort on the graph. we traverse form start node and
        during traversing, if node has a matching-join, we add it to stack first to be sure
        that join runs after its in_funcs. The only reason that we need to do topological
        sort is if the branching happen inside the nested-foreach. Because when we have
        branching, we need to be sure all branches for the same parent add to the same DAG.
        Because user can build his/her pipelines in any order for branching. So we should be
        sure that one branch is not added to another nested DAG. we need to have a property
        visited to steps that each step that once traverse, not go to stack again.
        """
        ordered_queue = []

        if not step.visited:
            ordered_queue.append(step)

        matching_join = step.node.matching_join
        if matching_join:
            # if there is a matching_join for the node, we need to add it to dfs_stack before
            # its node because join must be run always after all its in_funcs's nodes
            matching_join_step = map_node_to_step[self.graph[matching_join]]
            if not matching_join_step.visited:
                matching_join_step.visited = True
                stack.append(self.graph[matching_join])

        for node in step.node.out_funcs:
            step = map_node_to_step[self.graph[node]]
            if not step.visited:
                step.visited = True
                stack.append(self.graph[node])

        while stack:
            node = stack.pop()
            ordered_queue.append(map_node_to_step[node])

            if node.type == 'end':
                return ordered_queue

            return ordered_queue + self._sort_steps(map_node_to_step[node], map_node_to_step, stack)

    def _generate_templates(self, entry):
        tasks = []
        nested_dag_templates = []
        steps, map_node_to_step = self._generate_step()
        templates = [s.template() for s in steps]

        for i, step in enumerate(steps):
            if not step.visited:
                step.visited = True
                tasks.append(step.task())
                if step.is_nested_foreach():
                    exit_step = map_node_to_step[step.matching_join_of_split_parent()]
                    nested_dag_block = steps[i+1:steps.index(exit_step)+1]
                    nested_dag_templates += self._nested_dag(step, nested_dag_block, map_node_to_step)
                    tasks.append(exit_step.task(exit_nested_dag=True))
                    exit_step.visited = True

        dag = {
            'name': entry,
            'dag': {
                'tasks': tasks
            }
        }

        return [dag] + templates + nested_dag_templates

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

    def _nested_dag(self, step, ordered_steps, map_node_to_step):
        """
        each nested foreach (nested-dag) starts with the node (entry-node) that its parent
        is foreach and is inside foreach and exit when reach to the matching join for
        the entry-node's parent.
        """
        template = []
        tmpl = step.nested_dag_template()
        tasks = tmpl['dag']['tasks']
        # exit_condition is always join and this is belongs to the current dag
        exit_condition = step.matching_join_of_split_parent()

        for step in ordered_steps:

            if step.node == exit_condition:
                tmpl['dag']['tasks'] = tasks
                template.append(tmpl)
                return template

            if not step.visited:
                step.visited = True
                if step.is_nested_foreach():
                    tasks.append(step.task())
                    template += self._nested_dag(step, ordered_steps, map_node_to_step)
                    # exit_node is join and this is related to exit condition of inner nested dag
                    # the node which is come after nested dag, is always join
                    exit_step = map_node_to_step[step.matching_join_of_split_parent()]
                    task = exit_step.task(exit_nested_dag=True)
                    tasks.append(task)
                    exit_step.visited = True
                    continue

                task = step.task()
                tasks.append(task)


class Step:
    def __init__(self,
                 node,
                 graph,
                 default_image,
                 env,
                 env_from,
                 commands,
                 visited):
        self.name = dns_name(node.name)
        self.node = node
        self.graph = graph
        self.default_image = default_image
        self.flow_env = env
        self.flow_env_from = env_from
        self.cmds = commands
        self.visited = visited
        self._attr = self._parse_step_docorator(ArgoStepDecorator)
        self._attr.update(self._parse_step_docorator(ResourcesDecorator))

    def _parse_step_docorator(self, deco_type):
        deco = [d for d in self.node.decorators if isinstance(d, deco_type)]
        return deco[0].attributes if deco else {}

    def _parent(self):
        return self.node.in_funcs[0]

    def is_nested_foreach(self):
        return self.node.is_inside_foreach and \
            self.graph[self._parent()].type == 'foreach'

    def matching_join_of_split_parent(self):
        return self.graph[self.graph[self.node.split_parents[-1]].matching_join]

    def task(self, exit_nested_dag=False):
        with_param = None
        template = self.name
        dependencies = [dns_name(d) for d in self.node.in_funcs]

        parameters = {
            'input-paths': self._input_paths()
        }

        if self.is_nested_foreach():
            template = 'nested-%s' % self.name
            parameters['split-index'] = '{{item}}'
            with_param = '{{tasks.%s.outputs.parameters.num-splits}}' % \
                dns_name(self._parent())

        if exit_nested_dag:
            entry_nested_dag = self.graph[self.node.split_parents[-1]].out_funcs[0]
            dependencies = [dns_name(entry_nested_dag)]
            parameters['input-paths'] = '{{workflow.name}}/%s/{{tasks.%s.outputs.parameters}}' % \
                                        (self._parent(), dns_name(entry_nested_dag))

        task = {
            'name': self.name,
            'template': template,
            'dependencies': dependencies,
            'arguments': {
                'parameters': [{'name': k, 'value': v}
                               for k, v in parameters.items()]
            }
        }
        if with_param:
            task['withParam'] = with_param
        return task

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

    def nested_dag_template(self):
        output_node = self.graph[self.graph[self._parent()].matching_join].in_funcs[0]
        outputs = {
            'parameters': [
                {
                    'name': 'task-id',
                    'valueFrom': {
                        'parameter': '{{tasks.%s.outputs.parameters.task-id}}' % dns_name(output_node)
                    }
                }
            ]
        }

        tmpl = {
            'name': 'nested-%s' % self.name,
            'inputs': self._inputs(),
            'dag': {
                'tasks': []
            },
            'outputs': outputs,
        }

        entry_task = {
            'name': self.name,
            'template': self.name,
            'arguments': {
                'parameters': [{'name': k['name'], 'value': "{{inputs.parameters.%s}}" % k['name']}
                               for k in tmpl['inputs']['parameters']]
            }
        }
        tmpl['dag']['tasks'].append(entry_task)

        return tmpl

    def _input_paths(self):
        if self.name == 'start':
            return '{{workflow.name}}/_parameters/0'

        if self.node.type == 'join':
            if self.graph[self.node.split_parents[-1]].type == 'foreach':
                return self._task_params()
            all_parents_params = [self._task_params('task-id', parent)
                                  for parent in self.node.in_funcs]
            return compress_list(all_parents_params)

        return self._task_params('task-id')

    def _task_params(self, param=None, parent=None):
        if parent is None:
            parent = self._parent()
        s = 'tasks.%s.outputs.parameters' % dns_name(parent)
        if param:
            s += '.' + param
        return '{{workflow.name}}/%s/{{%s}}' % (parent, s)

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
        artifacts = self._attr.get('artifacts', []) + self._attr.get('output_artifacts', [])
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
        default_env = [{'name': k, 'value': v} for k, v in default.items()]
        env = default_env + self.flow_env + self._attr.get('env', [])
        env_from = self.flow_env_from + self._attr.get('envFrom', [])
        return env, env_from
