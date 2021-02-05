import os
import sys
import json
import platform

from metaflow.util import get_username, compress_list
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, METADATA_SERVICE_URL
from metaflow.parameters import deploy_time_eval
from metaflow.plugins.aws.batch.batch_decorator import ResourcesDecorator
from .argo_decorator import ArgoStepDecorator, ArgoInternalStepDecorator
from .argo_exception import ArgoException
from .argo_client import ArgoClient


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
        meta = {k: v for k,v in self._flow_attributes.items()
                if k in ('labels', 'annotations') and v}
        meta['name'] = self.name
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

        steps = [Step(node,
                      self.graph,
                      self._default_image(),
                      self._flow_attributes.get('env', []),
                      self._flow_attributes.get('envFrom', []),
                      self._commands(node, parameters))
                 for node in self.graph.nodes.values()]
        spec['templates'] = self._generate_templates(steps, entrypoint)

        return spec

    @staticmethod
    def _generate_templates(steps, entry):
        dag = {
            'name': entry,
            'dag': {
                'tasks': [s.task() for s in steps]
            }
        }
        return [dag] + [s.template() for s in steps]

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
        self._attr = self._parse_step_docorator(ArgoStepDecorator)
        self._attr.update(self._parse_step_docorator(ResourcesDecorator))

    def _parse_step_docorator(self, deco_type):
        deco = [d for d in self.node.decorators if isinstance(d, deco_type)]
        return deco[0].attributes if deco else {}

    def _parent(self):
        return self.node.in_funcs[0]

    def _foreach_child(self):
        return self.node.is_inside_foreach and \
            self.graph[self._parent()].type == 'foreach'

    def task(self):
        with_param = None
        parameters = {
            'input-paths': self._input_paths()
        }
        if self._foreach_child():
            parameters['split-index'] = '{{item}}'
            with_param = '{{tasks.%s.outputs.parameters.num-splits}}' % \
                dns_name(self._parent())
        task = {
            'name': self.name,
            'template': self.name,
            'dependencies': [dns_name(d) for d in self.node.in_funcs],
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
        metadata = {k: v for k,v in self._attr.items()
                    if k in ('labels', 'annotations') and v}
        if metadata:
            tmpl['metadata'] = metadata
        if self._attr.get('nodeSelector'):
            tmpl['nodeSelector'] = self._attr['nodeSelector']
        tmpl['container'] = self._container()
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
        if self._foreach_child():
            params.append('split-index')
        return {
            'parameters': [{'name': ip} for ip in params]
        }

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
        artifacts = self._attr.get('artifacts')
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
        default_env = [{'name': k, 'value': v} for k, v in default.items()]
        env = default_env + self.flow_env + self._attr.get('env', [])
        env_from = self.flow_env_from + self._attr.get('envFrom', [])
        return env, env_from
