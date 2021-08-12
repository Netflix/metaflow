import subprocess
import os
import sys
from metaflow.decorators import StepDecorator,flow_decorators
from metaflow.current import current
from metaflow.datastore.datastore import MetaflowDataStore
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.metadata.metadata import MetadataProvider
from metaflow.util import to_unicode, compress_list, unicode_type

class CardDecorator(StepDecorator):
    name='card'
    defaults = {
        'type':'basic'
    }
    
    def __init__(self, *args, **kwargs):
        super(CardDecorator,self).__init__(*args, **kwargs)
        self._datastore:MetaflowDataStore = None
        self._environment:MetaflowEnvironment = None
        self._metadata:MetadataProvider= None
    
    def task_pre_step(self, step_name, datastore, metadata, run_id, task_id, flow, graph, retry_count, max_user_code_retries, ubf_context, inputs):
        self._metadata = metadata
        self._datastore = datastore

    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries):
        if not is_task_ok:
            # todo : What do we do when underlying `step` soft-fails. 
            # Todo : What do we do when underlying `@card` fails in some way?
            return 
        # todo : Launch as task 
        # todo : write command that will call the sub process CLI
        runspec = '/'.join([
            current.flow_name,
            current.run_id,
            current.step_name,
            current.task_id
        ])
        self._run_cards_subprocess(flow,runspec)
    
    def step_init(self, flow, graph, step_name, decorators, environment, datastore, logger):
        self._environment = environment
        # over here we get datastore class and not the iinstance
         
    def _create_top_level_args(self):
        def _options(mapping):
            for k, v in mapping.items():
                if v:
                    # we need special handling for 'with' since it is a reserved
                    # keyword in Python, so we call it 'decospecs' in click args
                    if k == 'decospecs':
                        k = 'with'
                    k = k.replace('_', '-')
                    v = v if isinstance(v, (list, tuple, set)) else [v]
                    for value in v:
                        yield '--%s' % k
                        if not isinstance(value, bool):
                            yield to_unicode(value)

        top_level_options = {
            'quiet': True,
            'coverage': 'coverage' in sys.modules,
            'metadata': self._metadata.TYPE,
            'environment': self._environment.TYPE,
            'datastore': self._datastore.TYPE,
            'datastore-root': self._datastore.datastore_root,
            # We don't provide --with as all execution is taking place in 
            # the context of the main processs
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook.
        for deco in flow_decorators():
            top_level_options.update(deco.get_top_level_options())
        return list(_options(top_level_options))
    
    def _run_cards_subprocess(self,flow,runspec):
        executable = sys.executable
        
        cmd = [
            executable,
            os.path.basename(sys.argv[0]),
            *self._create_top_level_args(),
            "card",
            "generate",
            "--card-type",
            self.attributes['type'],
            "--run-id-file",
            runspec
        ]
        response,fail = self._run_command(cmd,os.environ)
        if fail:
            # todo : Handle failure scenarios better. 
            print("Process Failed",response.decode('utf-8'))
    
    def _run_command(self,cmd,env):
        fail = False
        try:
            rep = subprocess.check_output(
                cmd,
                env=env,stderr=subprocess.STDOUT,
            )
        except subprocess.CalledProcessError as e:
            rep = e.output
            fail=True
        return rep,fail