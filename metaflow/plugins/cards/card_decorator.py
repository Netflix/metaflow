import subprocess
import os
import sys
from metaflow.decorators import StepDecorator,flow_decorators
from metaflow.current import current
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.metadata.metadata import MetadataProvider
from metaflow.util import to_unicode, compress_list, unicode_type
# from metaflow import get_metadata

class CardDecorator(StepDecorator):
    name='card'
    defaults = {
        'type':'basic',
        'level': 'task' # flow,task,run
    }
    
    def __init__(self, *args, **kwargs):
        super(CardDecorator,self).__init__(*args, **kwargs)
        self._task_datastore = None
        self._environment = None
        self._metadata= None
    
    def add_to_package(self):
        return list(self._load_card_package())

    def _load_card_package(self):
        try:
            import metaflow_cards
        except ImportError:
            metaflow_cards_root = None
        else:
            metaflow_cards_root = os.path.dirname(metaflow_cards.__file__)
        
        if metaflow_cards_root:
            # What if a file is too large and 
            # gets tagged along the metaflow_cards
            # path; In such cases we can have huge tarballs 
            # that get created;
            # Should we have package suffixes added over here? 
            for path_tuple in \
                self._walk(metaflow_cards_root):
                yield path_tuple
    
    def _walk(self,root):
        root = to_unicode(root)  # handle files/folder with non ascii chars
        prefixlen = len('%s/' % os.path.dirname(root))
        for path, dirs, files in os.walk(root):
            for fname in files:
                # ignoring filesnames which are hidden;
                # TODO : Should we ignore hidden filenames
                if fname[0] == '.':
                    continue

                p = os.path.join(path, fname)
                yield p, p[prefixlen:]

    def step_init(self, flow, graph, step_name, decorators, environment, flow_datastore, logger):
        self._flow_datastore = flow_datastore
        self._environment = environment
    
    def task_pre_step(self, step_name, task_datastore, metadata, run_id, task_id, flow, graph, retry_count, max_user_code_retries, ubf_context, inputs):
        self._task_datastore = task_datastore
        self._metadata = metadata
        

    def task_finished(self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries):
        if not is_task_ok:
            # todo : What do we do when underlying `step` soft-fails. 
            # Todo : What do we do when underlying `@card` fails in some way?
            return 
        runspec = '/'.join([
            current.flow_name,
            current.run_id,
            current.step_name,
            current.task_id
        ])
        self._run_cards_subprocess(runspec)
    
         
    def _create_top_level_args(self):
        def _options(mapping):
            for k, v in mapping.items():
                if v:
                    k = k.replace('_', '-')
                    v = v if isinstance(v, (list, tuple, set)) else [v]
                    for value in v:
                        yield '--%s' % k
                        if not isinstance(value, bool):
                            yield to_unicode(value)

        top_level_options = {
            'quiet': True,
            'metadata': self._metadata.TYPE,
            'coverage': 'coverage' in sys.modules,
            'environment': self._environment.TYPE,
            'datastore': self._flow_datastore.TYPE,
            'datastore-root': self._flow_datastore.datastore_root,
            # We don't provide --with as all execution is taking place in 
            # the context of the main processs
        }
        # FlowDecorators can define their own top-level options. They are
        # responsible for adding their own top-level options and values through
        # the get_top_level_options() hook.
        for deco in flow_decorators():
            top_level_options.update(deco.get_top_level_options())
        return list(_options(top_level_options))
    
    def _run_cards_subprocess(self,runspec):
        from metaflow import get_metadata
        executable = sys.executable
        cmd = [
            executable,
            os.path.basename(sys.argv[0]),]
        cmd+= self._create_top_level_args() +[
            "card",
            "create",
            "--card-type",
            self.attributes['type'],
            "--run-path-spec",
            runspec,
            # todo : test correctness of get_metadata() in future 
            "--metadata-path",
            get_metadata()
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