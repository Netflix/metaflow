import subprocess
import os
import sys
import json
from metaflow.decorators import StepDecorator,flow_decorators
from metaflow.current import current
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.metadata.metadata import MetadataProvider
from metaflow.util import to_unicode, compress_list, unicode_type
from .exception import BadCardNameException
# from metaflow import get_metadata
import re
CARD_ID_PATTERN = re.compile('^[a-zA-Z0-9_]+$',)

class CardDecorator(StepDecorator):
    name='card'
    defaults = {
        "type":'basic',
        "options": {},
        "id" : None,
        "scope": 'task'
    }
    def runtime_init(self, flow, graph, package, run_id):
        # Check if the card-id matches the regex pattern . 
        # same pattern is asserted in the import to ensure no "-" in card ids/Otherwise naming goes for a toss. 
        if self.attributes['id'] is not None:
            regex_match = re.match(CARD_ID_PATTERN,self.attributes['id'])
            if regex_match is None:
                raise BadCardNameException(self.attributes['id'])
        # set the index property over here so that we can supporting multiple-decorators  
        for step in flow._steps:
            deco_idx = 0
            for deco in step.decorators:
                if isinstance(deco,self.__class__):
                    deco._index = deco_idx
                    deco_idx+=1
    
        
    def __init__(self, *args, **kwargs):
        super(CardDecorator,self).__init__(*args, **kwargs)
        self._task_datastore = None
        self._environment = None
        self._metadata= None
        # todo : first allow multiple decorators with a step
        
        # self._index is useful when many decorators are stacked on top of one another.  
        self._index = None
    
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
        # We do this because of Py3 support JSONDecodeError and 
        # Py2 raises ValueError
        # https://stackoverflow.com/questions/53355389/python-2-3-compatibility-issue-with-exception
        try:
            import json 
            RaisingError = json.decoder.JSONDecodeError
        except AttributeError:  # Python 2
            RaisingError = ValueError
            
        if type(self.attributes['options']) is str:
            try:
                self.attributes['options'] = json.loads(self.attributes['options'])
            except RaisingError:
                # Setting Options from defaults
                self.attributes['options'] = self.defaults['options']
                
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
            current.run_id,
            current.step_name,
            current.task_id
        ])
        self._run_cards_subprocess(runspec)

    @staticmethod
    def _options(mapping):
        for k, v in mapping.items():
            if v:
                k = k.replace('_', '-')
                v = v if isinstance(v, (list, tuple, set)) else [v]
                for value in v:
                    yield '--%s' % k
                    if not isinstance(value, bool):
                        yield to_unicode(value)
         
    def _create_top_level_args(self):

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
        return list(self._options(top_level_options))
    
    def _run_cards_subprocess(self,runspec):
        executable = sys.executable
        cmd = [
            executable,
            os.path.basename(sys.argv[0]),]
        cmd+= self._create_top_level_args() +[
            "card",
            "create",
            runspec,
            "--type",
            self.attributes['type'],
        # Add the options relating to card arguments. 
        # todo : add scope as a CLI arg for the create method. 
        ]
        if self.attributes['options'] is not None and len(self.attributes['options']) > 0:
            cmd+= ['--options',json.dumps(self.attributes['options'])]
        # set the id argument. 
        if self.attributes['id'] is not None and self.attributes['id'] != "":
            id_args = ["--id",self.attributes['id']]
            cmd+=id_args
        if self._index is not None:
            idx_args = ["--index",str(self._index)]
        else:
            idx_args = ["--index",'0'] # setting zero as default
        cmd+=idx_args

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