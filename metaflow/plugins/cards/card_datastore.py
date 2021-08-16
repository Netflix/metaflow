from hashlib import sha1
from io import BytesIO
import os
from ...datastore.task_datastore import \
    only_if_not_done,\
    require_mode

class CardPathBuilder(object):

    @classmethod
    def path_spec_resolver(cls,
                  pathspec):
        run_id,step_name,task_id = None,None,None
        splits = pathspec.split('/')
        if len(splits) == 1: # only flowname mentioned
            return splits[0],run_id,step_name,task_id
        elif len(splits) == 2:# flowname , runid mentioned
            return splits[0],splits[1],step_name,task_id
        elif len(splits) == 3: # flowname , runid , stepname
            return splits[0],splits[1],splits[2],task_id
        elif len(splits) == 4:# flowname ,runid ,stepname , taskid
            return splits[0],splits[1],splits[2],splits[3]

    @classmethod
    def make_path(cls,
                  sysroot,
                  flow_name,
                  run_id=None,
                  task_id=None,
                  pathspec=None):
        if sysroot is None:
            return None
        if pathspec is not None:
            flow_name,run_id,step_name,task_id = cls.path_spec_resolver(pathspec)
        if flow_name is None:
            return sysroot
        elif run_id is None:
            # todo :[DSC][FUTURE] find namespace here
            # todo :[DSC][FUTURE] what happens when user has namepsace set to none; 
            #             What do we default to here ?
            from metaflow import get_namespace
            namespacename = get_namespace()
            if not namespacename:
                return os.path.join(sysroot, flow_name,'cards',)
            else:
                # cls.card_root/$flow_id/cards/$namespace/cards/$card_name-$hash.html
                return os.path.join(sysroot, flow_name,'cards',namespacename,'cards')
        elif task_id is None:
            return os.path.join(sysroot, flow_name,'runs', run_id,'cards')
        else:
            return os.path.join(sysroot, flow_name,'runs' ,run_id,'tasks', task_id,'cards')


class CardDatastore(object):
    root = None
    # Todo : 
        # should the datastore backend be a direct arguement or 
        # should we use the flow datastore argument ? 
    def __init__(self,
                flow_datastore,
                 run_id,
                 step_name,
                 task_id,
                 attempt=None,
                 data_metadata=None,
                 mode='r',
                 path_spec = None,
                 allow_not_done=False) -> None:
        self._backend = flow_datastore._backend
        self._flow_name = flow_datastore.flow_name
        self.TYPE = self._backend.TYPE
        self._ca_store = flow_datastore.ca_store
        self._environment = flow_datastore.environment
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._mode = mode
        self._attempt = attempt
        self._metadata = flow_datastore.metadata
        self._parent = flow_datastore
        # TODO : 
            # Figure if the path should follow the same pattern 
            # for local and s3 datastore backend
        
        # todo: main root card path comes here
        self._root_path = self._backend.get_card_root_from_config(None)
        self._path_spec = path_spec
        self._is_done_set = False
    
    
    @classmethod
    def get_card_location(cls,base_path,card_name,card_html):
        return os.path.join(base_path,\
                         '%s-%s.html' % (card_name,sha1(bytes(card_html,'utf-8')).hexdigest()))

    @only_if_not_done
    @require_mode('w')
    def save_card(self,card_name,card_html, overwrite=False):
        card_path = CardPathBuilder.make_path(
            self._root_path,
            self._flow_name,
            run_id=self._run_id,
            task_id=self._task_id,
            pathspec=self._path_spec,
        )
        card_path = self.get_card_location(card_path,card_name,card_html)
        
        self._backend.save_bytes(
            {card_path:BytesIO(bytes(card_html,'utf-8'),)},
            overwrite=overwrite
        )
        pass

    @only_if_not_done
    @require_mode('w')
    def done(self):
        self._is_done_set = True