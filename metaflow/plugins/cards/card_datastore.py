""" 
Some Questions in the card_cli.view and card_cli.get methods
1. if we slap many @cards on the @step then there can be many cards that are stored. 
    - The path to these cards are based on the type and content; ideally it is given by `type-contenthash`
    - The content can be based on the args given to the card decorator. 
    - With mutliple decorators we can have may @cards with same type but different arguements
        - Retrieving these cards with `card_cli.get` or `card_cli.view` would require an addition arguement relating to the card args
    Q) What should be the arguements for `get` and `view` methods;
        - We can change the  
        - We can change this to `type-contenthash-arghash`
        - Should we pass the content hash  
        - Should the 
    Q) Which of these methods should on the fly compile the card ; 
        - I think view may be such a method where it compiles on the fly; 
    Q) 
"""

from hashlib import sha1
from io import BytesIO
import os
import shutil
from metaflow.datastore.local_backend import LocalBackend
import webbrowser
from .exception import CardNotPresentException

CARD_DIRECTORY_NAME = 'mf.cards'
TEMP_DIR_NAME = 'metaflow_card_cache'
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
                pth_arr = [sysroot,CARD_DIRECTORY_NAME,flow_name,'cards']
            else:
                pth_arr = [sysroot, CARD_DIRECTORY_NAME,flow_name,'cards',namespacename,'cards']
        elif task_id is None:
            pth_arr = [sysroot, CARD_DIRECTORY_NAME,flow_name,'runs', run_id,'cards']
        else:
            pth_arr = [sysroot,CARD_DIRECTORY_NAME, flow_name,'runs' ,run_id,'tasks', task_id,'cards']
        
        if sysroot == '':
            pth_arr.pop(0)
        return os.path.join(*pth_arr)

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
                 path_spec = None):
        self._backend = flow_datastore._backend
        self._flow_name = flow_datastore.flow_name
        self.TYPE = self._backend.TYPE
        self._run_id = run_id
        self._step_name = step_name
        self._task_id = task_id
        self._path_spec = path_spec
        self._temp_card_save_path = self._get_card_path(base_pth=TEMP_DIR_NAME)
        # if self.TYPE != 'local':
        LocalBackend._makedirs(self._temp_card_save_path)
        

        # As cards are rendered best effort attempt may not be 
        # super useful
        # TODO : 
            # Figure if the path should follow the same pattern 
            # for local and s3 datastore backend
            # todo: Check if main root is needed;
    
    @classmethod
    def get_card_location(cls,base_path,card_name,card_html):
        return os.path.join(base_path,\
                         '%s-%s.html' % (card_name,sha1(bytes(card_html,'utf-8')).hexdigest()))
    
    def _get_card_path(self,base_pth=""):
        return CardPathBuilder.make_path(
            # Adding this to avoid errors with s3; 
            # S3Backend has s3root set which requires a relative path 
            # over an absolute path; Providing relative path for local datastore 
            # also works similarly;
            base_pth,
            self._flow_name,
            run_id=self._run_id,
            task_id=self._task_id,
            pathspec=self._path_spec,
        )

    def save_card(self,card_name,card_html, overwrite=True):
        card_path = self.get_card_location(self._get_card_path(),card_name,card_html)
        self._backend.save_bytes(
            [(card_path,BytesIO(bytes(card_html,'utf-8')))],
            overwrite=overwrite
        )
    
    def get_card(self,card_type):
        card_path = self._get_card_path()
        card_paths = self._backend.list_content([card_path])
        if len(card_paths) == 0 :
            # If there are no files found on the Path then raise an error of 
            raise CardNotPresentException(self._flow_name,self._run_id,self._step_name,card_type)
        for task_card_path in card_paths:
            # Check if the card is present.
            if card_type is not None:
                if card_type not in task_card_path.path.split('/')[-1]:
                    continue
            if task_card_path.is_file:
                with self._backend.load_bytes([task_card_path.path]) as get_results:
                    for key, path, meta in get_results:
                        if path is not None:
                            with open(path,'r') as f:
                                print(f.read())




    def view_card(self,card_type):
        card_path = self._get_card_path()
        card_paths = self._backend.list_content([card_path])
        if len(card_paths) == 0 :
            # If there are no files found on the Path then raise an error of 
            raise CardNotPresentException(self._flow_name,self._run_id,self._step_name,card_type)
        
        for task_card_path in card_paths:
            # Check if the card is present.
            if card_type is not None:
                if card_type not in task_card_path.path.split('/')[-1]:
                    continue
            if task_card_path.is_file:
                # We have found the card file.
                with self._backend.load_bytes([task_card_path.path]) as get_results:
                    for key, path, meta in get_results:
                        if path is not None:
                            main_path = path
                            # if self.TYPE != 'local':
                            file_name = key.split('/')[-1]
                            main_path = os.path.join(self._temp_card_save_path,file_name)
                            shutil.copy(path,main_path)
                            url = 'file://' + os.path.abspath(main_path)
                            webbrowser.open(url)
                            break