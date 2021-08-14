import os
import imp
from tempfile import NamedTemporaryFile

from .util import to_bytes

R_FUNCTIONS = {}
R_PACKAGE_PATHS = None
RDS_FILE_PATH = None
R_CONTAINER_IMAGE = None
METAFLOW_R_VERSION = None
R_VERSION = None
R_VERSION_CODE = None

class MetaflowRObject:
    """Wrapper for serialised R data
    
    R objects are presented to Python as bytearrays. This causes issues when
    Python attempts to index the object, as it doesn't understand the structure
    of the R object. For example, the R vector `c(1, 2, 3)` is serialised to a
    bytearray of length 55. If this array is provided as a `foreach` argument
    Python will attempt to generate 55 tasks for the following step, rather than
    the expected 3.
    
    The solution here is to initialise the `MetaflowRObject` class from R with
    the serialised data and the length of the object as if it were deserialised.
    We override the `__len__` method so that it returns this value. Indexing
    this object returns a `MetaflowRObjectIndex` which prevents Python from
    doing the actual indexing, instead delaying it until the object is in R and
    can be properly deserialized.
    
    Parameters
    ----------
    data : bytearray
        Serialised data, converted via reticulate from an R raw vector to a
        Python bytearray.
    length : int
        The length of the object when deserialized in R. Because this
        information comes from outside Python, it must be provided manually at
        initialisation. It is returned by the usual Python `len` function.
        
    Attributes
    ----------
    data : bytearray
        Serialised data, converted via reticulate from an R `raw` vector to a
        Python `bytearray`.
    length : int
        The length of the object when deserialized in R. Because this
        information comes from outside Python, it must be provided manually at
        initialisation. It is returned by the usual Python `len` function.
    """
    def __init__(self, data, length):
        self.data = data
        self.length = length
    
    def __len__(self):
        """Return the length of the object as if it were deserialized in R
        
        Returns
        -------
        int
            The length of the object as if it were deserialized in R.
        """
        return self.length
    
    def __getitem__(self, x):
        """Prepare an indexed value of the R object
        
        If Python indexes the data, it will retrieve a byte from the bytearray
        instead of an element of the deserialized R object. Instead we return an
        object of MetaflowRObjectIndex, which is effectively a copy of the full
        object along with the intended index. This can later be deserialised and
        indexed in R.
        
        Returns
        -------
        MetaflowRObjectIndex
        """
        return MetaflowRObjectIndex(self, x)


class MetaflowRObjectIndex:
    """Wrap an indexed value of a MetaflowR
    
    If Python indexes the data, it will retrieve a byte from the bytearray
    instead of an element of the deserialized R object. Instead we return an
    object of MetaflowRObjectIndex, which is effectively a copy of the full
    object along with the intended index. This can later be deserialised and
    indexed in R.
    
    Parameters
    ----------
    full_object : MetaflowRObject
        The full object to be indexed.
    index : int
        The intended index to be taken. This should be the Python index, that
        is, the index in which `[0]` is the first element.

    Attributes
    ----------
    full_object : MetaflowRObject
        The full object to be indexed.
    index : int
        The intended index to be taken. This should be the Python index, that
        is, the index in which `[0]` is the first element.
    """
    def __init__(self, full_object, index):
        self.full_object = full_object
        self.index = index
        
        if index < 0 or index >= len(full_object):
            raise IndexError("index of MetaflowRObject out of range")
        
    @property
    def r_index(self):
        """Return the equivalent index of the object in R
        
        R is a 1-indexed language, so this property handles the conversion.
        
        Returns
        -------
        int
        """
        return self.index + 1


def call_r(func_name, args):
    R_FUNCTIONS[func_name](*args)

def get_r_func(func_name):
    return R_FUNCTIONS[func_name]

def package_paths():
    if R_PACKAGE_PATHS is not None:
        root = R_PACKAGE_PATHS['package']
        prefixlen = len('%s/' % root.rstrip('/'))
        for path, dirs, files in os.walk(R_PACKAGE_PATHS['package']):
            if '/.' in path:
                continue
            for fname in files:
                if fname[0] == '.':
                    continue
                p = os.path.join(path, fname)
                yield p, os.path.join('metaflow-r', p[prefixlen:])
        flow = R_PACKAGE_PATHS['flow']
        yield flow, os.path.basename(flow)

def entrypoint():
    return 'PYTHONPATH=/root/metaflow R_LIBS_SITE=`Rscript -e \'cat(paste(.libPaths(), collapse=\\":\\"))\'`:metaflow/ Rscript metaflow-r/run_batch.R --flowRDS=%s' % RDS_FILE_PATH

def use_r():
    return R_PACKAGE_PATHS is not None

def container_image():
    return R_CONTAINER_IMAGE

def metaflow_r_version():
    return METAFLOW_R_VERSION

def r_version():
    return R_VERSION

def r_version_code():
    return R_VERSION_CODE

def working_dir():
    if use_r(): 
        return R_PACKAGE_PATHS['wd']
    return None

def run(flow_script,
        r_functions,
        rds_file,
        metaflow_args,
        full_cmdline,
        r_paths,
        r_container_image,
        metaflow_r_version,
        r_version,
        r_version_code):
    global R_FUNCTIONS, \
        R_PACKAGE_PATHS, \
        RDS_FILE_PATH, \
        R_CONTAINER_IMAGE, \
        METAFLOW_R_VERSION, \
        R_VERSION, \
        R_VERSION_CODE

    R_FUNCTIONS = r_functions
    R_PACKAGE_PATHS = r_paths
    RDS_FILE_PATH = rds_file
    R_CONTAINER_IMAGE = r_container_image
    METAFLOW_R_VERSION = metaflow_r_version
    R_VERSION = r_version
    R_VERSION_CODE = r_version_code

    # there's some reticulate(?) sillyness which causes metaflow_args
    # not to be a list if it has only one item. Here's a workaround
    if not isinstance(metaflow_args, list):
        metaflow_args = [metaflow_args]
    # remove any reference to local path structure from R
    full_cmdline[0] = os.path.basename(full_cmdline[0])
    with NamedTemporaryFile(prefix="metaflowR.", delete=False) as tmp:
        tmp.write(to_bytes(flow_script))
    module = imp.load_source('metaflowR', tmp.name)
    flow = module.FLOW(use_cli=False)

    from . import exception 
    from . import cli 
    try:
        cli.main(flow,
                 args=metaflow_args,
                 handle_exceptions=False,
                 entrypoint=full_cmdline[:-len(metaflow_args)])
    except exception.MetaflowException as e:
        cli.print_metaflow_exception(e)
        os.remove(tmp.name)
        os._exit(1)
    except Exception as e:
        import sys
        print(e)
        sys.stdout.flush()
        os.remove(tmp.name)
        os._exit(1)
    finally:
        os.remove(tmp.name)
