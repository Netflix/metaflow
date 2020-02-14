import os
import imp
from tempfile import NamedTemporaryFile

from .util import to_bytes

R_FUNCTIONS = {}
R_PACKAGE_PATHS = None

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
                yield p, os.path.join('metaflow-r/metaflow.core', p[prefixlen:])
        flow = R_PACKAGE_PATHS['flow']
        yield flow, os.path.basename(flow)

def entrypoint():
    return 'PYTHONPATH=/root/metaflow R_LIBS_SITE=`Rscript -e \'cat(paste(.libPaths(), collapse=\\":\\"))\'`:metaflow-r/ Rscript metaflow-r/metaflow.core/run_batch.R'

def use_r():
    return R_PACKAGE_PATHS is not None

def working_dir():
    if use_r(): 
        return R_PACKAGE_PATHS['wd']
    return None

def run(flow_script, r_functions, metaflow_args, full_cmdline, r_paths):
    global R_FUNCTIONS, R_PACKAGE_PATHS
    R_FUNCTIONS = r_functions
    R_PACKAGE_PATHS = r_paths
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
    try:
        from . import cli
        cli.main(flow,
                 args=metaflow_args,
                 entrypoint=full_cmdline[:-len(metaflow_args)])
    finally:
        os.remove(tmp.name)
