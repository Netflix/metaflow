import os
import sys
from hashlib import sha1
from multiprocessing.dummy import Pool
import platform
import requests
import shutil
import tempfile
try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse


from metaflow.datastore.local import LocalDataStore
from metaflow.decorators import StepDecorator
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import get_pinned_conda_libs, CONDA_PACKAGE_S3ROOT
from metaflow.util import get_metaflow_root
from metaflow.datatools import S3

from . import read_conda_manifest, write_to_conda_manifest
from .conda import Conda

try:
    unicode
except NameError:
    unicode = str
    basestring = str

class CondaStepDecorator(StepDecorator):
    """
    Conda decorator that sets the Conda environment for your step

    To use, add this decorator to your step:
    ```
    @conda
    @step
    def MyStep(self):
        ...
    ```

    Information in this decorator will override any eventual @conda_base flow level decorator.
    Parameters
    ----------
    libraries : Dict
        Libraries to use for this flow. The key is the name of the package and the value
        is the version to use. Defaults to {}
    python : string
        Version of Python to use (for example: '3.7.4'). Defaults to None
        (will use the current python version)
    disabled : bool
        If set to True, disables Conda. Defaults to False
    """
    name = 'conda'
    defaults = {'libraries': {},
                'python': None,
                'disabled': None}

    conda = None
    environments = None

    def _get_base_attributes(self):
        if 'conda_base' in self.flow._flow_decorators:
            return self.flow._flow_decorators['conda_base'].attributes
        return self.defaults

    def _python_version(self):
        return next(x for x in [
                    self.attributes['python'], 
                    self.base_attributes['python'], 
                    platform.python_version()] if x is not None)

    def is_enabled(self):
        return not next(x for x in [
                        self.attributes['disabled'],
                        self.base_attributes['disabled'],
                        False] if x is not None)

    def _lib_deps(self):
        deps = get_pinned_conda_libs(self._python_version())

        base_deps = self.base_attributes['libraries']
        deps.update(base_deps)
        step_deps = self.attributes['libraries']
        if isinstance(step_deps, (unicode, basestring)):
            step_deps = step_deps.strip('"{}\'')
            if step_deps:
                step_deps = dict(map(lambda x: x.strip().strip('"\''), a.split(':')) for a in step_deps.split(','))
        deps.update(step_deps)
        return deps

    def _step_deps(self):
        deps = [b'python==%s' % self._python_version().encode()]
        deps.extend(b'%s==%s' % (name.encode('ascii'), ver.encode('ascii'))
                    for name, ver in self._lib_deps().items())
        return deps

    def _env_id(self):
        deps = self._step_deps()
        return 'metaflow_%s_%s_%s' % (self.flow.name, 
                                        self.architecture, 
                                        sha1(b' '.join(sorted(deps))).hexdigest())

    def _resolve_step_environment(self, ds_root, force=False):
        env_id = self._env_id()
        cached_deps = read_conda_manifest(ds_root, self.flow.name)
        if CondaStepDecorator.conda is None:
            CondaStepDecorator.conda = Conda()
            CondaStepDecorator.environments =\
                CondaStepDecorator.conda.environments(self.flow.name)
        if force or env_id not in cached_deps or 'cache_urls' not in cached_deps[env_id]:
            if force or env_id not in cached_deps:
                deps = self._step_deps()
                (exact_deps, urls, order) = \
                    self.conda.create(self.step,
                                      env_id,
                                      deps,
                                      architecture=self.architecture,
                                      disable_safety_checks=self.disable_safety_checks)
                payload = {
                    'explicit': exact_deps,
                    'deps': [d.decode('ascii') for d in deps],
                    'urls': urls,
                    'order': order
                }
            else:
                payload = cached_deps[env_id]
            if self.datastore.TYPE == 's3' and 'cache_urls' not in payload:
                payload['cache_urls'] = self._cache_env()
            write_to_conda_manifest(ds_root, self.flow.name, env_id, payload)
            CondaStepDecorator.environments =\
                CondaStepDecorator.conda.environments(self.flow.name)
        return env_id

    def _cache_env(self):
        def _download(entry):
            url, local_path = entry
            with requests.get(url, stream=True) as r:
                with open(local_path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)

        env_id = self._env_id()
        files = []
        to_download = []
        for package_info in self.conda.package_info(env_id):
            url = urlparse(package_info['url'])
            path = os.path.join(CONDA_PACKAGE_S3ROOT, 
                                url.netloc,
                                url.path.lstrip('/'),
                                package_info['md5'],
                                package_info['fn'])
            tarball_path = package_info['package_tarball_full_path']
            if tarball_path.endswith('.conda'):
                # Conda doesn't set the metadata correctly for certain fields
                # when the underlying OS is spoofed.
                tarball_path = tarball_path[:-6]
            if not tarball_path.endswith('.tar.bz2'):
                tarball_path = '%s.tar.bz2' % tarball_path
            if not os.path.isfile(tarball_path):
                # The tarball maybe missing when user invokes `conda clean`!
                to_download.append((package_info['url'], tarball_path))
            files.append((path, tarball_path))
        if to_download:
            Pool(8).map(_download, to_download)
        with S3() as s3:
            s3.put_files(files, overwrite=False)
        return [files[0] for files in files]

    def _prepare_step_environment(self, step_name, ds_root):
        env_id = self._resolve_step_environment(ds_root)
        if env_id not in CondaStepDecorator.environments:
            cached_deps = read_conda_manifest(ds_root, self.flow.name)
            self.conda.create(self.step,
                              env_id,
                              cached_deps[env_id]['urls'],
                              architecture=self.architecture,
                              explicit=True,
                              disable_safety_checks=self.disable_safety_checks)
            CondaStepDecorator.environments =\
                CondaStepDecorator.conda.environments(self.flow.name)
        return env_id

    def _disable_safety_checks(self, decos):
        # Disable conda safety checks when creating linux-64 environments on
        # a macOS. This is needed because of gotchas around inconsistently 
        # case-(in)sensitive filesystems for macOS and linux.
        for deco in decos:
            if deco.name == 'batch' and platform.system() == 'Darwin':
                return True
        return False

    def _architecture(self, decos):
        for deco in decos:
            if deco.name == 'batch':
                # force conda resolution for linux-64 architectures
                return 'linux-64'
        bit = '32'
        if platform.machine().endswith('64'):
            bit = '64'
        if platform.system() == 'Linux':
            return 'linux-%s' % bit
        elif platform.system() == 'Darwin':
            return 'osx-%s' % bit
        else:
            raise InvalidEnvironmentException('The *@conda* decorator is not supported '
                                              'outside of Linux and Darwin platforms')

    def runtime_init(self, flow, graph, package, run_id):
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), 'metaflow')
        self.metaflow_home = tempfile.mkdtemp(dir='/tmp')
        self.addl_paths = None
        os.symlink(path_to_metaflow, os.path.join(self.metaflow_home, 'metaflow'))
        # Do the same for metaflow_custom
        try:
            import metaflow_custom as m
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel
            pass
        else:
            custom_paths = list(m.__path__)
            if len(custom_paths) == 1:
                # Regular package
                os.symlink(custom_paths[0], os.path.join(self.metaflow_home, 'metaflow_custom'))
            else:
                # Namespace package; we don't symlink but add the additional paths
                # for the conda interpreter
                self.addl_paths = [os.path.split(p)[0] for p in custom_paths]

    def step_init(self, flow, graph, step, decos, environment, datastore, logger):
        if environment.TYPE != 'conda':
            raise InvalidEnvironmentException('The *@conda* decorator requires '
                                              '--environment=conda')
        def _logger(line, **kwargs):
            logger(line)
        self.local_root = LocalDataStore.get_datastore_root_from_config(_logger)
        environment.set_local_root(self.local_root)
        self.architecture = self._architecture(decos)
        self.disable_safety_checks = self._disable_safety_checks(decos)
        self.step = step
        self.flow = flow
        self.datastore = datastore
        self.base_attributes = self._get_base_attributes()
        os.environ['PYTHONNOUSERSITE'] = '1'

    def package_init(self, flow, step, environment):
        if self.is_enabled():
            self._prepare_step_environment(step, self.local_root)

    def runtime_task_created(self, datastore, task_id, split_index, input_paths, is_cloned):
        if self.is_enabled():
            self.env_id = self._prepare_step_environment(self.step, self.local_root)

    def task_pre_step(
            self, step_name, ds, meta, run_id, task_id, flow, graph, retry_count, max_retries):
        meta.register_metadata(run_id, step_name, task_id,
                                   [MetaDatum(field='conda_env_id',
                                              value=self._env_id(),
                                              type='conda_env_id',
                                              tags=[])])

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries):
        if self.is_enabled() and 'batch' not in cli_args.commands:
            python_path = self.metaflow_home
            if os.environ.get('PYTHONPATH') is not None:
                python_path = os.pathsep.join([os.environ['PYTHONPATH'], python_path])
            if self.addl_paths is not None:
                addl_paths = os.pathsep.join(self.addl_paths)
                python_path = os.pathsep.join([addl_paths, python_path])
            env_path = os.path.dirname(self.conda.python(self.env_id))
            if os.environ.get('PATH') is not None:
                env_path = os.pathsep.join([env_path, os.environ['PATH']])
            
            cli_args.env['PATH'] = env_path
            cli_args.env['PYTHONPATH'] = python_path
            cli_args.env['_METAFLOW_CONDA_ENV'] = self.env_id
            cli_args.entrypoint[0] = self.conda.python(self.env_id)

    def runtime_finished(self, exception):
        shutil.rmtree(self.metaflow_home)
