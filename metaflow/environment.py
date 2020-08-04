import os
import platform
import sys

from .util import get_username, to_unicode
from . import metaflow_version
from metaflow.exception import MetaflowException
from . import R

version_cache = None


class InvalidEnvironmentException(MetaflowException):
    headline = 'Incompatible environment'


class MetaflowEnvironment(object):
    TYPE = 'local'

    def __init__(self, flow):
        pass

    def init_environment(self, logger):
        """
        Run before any step decorators are initialized.
        """
        pass

    def validate_environment(self, logger):
        """
        Run before any command to validate that we are operating in
        a desired environment.
        """
        pass

    def decospecs(self):
        """
        Environment may insert decorators, equivalent to setting --with
        options on the command line.
        """
        return ()

    def bootstrap_commands(self, step_name):
        """
        A list of shell commands to bootstrap this environment in a remote runtime.
        """
        return []

    def add_to_package(self):
        """
        A list of tuples (file, arcname) to add to the job package.
        `arcname` is an alterative name for the file in the job package.
        """
        return []

    def pylint_config(self):
        """
        Environment may override pylint config.
        """
        return []

    @classmethod
    def get_client_info(cls, flow_name, metadata):
        """
        Environment may customize the information returned to the client about the environment

        Parameters
        ----------
        flow_name : str
            Name of the flow
        metadata : dict
            Metadata information regarding the task

        Returns
        -------
        str : Information printed and returned to the user
        """
        return "Local environment"

    def get_package_commands(self, code_package_url):
        cmds = ["set -e",
                "echo \'Setting up task environment.\'",
                "%s -m pip install awscli click requests boto3 -qqq" 
                    % self._python(),
                "mkdir metaflow",
                "cd metaflow",
                "mkdir .metaflow", # mute local datastore creation log
                "i=0; while [ $i -le 5 ]; do "
                    "echo \'Downloading code package.\'; "
                    "%s -m awscli s3 cp %s job.tar >/dev/null && \
                        echo \'Code package downloaded.\' && break; "
                    "sleep 10; i=$((i+1)); "
                "done" % (self._python(), code_package_url),
                "if [ $i -gt 5 ]; then "
                    "echo \'Failed to download code package from %s "
                    "after 6 tries. Exiting...\' && exit 1; "
                "fi" % code_package_url,
                "tar xf job.tar"
                ]
        return cmds

    def get_environment_info(self):
        global version_cache
        if version_cache is None:
            version_cache = metaflow_version.get_version()

        # note that this dict goes into the code package
        # so variables here should be relatively stable (no
        # timestamps) so the hash won't change all the time
        env = {'platform': platform.system(),
               'username': get_username(),
               'production_token': os.environ.get('METAFLOW_PRODUCTION_TOKEN'),
               'runtime': os.environ.get('METAFLOW_RUNTIME_NAME', 'dev'),
               'app': os.environ.get('APP'),
               'environment_type': self.TYPE,
               'use_r': R.use_r(),
               'python_version': sys.version,
               'python_version_code': '%d.%d.%d' % sys.version_info[:3],
               'metaflow_version': version_cache,
               'script': os.path.basename(os.path.abspath(sys.argv[0]))}
        if R.use_r():
            env['metaflow_r_version'] = R.metaflow_r_version()
            env['r_version'] = R.r_version()
            env['r_version_code'] = R.r_version_code()
        return env

    def executable(self, step_name):
        return self._python()

    def _python(self):
        if R.use_r():
            return "python3"
        else:
            return "python"