import os
import platform
import sys

from .util import get_username
from . import metaflow_version
from metaflow.exception import MetaflowException
from metaflow.extension_support import dump_module_info
from metaflow.mflog import BASH_MFLOG
from . import R

version_cache = None


class InvalidEnvironmentException(MetaflowException):
    headline = "Incompatible environment"


class MetaflowEnvironment(object):
    TYPE = "local"

    def __init__(self, flow):
        pass

    def init_environment(self, echo):
        """
        Run before any step decorators are initialized.
        """
        pass

    def validate_environment(self, echo):
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

    def bootstrap_commands(self, step_name, datastore_type):
        """
        A list of shell commands to bootstrap this environment in a remote runtime.
        """
        return []

    def add_to_package(self):
        """
        A list of tuples (file, arcname) to add to the job package.
        `arcname` is an alternative name for the file in the job package.
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

    def _get_download_code_package_cmd(self, code_package_url, datastore_type):
        """Return a command that downloads the code package from the datastore. We use various
        cloud storage CLI tools because we don't have access to Metaflow codebase (which we
        are about to download in the command).

        The command should download the package to "job.tar" in the current directory.

        It should work silently if everything goes well.
        """
        if datastore_type == "s3":
            return (
                '%s -m awscli ${METAFLOW_S3_ENDPOINT_URL:+--endpoint-url=\\"${METAFLOW_S3_ENDPOINT_URL}\\"} '
                + "s3 cp %s job.tar >/dev/null"
            ) % (self._python(), code_package_url)
        elif datastore_type == "azure":
            from .plugins.azure.azure_utils import parse_azure_full_path

            container_name, blob = parse_azure_full_path(code_package_url)
            blob_url = "{storage_account_url}/{container}/{blob}".format(
                storage_account_url="${METAFLOW_AZURE_STORAGE_ACCOUNT_URL%/}",  # remove a trailing slash, if present
                container=container_name,
                blob=blob,
            )
            # Note that the signature is attached only if it is defined in env.
            # Alternative Azure auth mechanisms will likely be implemented in future.
            source_quoted = (
                '\\"%s${METAFLOW_AZURE_STORAGE_SHARED_ACCESS_SIGNATURE:+?${METAFLOW_AZURE_STORAGE_SHARED_ACCESS_SIGNATURE}}\\"'
                % (blob_url,)
            )
            return "/tmp/azcopy copy {source_quoted} job.tar".format(
                source_quoted=source_quoted
            )
        else:
            raise NotImplementedError(
                "We don't know how to generate a download code package cmd for datastore %s"
                % datastore_type
            )

    def _get_install_dependencies_cmd(self, datastore_type):
        cmds = ["%s -m pip install requests -qqq" % self._python()]
        if datastore_type == "s3":
            cmds.append("%s -m pip install awscli boto3 -qqq" % self._python())
        elif datastore_type == "azure":
            cmds.append(
                "%s -m pip install azure-identity azure-storage-blob -qqq"
                % self._python()
            )
            # See: https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10#obtain-a-static-download-link
            # This links to amd64 build. Yes, this not return the right thing for arm64.
            # Note as of 7/19/2022, ARM build exists but is in preview status.
            # We will need to account for this in future.  Same issue in miniconda installation in batch_bootstrap.py
            cmds.extend(
                [
                    "wget -O azcopy_v10.tar.gz https://aka.ms/downloadazcopy-v10-linux",
                    "tar -xf azcopy_v10.tar.gz --strip-components=1",
                    "mv azcopy /tmp/azcopy",  # we make no assumptions about evolution of CWD
                    "chmod +x /tmp/azcopy",
                ]
            )
        else:
            raise NotImplementedError(
                "We don't know how to generate an install dependencies cmd for datastore %s"
                % datastore_type
            )
        return " && ".join(cmds)

    def get_package_commands(self, code_package_url, datastore_type):
        cmds = [
            BASH_MFLOG,
            "mflog 'Setting up task environment.'",
            self._get_install_dependencies_cmd(datastore_type),
            "mkdir metaflow",
            "cd metaflow",
            "mkdir .metaflow",  # mute local datastore creation log
            "i=0; while [ $i -le 5 ]; do "
            "mflog 'Downloading code package...'; "
            + self._get_download_code_package_cmd(code_package_url, datastore_type)
            + " && mflog 'Code package downloaded.' && break; "
            "sleep 10; i=$((i+1)); "
            "done",
            "if [ $i -gt 5 ]; then "
            "mflog 'Failed to download code package from %s "
            "after 6 tries. Exiting...' && exit 1; "
            "fi" % code_package_url,
            "TAR_OPTIONS='--warning=no-timestamp' tar xf job.tar",
            "mflog 'Task is starting.'",
        ]
        return cmds

    def get_environment_info(self):
        global version_cache
        if version_cache is None:
            version_cache = metaflow_version.get_version()

        # note that this dict goes into the code package
        # so variables here should be relatively stable (no
        # timestamps) so the hash won't change all the time
        env = {
            "platform": platform.system(),
            "username": get_username(),
            "production_token": os.environ.get("METAFLOW_PRODUCTION_TOKEN"),
            "runtime": os.environ.get("METAFLOW_RUNTIME_NAME", "dev"),
            "app": os.environ.get("APP"),
            "environment_type": self.TYPE,
            "use_r": R.use_r(),
            "python_version": sys.version,
            "python_version_code": "%d.%d.%d" % sys.version_info[:3],
            "metaflow_version": version_cache,
            "script": os.path.basename(os.path.abspath(sys.argv[0])),
        }
        if R.use_r():
            env["metaflow_r_version"] = R.metaflow_r_version()
            env["r_version"] = R.r_version()
            env["r_version_code"] = R.r_version_code()
        # Information about extension modules (to load them in the proper order)
        ext_key, ext_val = dump_module_info()
        env[ext_key] = ext_val
        return env

    def executable(self, step_name):
        return self._python()

    def _python(self):
        if R.use_r():
            return "python3"
        else:
            return "python"
