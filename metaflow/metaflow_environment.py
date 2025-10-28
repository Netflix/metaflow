import json
import os
import platform
import sys

from .util import get_username
from . import metaflow_version
from . import metaflow_git
from metaflow.exception import MetaflowException
from metaflow.extension_support import dump_module_info
from metaflow.mflog import BASH_MFLOG, BASH_FLUSH_LOGS
from metaflow.package import MetaflowPackage

from . import R


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

    def validate_environment(self, echo, datastore_type):
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
        Called to add custom files needed for this environment. This hook will be
        called in the `MetaflowPackage` class where metaflow compiles the code package
        tarball. This hook can return one of two things (the first is for backwards
        compatibility -- move to the second):
          - a generator yielding a tuple of `(file_path, arcname)` to add files to
            the code package. `file_path` is the path to the file on the local filesystem
            and `arcname` is the path relative to the packaged code.
          - a generator yielding a tuple of `(content, arcname, type)` where:
            - type is one of
            ContentType.{USER_CONTENT, CODE_CONTENT, MODULE_CONTENT, OTHER_CONTENT}
            - for USER_CONTENT:
              - the file will be included relative to the directory containing the
                user's flow file.
              - content: path to the file to include
              - arcname: path relative to the directory containing the user's flow file
            - for CODE_CONTENT:
              - the file will be included relative to the code directory in the package.
                This will be the directory containing `metaflow`.
              - content: path to the file to include
              - arcname: path relative to the code directory in the package
            - for MODULE_CONTENT:
              - the module will be added to the code package as a python module. It will
                be accessible as usual (import <module_name>)
              - content: name of the module
              - arcname: None (ignored)
            - for OTHER_CONTENT:
              - the file will be included relative to any other configuration/metadata
                files for the flow
              - content: path to the file to include
              - arcname: path relative to the config directory in the package
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
            from .plugins.aws.aws_utils import parse_s3_full_path

            bucket, s3_object = parse_s3_full_path(code_package_url)
            # NOTE: the script quoting is extremely sensitive due to the way shlex.split operates and this being inserted
            # into a quoted command elsewhere.
            # NOTE: Reason for the extra conditionals in the script are because
            # Boto3 does not play well with passing None or an empty string to endpoint_url
            return "{python} -c '{script}'".format(
                python=self._python(),
                script='import boto3, os; ep=os.getenv(\\"METAFLOW_S3_ENDPOINT_URL\\"); boto3.client(\\"s3\\", **({\\"endpoint_url\\":ep} if ep else {})).download_file(\\"%s\\", \\"%s\\", \\"job.tar\\")'
                % (bucket, s3_object),
            )
        elif datastore_type == "azure":
            from .plugins.azure.azure_utils import parse_azure_full_path

            container_name, blob = parse_azure_full_path(code_package_url)
            # remove a trailing slash, if present
            blob_endpoint = "${METAFLOW_AZURE_STORAGE_BLOB_SERVICE_ENDPOINT%/}"
            return "download-azure-blob --blob-endpoint={blob_endpoint} --container={container} --blob={blob} --output-file=job.tar".format(
                blob_endpoint=blob_endpoint,
                blob=blob,
                container=container_name,
            )
        elif datastore_type == "gs":
            from .plugins.gcp.gs_utils import parse_gs_full_path

            bucket_name, gs_object = parse_gs_full_path(code_package_url)
            return (
                "download-gcp-object --bucket=%s --object=%s --output-file=job.tar"
                % (bucket_name, gs_object)
            )
        else:
            raise NotImplementedError(
                "We don't know how to generate a download code package cmd for datastore %s"
                % datastore_type
            )

    def _get_install_dependencies_cmd(self, datastore_type):
        base_cmd = "{} -m pip install -qqq --no-compile --no-cache-dir --disable-pip-version-check".format(
            self._python()
        )

        datastore_packages = {
            "s3": ["boto3"],
            "azure": [
                "azure-identity",
                "azure-storage-blob",
                "azure-keyvault-secrets",
                "simple-azure-blob-downloader",
            ],
            "gs": [
                "google-cloud-storage",
                "google-auth",
                "simple-gcp-object-downloader",
                "google-cloud-secret-manager",
                "packaging",
            ],
        }

        if datastore_type not in datastore_packages:
            raise NotImplementedError(
                "Unknown datastore type: {}".format(datastore_type)
            )

        cmd = "{} {}".format(
            base_cmd, " ".join(datastore_packages[datastore_type] + ["requests"])
        )
        # skip pip installs if we know that packages might already be available
        return "if [ -z $METAFLOW_SKIP_INSTALL_DEPENDENCIES ]; then {}; fi".format(cmd)

    def get_package_commands(
        self, code_package_url, datastore_type, code_package_metadata=None
    ):
        # HACK: We want to keep forward compatibility with compute layers so that
        # they can still call get_package_commands and NOT pass any metadata. If
        # there is no additional information, we *assume* that it is the default
        # used.
        if code_package_metadata is None:
            code_package_metadata = json.dumps(
                {
                    "version": 0,
                    "archive_format": "tgz",
                    "mfcontent_version": 1,
                }
            )

        extra_exports = []
        for k, v in MetaflowPackage.get_post_extract_env_vars(
            code_package_metadata, dest_dir="$(pwd)"
        ).items():
            if k.endswith(":"):
                # If the value ends with a colon, we override the existing value
                extra_exports.append("export %s=%s" % (k[:-1], v))
            else:
                extra_exports.append(
                    "export %s=%s:$(printenv %s)" % (k, v.replace('"', '\\"'), k)
                )

        cmds = (
            [
                BASH_MFLOG,
                BASH_FLUSH_LOGS,
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
            ]
            + MetaflowPackage.get_extract_commands(
                code_package_metadata, "job.tar", dest_dir="."
            )
            + extra_exports
            + [
                "mflog 'Task is starting.'",
                "flush_mflogs",
            ]
        )
        return cmds

    def get_environment_info(self, include_ext_info=False):
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
            "metaflow_version": metaflow_version.get_version(),
            "script": os.path.basename(os.path.abspath(sys.argv[0])),
            # Add git info
            **metaflow_git.get_repository_info(
                path=os.path.dirname(os.path.abspath(sys.argv[0]))
            ),
        }
        if R.use_r():
            env["metaflow_r_version"] = R.metaflow_r_version()
            env["r_version"] = R.r_version()
            env["r_version_code"] = R.r_version_code()
        if include_ext_info:
            # Information about extension modules (to load them in the proper order)
            ext_key, ext_val = dump_module_info()
            env[ext_key] = ext_val
        return {k: v for k, v in env.items() if v is not None and v != ""}

    def executable(self, step_name, default=None):
        if default is not None:
            return default
        return self._python()

    def _python(self):
        if R.use_r():
            return "python3"
        else:
            return "python"
