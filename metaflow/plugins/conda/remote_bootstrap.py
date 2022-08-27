import json
import os
import shutil
import stat
import subprocess
import sys

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import (
    CONDA_REMOTE_INSTALLER_DIRNAME,
    DATASTORE_LOCAL_DIR,
    CONDA_MAGIC_FILE,
    CONDA_REMOTE_INSTALLER,
)

from ..env_escape import generate_trampolines, ENV_ESCAPE_PY

from . import arch_id, get_conda_package_root, get_conda_root


def bootstrap_environment(flow_name, env_id, datastore_type):
    print("    Setting up Conda...")
    conda_exec = setup_conda(datastore_type)
    setup_conda_manifest(flow_name)
    print("    Downloading packages...")
    packages = download_conda_packages(flow_name, env_id, datastore_type)
    print("    Initializing environment...")
    install_conda_environment(conda_exec, env_id, packages)


def setup_conda(datastore_type):
    if CONDA_REMOTE_INSTALLER is not None:
        # We download the installer and return a path to it
        final_path = os.path.join(os.getcwd(), "__conda_installer")
        from metaflow.datastore import DATASTORES

        path_to_fetch = os.path.join(
            CONDA_REMOTE_INSTALLER_DIRNAME,
            CONDA_REMOTE_INSTALLER.format(arch=arch_id()),
        )
        if datastore_type not in DATASTORES:
            raise MetaflowException(
                msg="Downloading conda remote installer from backend %s is unimplemented!"
                % datastore_type
            )
        storage = DATASTORES[datastore_type](get_conda_root(datastore_type))
        with storage.load_bytes([path_to_fetch]) as load_results:
            for _, tmpfile, _ in load_results:
                if tmpfile is None:
                    raise MetaflowException(
                        msg="Cannot find Conda remote installer '%s'"
                        % os.path.join(get_conda_root(datastore_type), path_to_fetch)
                    )
                shutil.move(tmpfile, final_path)
        os.chmod(
            final_path,
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
        )
        return final_path
    # If we don't have a REMOTE_INSTALLER, we check if we need to install one
    args = [
        "if ! type conda  >/dev/null 2>&1; \
        then wget --no-check-certificate "
        "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -O Miniforge3.sh >/dev/null 2>&1; \
        bash ./Miniforge3.sh -b >/dev/null 2>&1; echo $HOME/miniforge3/bin; "
        "else which conda; fi",
    ]
    return subprocess.check_output(args)


def setup_conda_manifest(flow_name):
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    if not os.path.exists(manifest_folder):
        os.makedirs(manifest_folder)
    shutil.move(
        os.path.join(os.getcwd(), CONDA_MAGIC_FILE),
        os.path.join(manifest_folder, CONDA_MAGIC_FILE),
    )


def download_conda_packages(flow_name, env_id, datastore_type):
    pkgs_folder = os.path.join(os.getcwd(), "pkgs")
    if not os.path.exists(pkgs_folder):
        os.makedirs(pkgs_folder)
    # NOTE: if two runs use the same DATASTORE_LOCAL_DIR but different cloud based
    # datastore roots (e.g. a different DATASTORE_SYSROOT_AZURE), then this breaks.
    # This shared local manifest CONDA_MAGIC_FILE will say that cache_urls exist,
    # but those URLs will not actually point to any real objects in the datastore
    # for the second run.
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    with open(os.path.join(manifest_folder, CONDA_MAGIC_FILE)) as f:
        env = json.load(f)[env_id]

        packages = []
        # git commit fbd6c9d8a819fad647958c9fa869153ab37bc0ca introduced support for
        # Microsoft Azure and made a minor tweak to how conda packages are uploaded. As
        # a result, the URL stored by Metaflow no longer includes the datastore root (
        # which arguably helps with portability of datastore). To ensure backwards
        # compatibility, we add this small check here that checks for the prefix of
        # the URLs before downloading them appropriately. Of course, a change can be
        # made to allow the datastore to consume full URLs as well instead of this
        # change, but given that change's far-reaching consequences, we introduce this
        # workfaround.
        # https://github.com/Netflix/metaflow/commit/fbd6c9d8a819fad647958c9fa869153ab37bc0ca#diff-1ecbb40de8aba5b41e149987de4aa797a47f4498e5e4e3f63a53d4283dcdf941R198
        if env["cache_urls"][0].startswith("s3://"):
            from metaflow.datatools import S3

            with S3() as s3:
                for pkg in s3.get_many(env["cache_urls"]):
                    shutil.move(
                        pkg.path, os.path.join(pkgs_folder, os.path.basename(pkg.key))
                    )
                    packages.append(os.path.basename(pkg.key))
        else:
            # Import DATASTORES dynamically... otherwise, circular import
            from metaflow.datastore import DATASTORES

            if datastore_type not in DATASTORES:
                raise MetaflowException(
                    msg="Downloading conda code packages from datastore backend %s is unimplemented!"
                    % datastore_type
                )

            conda_package_root = get_conda_package_root(datastore_type)
            storage = DATASTORES[datastore_type](conda_package_root)
            with storage.load_bytes(env["cache_urls"]) as load_result:
                for key, tmpfile, _ in load_result:
                    shutil.move(
                        tmpfile, os.path.join(pkgs_folder, os.path.basename(key))
                    )
                    packages.append(os.path.basename(key))
        return env.get("order", packages)


def install_conda_environment(conda_exec, env_id, packages):
    args = [
        "cd {0}".format(os.path.join(os.getcwd(), "pkgs")),
        "{0} create --yes --no-default-packages -p {1} --no-deps {2} >/dev/null 2>&1".format(
            conda_exec, os.path.join(os.getcwd(), env_id), " ".join(packages)
        ),
        "cd {0}".format(os.getcwd()),
    ]
    if ENV_ESCAPE_PY is not None:
        cwd = os.getcwd()
        generate_trampolines(cwd)
        # print("Environment escape will use %s as the interpreter" % ENV_ESCAPE_PY)
    else:
        pass
        # print("Could not find a environment escape interpreter")
    os.system(" && ".join(args))


if __name__ == "__main__":
    bootstrap_environment(sys.argv[1], sys.argv[2], sys.argv[3])
