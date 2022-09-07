import json
import os
import shutil
import sys

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR

from ..env_escape import generate_trampolines, ENV_ESCAPE_PY

from . import CONDA_MAGIC_FILE, get_conda_package_root


def bootstrap_environment(flow_name, env_id, datastore_type):
    setup_conda_manifest(flow_name)
    packages = download_conda_packages(flow_name, env_id, datastore_type)
    install_conda_environment(env_id, packages)


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

        # git commit fbd6c9d8a819fad647958c9fa869153ab37bc0ca introduced support for
        # Microsoft Azure and made a minor tweak to how conda packages are uploaded. As
        # a result, the URL stored by Metaflow no longer includes the datastore root (
        # which arguably helps with portability of datastore). To ensure backwards
        # compatibility, we add this small check here that checks for the prefix of
        # the URLs before downloading them appropriately. Of course, a change can be
        # made to allow the datastore to consume full URLs as well instead of this
        # change, but given that change's far-reaching consequences, we introduce this
        # workaround.
        # https://github.com/Netflix/metaflow/commit/fbd6c9d8a819fad647958c9fa869153ab37bc0ca#diff-1ecbb40de8aba5b41e149987de4aa797a47f4498e5e4e3f63a53d4283dcdf941R198
        if env["cache_urls"][0].startswith("s3://"):
            from metaflow.datatools import S3

            with S3() as s3:
                for pkg in s3.get_many(env["cache_urls"]):
                    shutil.move(
                        pkg.path, os.path.join(pkgs_folder, os.path.basename(pkg.key))
                    )
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

        return env["order"]


def install_conda_environment(env_id, packages):
    args = [
        "if ! type conda  >/dev/null 2>&1; \
            then wget --no-check-certificate https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -O Miniforge3.sh >/dev/null 2>&1; \
            bash ./Miniforge3.sh -b >/dev/null 2>&1; export PATH=$PATH:$HOME/miniforge3/bin; fi",
        "cd {0}".format(os.path.join(os.getcwd(), "pkgs")),
        "conda create --yes --no-default-packages -p {0} --no-deps {1} >/dev/null 2>&1".format(
            os.path.join(os.getcwd(), env_id), " ".join(packages)
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
