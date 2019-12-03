import functools
import json
from multiprocessing import Pool
import os
import tarfile
import shutil
import subprocess
import sys

from metaflow.datatools import S3
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from . import CONDA_MAGIC_FILE


def bootstrap_environment(flow_name, env_id):
    setup_conda_manifest(flow_name)
    packages = download_conda_packages(flow_name, env_id)
    install_conda_environment(env_id, packages)

def setup_conda_manifest(flow_name):
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    if not os.path.exists(manifest_folder):
        os.makedirs(manifest_folder)
    shutil.move(os.path.join(os.getcwd(), CONDA_MAGIC_FILE), 
        os.path.join(manifest_folder, CONDA_MAGIC_FILE))

def download_conda_packages(flow_name, env_id):
    pkgs_folder = os.path.join(os.getcwd(), 'pkgs')
    if not os.path.exists(pkgs_folder):
        os.makedirs(pkgs_folder)
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    with open(os.path.join(manifest_folder, CONDA_MAGIC_FILE)) as f:
        env = json.load(f)[env_id]
        with S3() as s3:
            for pkg in s3.get_many(env['cache_urls']):
                shutil.move(pkg.path, os.path.join(pkgs_folder, os.path.basename(pkg.key)))
        return env['order']

def install_conda_environment(env_id, packages):
    args = [
        'if ! type conda  >/dev/null 2>&1; \
            then wget --no-check-certificate https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh >/dev/null 2>&1; \
            bash ~/miniconda.sh -b -p {0} >/dev/null 2>&1; \
            export PATH=$PATH:{0}/bin; fi'.format(os.path.join(os.getcwd(), 'conda')),
        'cd {0}'.format(os.path.join(os.getcwd(), 'pkgs')),
        'conda create --yes --no-default-packages -p {0} --no-deps {1} >/dev/null 2>&1'.format(os.path.join(os.getcwd(), env_id), ' '.join(packages)),
        'cd {0}'.format(os.getcwd())
    ]
    os.system(' && '.join(args))

if __name__ == '__main__':
    bootstrap_environment(sys.argv[1], sys.argv[2])