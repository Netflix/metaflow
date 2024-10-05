import bz2
import concurrent.futures
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time

import requests

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins import DATASTORES
from metaflow.util import which

from . import MAGIC_FILE, _datastore_packageroot

# Bootstraps a valid conda virtual environment composed of conda and pypi packages


def print_timer(operation, start_time):
    duration = time.time() - start_time
    print(f"Time taken for {operation}: {duration:.2f} seconds")


if __name__ == "__main__":
    total_start_time = time.time()
    if len(sys.argv) != 5:
        print("Usage: bootstrap.py <flow_name> <id> <datastore_type> <architecture>")
        sys.exit(1)
    _, flow_name, id_, datastore_type, architecture = sys.argv

    # TODO: Detect architecture on the fly when dealing with arm architectures.
    # ARCH=$(uname -m)
    # OS=$(uname)

    # if [[ "$OS" == "Linux" ]]; then
    #     PLATFORM="linux"
    #     if [[ "$ARCH" == "aarch64" ]]; then
    #         ARCH="aarch64";
    #     elif [[ $ARCH == "ppc64le" ]]; then
    #         ARCH="ppc64le";
    #     else
    #         ARCH="64";
    #     fi
    # fi

    # if [[ "$OS" == "Darwin" ]]; then
    #     PLATFORM="osx";
    #     if [[ "$ARCH" == "arm64" ]]; then
    #         ARCH="arm64";
    #     else
    #         ARCH="64"
    #     fi
    # fi

    prefix = os.path.join(os.getcwd(), architecture, id_)
    pkgs_dir = os.path.join(os.getcwd(), ".pkgs")
    conda_pkgs_dir = os.path.join(pkgs_dir, "conda")
    pypi_pkgs_dir = os.path.join(pkgs_dir, "pypi")
    manifest_dir = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)

    datastores = [d for d in DATASTORES if d.TYPE == datastore_type]
    if not datastores:
        print(f"No datastore found for type: {datastore_type}")
        sys.exit(1)

    storage = datastores[0](
        _datastore_packageroot(datastores[0], lambda *args, **kwargs: None)
    )

    # Move MAGIC_FILE inside local datastore.
    os.makedirs(manifest_dir, exist_ok=True)
    shutil.move(
        os.path.join(os.getcwd(), MAGIC_FILE),
        os.path.join(manifest_dir, MAGIC_FILE),
    )
    with open(os.path.join(manifest_dir, MAGIC_FILE)) as f:
        env = json.load(f)[id_][architecture]

    def run_cmd(cmd):
        cmd_start_time = time.time()
        result = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        print_timer(f"Command: {cmd}", cmd_start_time)
        if result.returncode != 0:
            print(f"Bootstrap failed while executing: {cmd}")
            print("Stdout:", result.stdout)
            print("Stderr:", result.stderr)
            sys.exit(1)

    def install_micromamba(architecture):
        # TODO: check if mamba or conda are already available on the image
        micromamba_timer = time.time()
        micromamba_dir = os.path.join(os.getcwd(), "micromamba")
        micromamba_path = os.path.join(micromamba_dir, "bin", "micromamba")

        if which("micromamba") or os.path.exists(micromamba_path):
            return micromamba_path

        os.makedirs(micromamba_dir, exist_ok=True)
        # TODO: download micromamba from datastore
        url = f"https://micro.mamba.pm/api/micromamba/{architecture}/1.5.7"
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            raise Exception(
                f"Failed to download micromamba: HTTP {response.status_code}"
            )
        tar_content = bz2.BZ2Decompressor().decompress(response.raw.read())
        with tarfile.open(fileobj=io.BytesIO(tar_content), mode="r:") as tar:
            tar.extract("bin/micromamba", path=micromamba_dir, set_attrs=False)

        os.chmod(micromamba_path, 0o755)
        if not os.path.exists(micromamba_path):
            raise Exception("Failed to install Micromamba!")

        os.environ["PATH"] += os.pathsep + os.path.dirname(micromamba_path)
        print_timer("Downloading micromamba", micromamba_timer)
        return micromamba_path

    def download_conda_packages(storage, packages, dest_dir):
        download_start_time = time.time()
        os.makedirs(dest_dir, exist_ok=True)
        with storage.load_bytes([package["path"] for package in packages]) as results:
            for key, tmpfile, _ in results:
                # Ensure that conda packages go into architecture specific folders.
                # The path looks like REPO/CHANNEL/CONDA_SUBDIR/PACKAGE. We trick
                # Micromamba into believing that all packages are coming from a local
                # channel - the only hurdle is ensuring that packages are organised
                # properly.

                # TODO: consider RAM disk
                dest = os.path.join(dest_dir, "/".join(key.split("/")[-2:]))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.move(tmpfile, dest)
        print_timer("Downloading conda packages", download_start_time)
        return dest_dir

    def download_pypi_packages(storage, packages, dest_dir):
        download_start_time = time.time()
        os.makedirs(dest_dir, exist_ok=True)
        with storage.load_bytes([package["path"] for package in packages]) as results:
            for key, tmpfile, _ in results:
                dest = os.path.join(dest_dir, os.path.basename(key))
                shutil.move(tmpfile, dest)
        print_timer("Downloading pypi packages", download_start_time)
        return dest_dir

    def create_conda_environment(prefix, conda_pkgs_dir):
        cmd = f'''set -e;
            tmpfile=$(mktemp);
            echo "@EXPLICIT" > "$tmpfile";
            ls -d {conda_pkgs_dir}/*/* >> "$tmpfile";
            export PATH=$PATH:$(pwd)/micromamba;
            export CONDA_PKGS_DIRS=$(pwd)/micromamba/pkgs;
            micromamba create --yes --offline --no-deps --safety-checks=disabled --no-extra-safety-checks --prefix {prefix} --file "$tmpfile";
            rm "$tmpfile"'''
        run_cmd(cmd)

    def install_pypi_packages(prefix, pypi_pkgs_dir):
        cmd = f"""set -e;
            export PATH=$PATH:$(pwd)/micromamba;
            export CONDA_PKGS_DIRS=$(pwd)/micromamba/pkgs;
            micromamba run --prefix {prefix} python -m pip --disable-pip-version-check install --root-user-action=ignore --no-compile --no-index --no-cache-dir --no-deps --prefer-binary --find-links={pypi_pkgs_dir} {pypi_pkgs_dir}/*.whl --no-user"""
        run_cmd(cmd)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # install micromamba, download conda and pypi packages in parallel
        future_install_micromamba = executor.submit(install_micromamba, architecture)
        future_download_conda_packages = executor.submit(
            download_conda_packages, storage, env["conda"], conda_pkgs_dir
        )
        future_download_pypi_packages = None
        if "pypi" in env:
            future_download_pypi_packages = executor.submit(
                download_pypi_packages, storage, env["pypi"], pypi_pkgs_dir
            )
        # create conda environment after micromamba is installed and conda packages are downloaded
        concurrent.futures.wait(
            [future_install_micromamba, future_download_conda_packages]
        )
        future_create_conda_environment = executor.submit(
            create_conda_environment, prefix, conda_pkgs_dir
        )
        if "pypi" in env:
            # install pypi packages after conda environment is created and pypi packages are downloaded
            concurrent.futures.wait(
                [future_create_conda_environment, future_download_pypi_packages]
            )
            future_install_pypi_packages = executor.submit(
                install_pypi_packages, prefix, pypi_pkgs_dir
            )
            # wait for pypi packages to be installed
            future_install_pypi_packages.result()
        else:
            # wait for conda environment to be created
            future_create_conda_environment.result()

    total_time = time.time() - total_start_time
    print(f"{total_time:.2f}")
