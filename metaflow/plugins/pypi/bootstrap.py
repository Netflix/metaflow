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


def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        # print(f"Time taken for {func.__name__}: {duration:.2f} seconds")
        return result

    return wrapper


if __name__ == "__main__":
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
        result = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            print(f"Bootstrap failed while executing: {cmd}")
            print("Stdout:", result.stdout)
            print("Stderr:", result.stderr)
            sys.exit(1)

    @timer
    def install_micromamba(architecture):
        micromamba_dir = os.path.join(os.getcwd(), "micromamba")
        micromamba_path = os.path.join(micromamba_dir, "bin", "micromamba")

        if which("micromamba"):
            return which("micromamba")
        if os.path.exists(micromamba_path):
            os.environ["PATH"] += os.pathsep + os.path.dirname(micromamba_path)
            return micromamba_path

        # Download and extract in one go
        # TODO: Serve from cloudflare
        url = f"https://micro.mamba.pm/api/micromamba/{architecture}/2.0.4"

        # Prepare directory once
        os.makedirs(os.path.dirname(micromamba_path), exist_ok=True)

        # Stream and process directly to file
        with requests.get(url, stream=True, timeout=30) as response:
            if response.status_code != 200:
                raise Exception(
                    f"Failed to download micromamba: HTTP {response.status_code}"
                )

            decompressor = bz2.BZ2Decompressor()

            # Process in memory without temporary files
            tar_content = decompressor.decompress(response.raw.read())

            with tarfile.open(fileobj=io.BytesIO(tar_content), mode="r:") as tar:
                member = tar.getmember("bin/micromamba")
                # Extract directly to final location
                with open(micromamba_path, "wb") as f:
                    f.write(tar.extractfile(member).read())

        # Set executable permission
        os.chmod(micromamba_path, 0o755)

        # Update PATH only once at the end
        os.environ["PATH"] += os.pathsep + os.path.dirname(micromamba_path)
        return micromamba_path

    @timer
    def download_conda_packages(storage, packages, dest_dir):

        def process_conda_package(args):
            # Ensure that conda packages go into architecture specific folders.
            # The path looks like REPO/CHANNEL/CONDA_SUBDIR/PACKAGE. We trick
            # Micromamba into believing that all packages are coming from a local
            # channel - the only hurdle is ensuring that packages are organised
            # properly.
            key, tmpfile, dest_dir = args
            dest = os.path.join(dest_dir, "/".join(key.split("/")[-2:]))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(tmpfile, dest)

        os.makedirs(dest_dir, exist_ok=True)
        with storage.load_bytes([package["path"] for package in packages]) as results:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(
                    process_conda_package,
                    [(key, tmpfile, dest_dir) for key, tmpfile, _ in results],
                )
            # for key, tmpfile, _ in results:

            #     # TODO: consider RAM disk
            #     dest = os.path.join(dest_dir, "/".join(key.split("/")[-2:]))
            #     os.makedirs(os.path.dirname(dest), exist_ok=True)
            #     shutil.move(tmpfile, dest)
        return dest_dir

    @timer
    def download_pypi_packages(storage, packages, dest_dir):

        def process_pypi_package(args):
            key, tmpfile, dest_dir = args
            dest = os.path.join(dest_dir, os.path.basename(key))
            shutil.move(tmpfile, dest)

        os.makedirs(dest_dir, exist_ok=True)
        with storage.load_bytes([package["path"] for package in packages]) as results:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(
                    process_pypi_package,
                    [(key, tmpfile, dest_dir) for key, tmpfile, _ in results],
                )
            # for key, tmpfile, _ in results:
            #     dest = os.path.join(dest_dir, os.path.basename(key))
            #     shutil.move(tmpfile, dest)
        return dest_dir

    @timer
    def create_conda_environment(prefix, conda_pkgs_dir):
        cmd = f'''set -e;
            tmpfile=$(mktemp);
            echo "@EXPLICIT" > "$tmpfile";
            ls -d {conda_pkgs_dir}/*/* >> "$tmpfile";
            export PATH=$PATH:$(pwd)/micromamba;
            export CONDA_PKGS_DIRS=$(pwd)/micromamba/pkgs;
            export MAMBA_NO_LOW_SPEED_LIMIT=1;
            export MAMBA_USE_INDEX_CACHE=1;
            export MAMBA_NO_PROGRESS_BARS=1;
            export CONDA_FETCH_THREADS=1;
            micromamba create --yes --offline --no-deps \
                --safety-checks=disabled --no-extra-safety-checks \
                --prefix {prefix} --file "$tmpfile" \
                --no-pyc --no-rc --always-copy;
            rm "$tmpfile"'''
        run_cmd(cmd)

    @timer
    def install_pypi_packages(prefix, pypi_pkgs_dir):

        cmd = f"""set -e;
            export PATH=$PATH:$(pwd)/micromamba;
            export CONDA_PKGS_DIRS=$(pwd)/micromamba/pkgs;
            micromamba run --prefix {prefix} python -m pip --disable-pip-version-check \
                install --root-user-action=ignore --no-compile --no-index \
                --no-cache-dir --no-deps --prefer-binary \
                --find-links={pypi_pkgs_dir}  --no-user \
                --no-warn-script-location --no-input \
                {pypi_pkgs_dir}/*.whl
            """
        run_cmd(cmd)

    @timer
    def setup_environment(
        architecture, storage, env, prefix, conda_pkgs_dir, pypi_pkgs_dir
    ):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # install micromamba, download conda and pypi packages in parallel
            futures = {
                "micromamba": executor.submit(install_micromamba, architecture),
                "conda_pkgs": executor.submit(
                    download_conda_packages, storage, env["conda"], conda_pkgs_dir
                ),
            }
            if "pypi" in env:
                futures["pypi_pkgs"] = executor.submit(
                    download_pypi_packages, storage, env["pypi"], pypi_pkgs_dir
                )

            # create conda environment after micromamba is installed and conda packages are downloaded
            done, _ = concurrent.futures.wait(
                [futures["micromamba"], futures["conda_pkgs"]],
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            for future in done:
                future.result()

            # start conda environment creation
            futures["conda_env"] = executor.submit(
                create_conda_environment, prefix, conda_pkgs_dir
            )

            if "pypi" in env:
                # install pypi packages after conda environment is created and pypi packages are downloaded
                done, _ = concurrent.futures.wait(
                    [futures["conda_env"], futures["pypi_pkgs"]],
                    return_when=concurrent.futures.ALL_COMPLETED,
                )

                for future in done:
                    future.result()

                # install pypi packages
                futures["pypi_install"] = executor.submit(
                    install_pypi_packages, prefix, pypi_pkgs_dir
                )
                # wait for pypi packages to be installed
                futures["pypi_install"].result()
            else:
                # wait for conda environment to be created
                futures["conda_env"].result()

    setup_environment(architecture, storage, env, prefix, conda_pkgs_dir, pypi_pkgs_dir)
