import bz2
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins import DATASTORES
from metaflow.util import which

from . import MAGIC_FILE, _datastore_packageroot

# Bootstraps a valid conda virtual environment composed of conda and pypi packages

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

    # Download Conda packages.
    conda_pkgs_dir = os.path.join(pkgs_dir, "conda")
    with storage.load_bytes([package["path"] for package in env["conda"]]) as results:
        for key, tmpfile, _ in results:
            # Ensure that conda packages go into architecture specific folders.
            # The path looks like REPO/CHANNEL/CONDA_SUBDIR/PACKAGE. We trick
            # Micromamba into believing that all packages are coming from a local
            # channel - the only hurdle is ensuring that packages are organised
            # properly.

            # TODO: consider RAM disk
            dest = os.path.join(conda_pkgs_dir, "/".join(key.split("/")[-2:]))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(tmpfile, dest)

    # Create Conda environment.
    cmds = [
        # TODO: check if mamba or conda are already available on the image
        # TODO: micromamba installation can be pawned off to micromamba.py
        f"""set -e;
        if ! command -v micromamba >/dev/null 2>&1; then
            mkdir micromamba;
            python -c "import requests, bz2, sys; data = requests.get('https://micro.mamba.pm/api/micromamba/{architecture}/1.5.7').content; sys.stdout.buffer.write(bz2.decompress(data))" | tar -xv -C $(pwd)/micromamba bin/micromamba --strip-components 1;
            export PATH=$PATH:$(pwd)/micromamba;
            if ! command -v micromamba >/dev/null 2>&1; then
                echo "Failed to install Micromamba!";
                exit 1;
            fi;
        fi""",
        # Create a conda environment through Micromamba.
        f'''set -e;
        tmpfile=$(mktemp);
        echo "@EXPLICIT" > "$tmpfile";
        ls -d {conda_pkgs_dir}/*/* >> "$tmpfile";
        export PATH=$PATH:$(pwd)/micromamba;
        micromamba create --yes --offline --no-deps --safety-checks=disabled --no-extra-safety-checks --prefix {prefix} --file "$tmpfile";
        rm "$tmpfile"''',
    ]

    # Download PyPI packages.
    if "pypi" in env:
        pypi_pkgs_dir = os.path.join(pkgs_dir, "pypi")
        with storage.load_bytes(
            [package["path"] for package in env["pypi"]]
        ) as results:
            for key, tmpfile, _ in results:
                dest = os.path.join(pypi_pkgs_dir, os.path.basename(key))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.move(tmpfile, dest)

        # Install PyPI packages.
        cmds.extend(
            [
                f"""set -e;
                export PATH=$PATH:$(pwd)/micromamba;
                micromamba run --prefix {prefix} python -m pip --disable-pip-version-check install --root-user-action=ignore --no-compile {pypi_pkgs_dir}/*.whl --no-user"""
            ]
        )

    for cmd in cmds:
        result = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode != 0:
            print(f"Bootstrap failed while executing: {cmd}")
            print("Stdout:", result.stdout.decode())
            print("Stderr:", result.stderr.decode())
            sys.exit(1)
