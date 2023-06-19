import json
import os
import shutil
import sys
import subprocess

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins import DATASTORES
from . import MAGIC_FILE, _datastore_packageroot

# Bootstraps a valid conda virtual environment composed of conda and pypi packages
# on linux-64 architecture

if __name__ == "__main__":
    flow_name = sys.argv[1]
    id_ = sys.argv[2]
    datastore_type = sys.argv[3]

    _supported_architecture = "linux-64"

    prefix = os.path.join(os.getcwd(), id_)
    pkgs_dir = os.path.join(os.getcwd(), ".pkgs")

    storage = [d for d in DATASTORES if d.TYPE == datastore_type][0](
        _datastore_packageroot(datastore_type)
    )

    # Move MAGIC_FILE inside local datastore.
    manifest_dir = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    if not os.path.exists(manifest_dir):
        os.makedirs(manifest_dir)
    shutil.move(
        os.path.join(os.getcwd(), MAGIC_FILE),
        os.path.join(manifest_dir, MAGIC_FILE),
    )

    with open(os.path.join(manifest_dir, MAGIC_FILE)) as f:
        # TODO: Support arm architecture
        env = json.load(f)[id_][_supported_architecture]

    # Create conda virtual environment.
    conda_pkgs_dir = os.path.join(pkgs_dir, "conda")
    with storage.load_bytes([package["path"] for package in env["conda"]]) as results:
        for key, tmpfile, _ in results:
            # Ensure that conda packages go into architecture specific folders.
            # The path looks like REPO/CHANNEL/CONDA_SUBDIR/PACKAGE. We trick
            # Micromamba into believing that all packages are coming from a local
            # channel - the only hurdle is ensuring that packages are organised
            # properly.
            dest = os.path.join(conda_pkgs_dir, "/".join(key.split("/")[-2:]))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.move(tmpfile, dest)

    cmds = [
        # TODO: check if mamba or conda are already available on the image
        # Install Micromamba if it doesn't exist.
        'if ! command -v ./micromamba >/dev/null 2>&1; then \
            wget -qO- https://micromamba.snakepit.net/api/micromamba/linux-64/latest | tar -xvj bin/micromamba --strip-components=1; \
            export PATH=$PATH:$HOME/bin; \
            if ! command -v ./micromamba >/dev/null 2>&1; then \
                echo "Failed to install Micromamba!;" \
                exit 1; \
            fi; \
        fi',
        # Create a conda environment through Micromamba.
        " && ".join(
            [
                "tmpfile=$(mktemp)",
                'echo "@EXPLICIT" > "$tmpfile"',
                'ls -d {conda_pkgs_dir}/*/* >> "$tmpfile"'.format(
                    conda_pkgs_dir=conda_pkgs_dir
                ),
                './micromamba create --yes --offline --no-deps --safety-checks=disabled --prefix {prefix} --file "$tmpfile"'.format(
                    prefix=prefix,
                ),
                'rm "$tmpfile"',
            ]
        ),
    ]

    # Install pypi packages if needed.
    if "pypi" in env:
        pypi_pkgs_dir = os.path.join(pkgs_dir, "pypi")
        with storage.load_bytes(
            [package["path"] for package in env["pypi"]]
        ) as results:
            for key, tmpfile, _ in results:
                dest = os.path.join(pypi_pkgs_dir, os.path.basename(key))
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.move(tmpfile, dest)

        cmds.extend(
            [
                # Install locally available PyPI packages.
                "./micromamba run --prefix {prefix} pip install --root-user-action=ignore {pypi_pkgs_dir}/*.whl".format(
                    prefix=prefix, pypi_pkgs_dir=pypi_pkgs_dir
                )
            ]
        )

    for cmd in cmds:
        result = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode != 0:
            print("Bootstrap failed while executing: {cmd}".format(cmd=cmd))
            print(result.stderr.decode())
            sys.exit(1)
