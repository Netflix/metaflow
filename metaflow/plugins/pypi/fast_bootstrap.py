import concurrent.futures
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from urllib.error import URLError
from urllib.request import urlopen
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.plugins import DATASTORES
from metaflow.plugins.pypi.utils import MICROMAMBA_MIRROR_URL, MICROMAMBA_URL
from metaflow.util import which
from urllib.request import Request
import warnings

from . import MAGIC_FILE, _datastore_packageroot

URL = os.environ.get("FAST_INIT_URL")

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

    def run_cmd(cmd, stdin_str):
        result = subprocess.run(
            cmd,
            shell=True,
            input=stdin_str,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            print(f"Bootstrap failed while executing: {cmd}")
            print("Stdout:", result.stdout)
            print("Stderr:", result.stderr)
            sys.exit(1)

    @timer
    def install_fast_initializer(architecture):
        fast_initializer_path = os.path.join(
            os.getcwd(), "fast-initializer", "bin", "fast-initializer"
        )

        if which("fast-initializer"):
            return which("fast-initializer")
        if os.path.exists(fast_initializer_path):
            os.environ["PATH"] += os.pathsep + os.path.dirname(fast_initializer_path)
            return fast_initializer_path

        # TODO: take architecture into account

        # Prepare directory once
        os.makedirs(os.path.dirname(fast_initializer_path), exist_ok=True)

        # Download and decompress in one go
        def _download_and_extract(url):
            headers = {
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "User-Agent": "python-urllib",
            }

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    req = Request(url, headers=headers)
                    with urlopen(req) as response:
                        with gzip.GzipFile(fileobj=response) as gz:
                            with open(fast_initializer_path, "wb") as f:
                                f.write(gz.read())
                    break
                except (URLError, IOError) as e:
                    if attempt == max_retries - 1:
                        raise Exception(
                            f"Failed to download fast-initializer after {max_retries} attempts: {e}"
                        )
                    time.sleep(2**attempt)

        if URL is None:
            raise Exception("URL for Binary is unset.")

        _download_and_extract(URL)

        # Set executable permission
        os.chmod(fast_initializer_path, 0o755)

        # Update PATH only once at the end
        os.environ["PATH"] += os.pathsep + os.path.dirname(fast_initializer_path)
        return fast_initializer_path

    @timer
    def setup_environment(architecture, storage, env, prefix, pkgs_dir):
        install_fast_initializer(architecture)

        # Get package urls
        conda_pkgs = env["conda"]
        pypi_pkgs = env.get("pypi", [])
        conda_pkg_urls = [package["path"] for package in conda_pkgs]
        pypi_pkg_urls = [package["path"] for package in pypi_pkgs]

        # Create string with package URLs
        all_package_urls = ""
        for url in conda_pkg_urls:
            all_package_urls += f"{storage.datastore_root}/{url}\n"
        all_package_urls += "---\n"
        for url in pypi_pkg_urls:
            all_package_urls += f"{storage.datastore_root}/{url}\n"

        # Initialize environment
        cmd = f"fast-initializer --prefix {prefix} --packages-dir {pkgs_dir}"
        run_cmd(cmd, all_package_urls)

    if len(sys.argv) != 5:
        print("Usage: bootstrap.py <flow_name> <id> <datastore_type> <architecture>")
        sys.exit(1)

    try:
        _, flow_name, id_, datastore_type, architecture = sys.argv

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

        setup_environment(architecture, storage, env, prefix, pkgs_dir)

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
