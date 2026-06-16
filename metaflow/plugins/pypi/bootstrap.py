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
import platform
from urllib.error import URLError
from urllib.request import urlopen
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR, CONDA_USE_FAST_INIT
from metaflow.packaging_sys import MetaflowCodeContent, ContentType
from metaflow.plugins import DATASTORES
from metaflow.plugins.pypi.utils import MICROMAMBA_MIRROR_URL, MICROMAMBA_URL
from metaflow.util import which
from urllib.request import Request
import warnings

from . import MAGIC_FILE, _datastore_packageroot

FAST_INIT_BIN_URL = "https://fast-flow-init.outerbounds.sh/{platform}/latest"

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

    def run_cmd(cmd, stdin_str=None):
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
        import gzip

        fast_initializer_path = os.path.join(
            os.getcwd(), "fast-initializer", "bin", "fast-initializer"
        )

        if which("fast-initializer"):
            return which("fast-initializer")
        if os.path.exists(fast_initializer_path):
            os.environ["PATH"] += os.pathsep + os.path.dirname(fast_initializer_path)
            return fast_initializer_path

        url = FAST_INIT_BIN_URL.format(platform=architecture)

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

        _download_and_extract(url)

        # Set executable permission
        os.chmod(fast_initializer_path, 0o755)

        # Update PATH only once at the end
        os.environ["PATH"] += os.pathsep + os.path.dirname(fast_initializer_path)
        return fast_initializer_path

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
        url = MICROMAMBA_URL.format(platform=architecture, version="2.0.4")
        mirror_url = MICROMAMBA_MIRROR_URL.format(
            platform=architecture, version="2.0.4"
        )

        # Prepare directory once
        os.makedirs(os.path.dirname(micromamba_path), exist_ok=True)

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
                        decompressor = bz2.BZ2Decompressor()
                        with warnings.catch_warnings():
                            warnings.filterwarnings(
                                "ignore", category=DeprecationWarning
                            )
                            with tarfile.open(
                                fileobj=io.BytesIO(
                                    decompressor.decompress(response.read())
                                ),
                                mode="r:",
                            ) as tar:
                                member = tar.getmember("bin/micromamba")
                                tar.extract(member, micromamba_dir)
                    break
                except (URLError, IOError) as e:
                    if attempt == max_retries - 1:
                        raise Exception(
                            f"Failed to download micromamba after {max_retries} attempts: {e}"
                        )
                    time.sleep(2**attempt)

        try:
            # first try from mirror
            _download_and_extract(mirror_url)
        except Exception:
            # download from mirror failed, try official source before failing.
            _download_and_extract(url)

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

    @timer
    def fast_setup_environment(architecture, storage, env, prefix, pkgs_dir):
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
        # NOTE: For the time being the fast-initializer only works for the S3 datastore implementation
        cmd = f"fast-initializer --prefix {prefix} --packages-dir {pkgs_dir}"
        run_cmd(cmd, all_package_urls)

    if len(sys.argv) != 4:
        print("Usage: bootstrap.py <flow_name> <id> <datastore_type>")
        sys.exit(1)

    try:
        _, flow_name, id_, datastore_type = sys.argv

        system = platform.system().lower()
        arch_machine = platform.machine().lower()

        if system == "darwin" and arch_machine == "arm64":
            architecture = "osx-arm64"
        elif system == "darwin":
            architecture = "osx-64"
        elif system == "linux" and arch_machine == "aarch64":
            architecture = "linux-aarch64"
        else:
            # default fallback
            architecture = "linux-64"

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
        path_to_manifest = MetaflowCodeContent.get_filename(
            MAGIC_FILE, ContentType.OTHER_CONTENT
        )
        if path_to_manifest is None:
            raise RuntimeError(f"Cannot find {MAGIC_FILE} in the package")
        shutil.move(
            path_to_manifest,
            os.path.join(manifest_dir, MAGIC_FILE),
        )
        with open(os.path.join(manifest_dir, MAGIC_FILE)) as f:
            env = json.load(f)[id_][architecture]

        if CONDA_USE_FAST_INIT:
            fast_setup_environment(architecture, storage, env, prefix, pkgs_dir)
        else:
            setup_environment(
                architecture, storage, env, prefix, conda_pkgs_dir, pypi_pkgs_dir
            )

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
