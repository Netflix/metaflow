import os
import shutil
import subprocess
import sys
import tarfile
import time

from metaflow.util import which
from metaflow.meta_files import read_info_file
from metaflow.metaflow_config import get_pinned_conda_libs
from metaflow.packaging_sys import MetaflowCodeContent, ContentType
from urllib.request import Request, urlopen
from urllib.error import URLError

# TODO: support version/platform/architecture selection.
UV_URL = "https://github.com/astral-sh/uv/releases/download/0.6.11/uv-x86_64-unknown-linux-gnu.tar.gz"


def _extract_uv(tar: tarfile.TarFile, uv_install_path: str) -> None:
    def _tar_filter(member: tarfile.TarInfo, path):
        if os.path.basename(member.name) != "uv":
            return None
        member.path = os.path.basename(member.path)
        return member

    try:
        tar.extractall(uv_install_path, filter=_tar_filter)
        return
    except TypeError:
        pass

    candidates = [
        m for m in tar.getmembers() if os.path.basename(m.name) == "uv" and m.isfile()
    ]
    if not candidates:
        raise RuntimeError("Could not find uv binary in archive")

    member = candidates[0]
    extracted = tar.extractfile(member)
    if extracted is None:
        raise RuntimeError("Could not extract uv binary from archive")

    target_path = os.path.join(uv_install_path, "uv")
    try:
        with open(target_path, "wb") as f:
            shutil.copyfileobj(extracted, f)
    finally:
        extracted.close()

    try:
        os.chmod(target_path, member.mode)
    except Exception:
        pass


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

    def install_uv():
        uv_install_path = os.path.join(os.getcwd(), "uv_install")
        if which("uv"):
            return

        print("Installing uv...")

        # Prepare directory once
        os.makedirs(uv_install_path, exist_ok=True)

        # Download and decompress in one go
        headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "User-Agent": "python-urllib",
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                req = Request(UV_URL, headers=headers)
                with urlopen(req) as response:
                    with tarfile.open(fileobj=response, mode="r:gz") as tar:
                        _extract_uv(tar, uv_install_path)
                break
            except (URLError, IOError) as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to download UV after {max_retries} attempts: {e}"
                    )
                time.sleep(2**attempt)

        # Update PATH only once at the end
        os.environ["PATH"] += os.pathsep + uv_install_path

    def get_dependencies(datastore_type):
        # return required dependencies for Metaflow that must be added to the UV environment.
        pinned = get_pinned_conda_libs(None, datastore_type)

        # return only dependency names instead of pinned versions
        return pinned.keys()

    def skip_metaflow_dependencies():
        skip_pkgs = ["metaflow"]
        info = read_info_file()
        if info is not None:
            try:
                skip_pkgs.extend([ext_name for ext_name in info["ext_info"][0].keys()])
            except Exception:
                print(
                    "Failed to read INFO. Metaflow-related packages might get installed during runtime."
                )

        return skip_pkgs

    def sync_uv_project(datastore_type):
        # Move the files to the current directory so uv can find them.
        for filename in ["uv.lock", "pyproject.toml"]:
            path_to_file = MetaflowCodeContent.get_filename(
                filename, ContentType.OTHER_CONTENT
            )
            if path_to_file is None:
                raise RuntimeError(f"Could not find {filename} in the package.")
            shutil.move(path_to_file, os.path.join(os.getcwd(), filename))

        print("Syncing uv project...")
        dependencies = " ".join(get_dependencies(datastore_type))
        skip_pkgs = " ".join(
            [f"--no-install-package {dep}" for dep in skip_metaflow_dependencies()]
        )
        cmd = f"""set -e;
            uv sync --frozen --no-dev {skip_pkgs};
            uv pip install {dependencies} --strict
            """
        run_cmd(cmd)

    if len(sys.argv) != 2:
        print("Usage: bootstrap.py <datastore_type>")
        sys.exit(1)

    try:
        datastore_type = sys.argv[1]
        install_uv()
        sync_uv_project(datastore_type)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
