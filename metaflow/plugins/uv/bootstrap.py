import io
import os
import shutil
import subprocess
import sys
import time
import zipfile
import platform

from metaflow.util import which
from metaflow.meta_files import read_info_file
from metaflow.metaflow_config import get_pinned_conda_libs
from metaflow.packaging_sys import MetaflowCodeContent, ContentType
from urllib.request import Request, urlopen
from urllib.error import URLError

DEFAULT_UV_VERSION = os.environ.get("METAFLOW_UV_VERSION", "0.6.11")
UV_RELEASE_URL = (
    "https://github.com/astral-sh/uv/releases/download/{version}/{asset_name}"
)
UV_PLATFORM_ASSETS = {
    ("linux", "x86_64"): ("uv-x86_64-unknown-linux-gnu.tar.gz", "uv", "tar.gz"),
    ("linux", "aarch64"): ("uv-aarch64-unknown-linux-gnu.tar.gz", "uv", "tar.gz"),
    ("darwin", "x86_64"): ("uv-x86_64-apple-darwin.tar.gz", "uv", "tar.gz"),
    ("darwin", "aarch64"): ("uv-aarch64-apple-darwin.tar.gz", "uv", "tar.gz"),
    ("windows", "x86_64"): ("uv-x86_64-pc-windows-msvc.zip", "uv.exe", "zip"),
    ("windows", "aarch64"): ("uv-aarch64-pc-windows-msvc.zip", "uv.exe", "zip"),
    ("windows", "i686"): ("uv-i686-pc-windows-msvc.zip", "uv.exe", "zip"),
}
UV_MACHINE_ALIASES = {
    "amd64": "x86_64",
    "x86-64": "x86_64",
    "arm64": "aarch64",
    "x64": "x86_64",
    "x86": "i686",
    "i386": "i686",
}


def get_uv_download_info(version=None, system=None, machine=None):
    system = (system or platform.system()).lower()
    if system.startswith("win"):
        system = "windows"
    elif system.startswith("linux"):
        system = "linux"
    elif system.startswith("darwin"):
        system = "darwin"

    machine = (machine or platform.machine()).lower()
    machine = UV_MACHINE_ALIASES.get(machine, machine)

    asset_info = UV_PLATFORM_ASSETS.get((system, machine))
    if asset_info is None:
        raise RuntimeError(
            "Unsupported platform for uv bootstrap: system=%s machine=%s"
            % (system, machine)
        )

    asset_name, executable_name, archive_type = asset_info
    version = version or DEFAULT_UV_VERSION
    return {
        "url": UV_RELEASE_URL.format(version=version, asset_name=asset_name),
        "asset_name": asset_name,
        "executable_name": executable_name,
        "archive_type": archive_type,
    }


def build_uv_sync_cmd(dependencies, skip_packages):
    skip_pkgs = " ".join(
        [f"--no-install-package {dep}" for dep in skip_packages]
    ).strip()
    sync_cmd = "uv sync --frozen --no-dev"
    if skip_pkgs:
        sync_cmd = f"{sync_cmd} {skip_pkgs}"
    return "{sync_cmd} && uv pip install {dependencies} --strict".format(
        sync_cmd=sync_cmd,
        dependencies=" ".join(dependencies),
    )

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
        import tarfile

        uv_install_path = os.path.join(os.getcwd(), "uv_install")
        if which("uv"):
            return
        download_info = get_uv_download_info()
        uv_binary_path = os.path.join(
            uv_install_path, download_info["executable_name"]
        )
        if os.path.exists(uv_binary_path):
            os.environ["PATH"] += os.pathsep + uv_install_path
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

        def _tar_filter(member: tarfile.TarInfo, path):
            if os.path.basename(member.name) != "uv":
                return None  # skip
            member.path = os.path.basename(member.path)
            return member

        max_retries = 3
        for attempt in range(max_retries):
            try:
                req = Request(download_info["url"], headers=headers)
                with urlopen(req) as response:
                    if download_info["archive_type"] == "tar.gz":
                        with tarfile.open(fileobj=response, mode="r:gz") as tar:
                            tar.extractall(uv_install_path, filter=_tar_filter)
                    else:
                        with zipfile.ZipFile(io.BytesIO(response.read())) as archive:
                            extracted = False
                            for member in archive.infolist():
                                if (
                                    os.path.basename(member.filename).lower()
                                    == download_info["executable_name"].lower()
                                ):
                                    with archive.open(member) as src, open(
                                        uv_binary_path, "wb"
                                    ) as dst:
                                        shutil.copyfileobj(src, dst)
                                    extracted = True
                                    break
                            if not extracted:
                                raise RuntimeError(
                                    "Could not find %s in uv archive %s"
                                    % (
                                        download_info["executable_name"],
                                        download_info["asset_name"],
                                    )
                                )
                break
            except (URLError, IOError) as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to download UV after {max_retries} attempts: {e}"
                    )
                time.sleep(2**attempt)

        if os.path.exists(uv_binary_path):
            os.chmod(uv_binary_path, 0o755)

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
        run_cmd(
            build_uv_sync_cmd(
                list(get_dependencies(datastore_type)),
                skip_metaflow_dependencies(),
            )
        )

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
