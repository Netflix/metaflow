import os
import subprocess
import sys

from metaflow.util import which

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
        if which("uv"):
            return

        print("Installing uv...")
        uv_version = "0.1.8"
        cmd = f"""set -e;
            curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/uv_install" UV_VERSION="{uv_version}" sh
            """
        run_cmd(cmd)

        uv_path = "/uv_install"
        if os.path.isdir(uv_path):
            os.environ["PATH"] += os.pathsep + uv_path
        else:
            print("uv failed to install.")
            print(os.listdir("/uv_install"))

    def get_dependencies(datastore_type):
        datastore_packages = {
            "s3": ["boto3"],
            "azure": [
                "azure-identity",
                "azure-storage-blob",
                "azure-keyvault-secrets",
                "simple-azure-blob-downloader",
            ],
            "gs": [
                "google-cloud-storage",
                "google-auth",
                "simple-gcp-object-downloader",
                "google-cloud-secret-manager",
            ],
        }

        if datastore_type not in datastore_packages:
            raise NotImplementedError(
                "Unknown datastore type: {}".format(datastore_type)
            )

        return datastore_packages[datastore_type] + ["requests"]

    def sync_uv_project(datastore_type):
        print("Syncing uv project...")
        dependencies = " ".join(get_dependencies(datastore_type))
        cmd = f"""set -e;
            ls -la;
            uv sync --frozen --no-install-package metaflow;
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
