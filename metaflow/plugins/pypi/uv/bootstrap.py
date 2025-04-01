import os
import subprocess
import sys
from metaflow.plugins import DATASTORES
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

        print("Installing UV")
        cmd = f"""set -e;
            curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL="/uv_install" sh
            """
        run_cmd(cmd)

        uv_path = "/uv_install"
        if os.path.isdir(uv_path):
            os.environ["PATH"] += os.pathsep + uv_path
        else:
            print("UV failed to install.")
            print(os.listdir("/uv_install"))

    def install_uv_project_packages():
        print("Syncing UV project")
        cmd = f"""set -e;
            uv sync;
            uv pip install boto3 requests --strict
            """
        run_cmd(cmd)

    def setup_environment():
        install_uv()
        install_uv_project_packages()

    if len(sys.argv) != 4:
        print("Usage: bootstrap.py <flow_name> <id> <datastore_type>")
        sys.exit(1)

    try:
        _, flow_name, id_, datastore_type = sys.argv

        datastores = [d for d in DATASTORES if d.TYPE == datastore_type]
        if not datastores:
            print(f"No datastore found for type: {datastore_type}")
            sys.exit(1)

        setup_environment()

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
