import sys
import subprocess
from pathlib import Path
import sysconfig


def main():
    share_dir = Path(sysconfig.get_paths()["data"]) / "share" / "metaflow" / "devtools"
    makefile_path = share_dir / "Makefile"
    cmd = ["make", "-f", str(makefile_path)] + sys.argv[1:]
    # subprocess.run(cmd, check=True)
    try:
        completed = subprocess.run(cmd, check=True)
        sys.exit(completed.returncode)
    except subprocess.CalledProcessError as ex:
        sys.exit(ex.returncode)
