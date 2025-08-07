import sys
import subprocess
from pathlib import Path
import sysconfig
import site


def find_makefile():
    possible_dirs = []

    # 1) The standard sysconfig-based location
    data_dir = sysconfig.get_paths()["data"]
    possible_dirs.append(Path(data_dir) / "share" / "metaflow" / "devtools")

    # 2) The user base (e.g. ~/.local on many systems)
    user_base = site.getuserbase()  # e.g. /home/runner/.local
    possible_dirs.append(Path(user_base) / "share" / "metaflow" / "devtools")

    # 3) site-packages can vary, we can guess share/.. near each site-packages
    # (Works if pip actually placed devtools near site-packages.)
    for p in site.getsitepackages():
        possible_dirs.append(Path(p).parent / "share" / "metaflow" / "devtools")
    user_site = site.getusersitepackages()
    possible_dirs.append(Path(user_site).parent / "share" / "metaflow" / "devtools")

    for candidate_dir in possible_dirs:
        makefile_candidate = candidate_dir / "Makefile"
        if makefile_candidate.is_file():
            return makefile_candidate

    # 4) When developing, Metaflow might be installed with --editable, which means the devtools will not be located within site-packages.
    # We read the actual location from package metadata in this case, but only do this heavier operation if the above lookups fail.
    try:
        import json
        from importlib.metadata import Distribution

        direct_url = Distribution.from_name("metaflow").read_text("direct_url.json")
        if direct_url:
            content = json.loads(direct_url)
            url = content.get("url", "")
            if not url.startswith("file://"):
                return None

            makefile_candidate = (
                Path(url.replace("file://", "")) / "devtools" / "Makefile"
            )
            if makefile_candidate.is_file():
                return makefile_candidate
        else:
            # No dist metadata found. This is tied to the version of pip being used
            # Do not bother with .egg-link installs due to the handling of the file contents being a headache due to lack of a unified spec.
            print(
                "Could not locate an installation of Metaflow. No package metadata found."
            )
            print(
                "If Metaflow is installed as editable, try upgrading the version of pip and reinstalling in order to generate proper package metadata.\n"
            )
    except Exception:
        return None

    return None


def main():
    makefile_path = find_makefile()
    if not makefile_path:
        print("ERROR: Could not find executable in any known location.")
        sys.exit(1)
    cmd = ["make", "-f", str(makefile_path)] + sys.argv[1:]

    try:
        completed = subprocess.run(cmd, check=True)
        sys.exit(completed.returncode)
    except subprocess.CalledProcessError as ex:
        sys.exit(ex.returncode)
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting cleanly.")
        sys.exit(1)
