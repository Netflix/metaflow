import os
import shlex
import subprocess
import tempfile


def test_metaflow_completion_script_smoke():
    """Smoke-test that bash completion loads and returns command candidates."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    completion_script = os.path.join(repo_root, "metaflow-complete.sh")

    # Use a temporary executable named `metaflow` so click reads _METAFLOW_COMPLETE.
    wrapper_code = (
        "#!/usr/bin/env python3\n"
        "import os\n"
        "import sys\n"
        f"sys.path.insert(0, {repr(repo_root)})\n"
        "from metaflow.cmd.main_cli import start\n"
        "start()\n"
    )

    wrapper_path = None
    try:
        fd, wrapper_path = tempfile.mkstemp(prefix="metaflow-cli-", dir=repo_root)
        with os.fdopen(fd, "w") as f:
            f.write(wrapper_code)
        os.chmod(wrapper_path, 0o755)

        bash_script = f"""
set -euo pipefail
source {shlex.quote(completion_script)}
complete -p metaflow >/dev/null
COMP_WORDS=("metaflow" "")
COMP_CWORD=1
_metaflow_completion {shlex.quote(wrapper_path)}
printf "%s\\n" "${{COMPREPLY[@]}}"
"""
        result = subprocess.run(
            ["bash", "-c", bash_script],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONPATH": repo_root},
        )
    finally:
        if wrapper_path is not None:
            try:
                os.remove(wrapper_path)
            except OSError:
                pass

    completions = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    expected_commands = {"configure", "develop", "help", "status", "tutorials", "code"}
    missing = expected_commands - completions
    assert not missing, (
        "metaflow completion output is missing expected commands: "
        f"{sorted(missing)}. Full output: {sorted(completions)}"
    )