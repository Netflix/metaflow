import json
import os
import sys
from metaflow.decorators import StepDecorator
from metaflow.util import get_username


class DevContainerDecorator(StepDecorator):
    """
    Phase 1 & 2 Prototype — @devcontainer step decorator.

    Phase 1: Registration — Metaflow recognizes @devcontainer via plugins/__init__.py.
    Phase 2: CLI Hijack + Spec Parsing — runs the step inside a Docker container
             using the image and env vars from .devcontainer/devcontainer.json.

    Uses the Docker SDK (docker-py) as the backend instead of shelling out to
    `docker run`. This was necessary to fix host environment leakage — the shell
    approach passed the host's full env (VIRTUAL_ENV, PATH, USER, etc.) into the
    container, making it a "dirty boot". The SDK passes only the vars we specify.
    """

    name = "devcontainer"

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        # Phase 2: Parse the devcontainer spec if it exists
        self.spec_path = os.path.join(os.getcwd(), ".devcontainer/devcontainer.json")
        self.image = "python:3.11-slim"
        self.env_vars = {}

        if os.path.exists(self.spec_path):
            with open(self.spec_path, "r") as f:
                spec = json.load(f)
                self.image = spec.get("image", self.image)
                self.env_vars = spec.get("containerEnv", {})

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        # Phase 2: The CLI Hijack.
        # Key learning: runtime_step_cli must modify cli_args IN-PLACE.
        # Returning a value does nothing — Metaflow ignores it.
        # (Discovered via TypeError when CLIArgs was an object, not a str.)

        # Build the container's environment — ONLY these vars will exist inside.
        # This is the fix for the host env leakage problem.
        container_env = self.env_vars.copy()
        container_env["PYTHONPATH"] = "/work"
        container_env["PYTHONUNBUFFERED"] = "x"

        # Metaflow needs a username to function (see metaflow/util.py:get_username)
        username = get_username()
        if username:
            container_env["USERNAME"] = username

        # The entrypoint is like ['/path/to/python', 'hello_sandbox2.py'].
        # We need the flow file (everything after the python binary) to
        # reconstruct the command inside the container.
        original_entrypoint_args = list(cli_args.entrypoint[1:])

        # Mount cwd for code, and HOME so the datastore root path
        # (an absolute host path like ~/.metaflow) is reachable.
        cwd = os.getcwd()
        home_dir = os.path.expanduser("~")
        volumes = {
            cwd: {"bind": "/work", "mode": "rw"},
            home_dir: {"bind": home_dir, "mode": "rw"},
        }
        container_env["HOME"] = home_dir

        config = json.dumps({
            "image": self.image,
            "env": container_env,
            "working_dir": "/work",
            "volumes": volumes,
            "entrypoint_args": original_entrypoint_args,
            "user": "%d:%d" % (os.getuid(), os.getgid()),
        })

        # Swap the entrypoint to our Docker SDK launcher.
        # Metaflow's Worker._launch() will Popen this instead of python directly.
        launcher_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "_docker_launcher.py"
        )
        cli_args.entrypoint = [sys.executable, launcher_path, config]

        print(
            "\n[SANDBOX] Launching %s via Docker SDK (clean boot)...\n" % self.image
        )
