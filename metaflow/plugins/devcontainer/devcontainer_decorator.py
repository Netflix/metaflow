import json
import os
import sys
from metaflow.decorators import StepDecorator
from metaflow.util import get_username


class DevContainerDecorator(StepDecorator):
    name = "devcontainer"

    # Allows user override: @devcontainer(config="custom/spec.json")
    defaults = {"config": None}

    def _find_spec(self, start_path):
        """Recursively searches upward for .devcontainer/devcontainer.json."""
        curr = os.path.abspath(start_path)
        while curr != os.path.dirname(curr):
            potential_path = os.path.join(curr, ".devcontainer", "devcontainer.json")
            if os.path.exists(potential_path):
                return potential_path
            curr = os.path.dirname(curr)
        return None

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        # Fix 1: Robust Path Discovery (No more os.getcwd() fragility)
        # We start looking from the directory where the flow file actually lives.
        flow_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        custom_path = self.attributes.get("config")

        if custom_path:
            self.spec_path = os.path.abspath(os.path.join(flow_dir, custom_path))
        else:
            self.spec_path = self._find_spec(flow_dir)

        self.image = "python:3.12-slim"  # Updated default
        self.env_vars = {}

        if self.spec_path and os.path.exists(self.spec_path):
            with open(self.spec_path, "r") as f:
                try:
                    # Note: Future milestones will add JSONC (comments) support
                    spec = json.load(f)
                    self.image = spec.get("image", self.image)
                    self.env_vars = spec.get("containerEnv", {})
                except json.JSONDecodeError:
                    print(f"[SANDBOX] Warning: Could not parse {self.spec_path}")

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        container_env = self.env_vars.copy()
        container_env["PYTHONPATH"] = "/work"
        container_env["PYTHONUNBUFFERED"] = "x"

        username = get_username()
        if username:
            container_env["USERNAME"] = username

        original_entrypoint_args = list(cli_args.entrypoint[1:])

        # Fix 2: Principle of Least Privilege (Targeted Mounting)
        # We no longer mount the entire home_dir. We only mount the .metaflow dir
        # so the local datastore works without exposing SSH/AWS keys.
        cwd = os.getcwd()
        home_dir = os.path.expanduser("~")
        metaflow_home = os.path.join(home_dir, ".metaflow")

        # Ensure .metaflow exists on host so Docker doesn't create it as root
        os.makedirs(metaflow_home, exist_ok=True)

        volumes = {
            cwd: {"bind": "/work", "mode": "rw"},
            metaflow_home: {"bind": metaflow_home, "mode": "rw"},
        }
        container_env["HOME"] = home_dir


        # Fix 3: Windows/WSL2 Compatibility Guard
        # os.getuid/gid don't exist on native Windows.
        user_spec = ""
        if hasattr(os, "getuid"):
            user_spec = "%d:%d" % (os.getuid(), os.getgid())

        config = json.dumps(
            {
                "image": self.image,
                "env": container_env,
                "working_dir": "/work",
                "volumes": volumes,
                "entrypoint_args": original_entrypoint_args,
                "user": user_spec,
            }
        )
        config = json.dumps({
            "image": self.image,
            "env": container_env,
            "working_dir": "/work",
            "volumes": volumes,
            "entrypoint_args": original_entrypoint_args,
            "user": "%d:%d" % (os.getuid(), os.getgid()) if hasattr(os, "getuid") else "",
        })

        launcher_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "_docker_launcher.py"
        )
        cli_args.entrypoint = [sys.executable, launcher_path, config]

        print(
            "\n[SANDBOX] Launching %s with Hardened Security Policies...\n" % self.image
        )
