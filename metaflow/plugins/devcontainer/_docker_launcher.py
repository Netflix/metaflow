"""
Docker SDK launcher for @devcontainer.

Invoked as: python _docker_launcher.py <config_json> <metaflow_cli_args...>

Uses docker-py instead of shelling out to `docker run` to avoid
host environment leakage. The SDK sends a clean env dict directly
to the Docker Engine — no shell in the middle to contaminate it.
"""

import json
import sys
import os
import shlex  # Added for shell injection protection
from metaflow import str_version  # Added for host-container version parity

def main():
    if len(sys.argv) < 3:
        print("Usage: _docker_launcher.py <config_json> <args...>", file=sys.stderr)
        sys.exit(1)

    config = json.loads(sys.argv[1])
    metaflow_cli_args = sys.argv[2:]

    # FIX: Correct variable names and shlex usage
    entrypoint_args = config.get("entrypoint_args", [])
    cmd_parts = ["python3"] + entrypoint_args + metaflow_cli_args
    safe_cmd_parts = [shlex.quote(part) for part in cmd_parts]
    step_cmd = " ".join(safe_cmd_parts)  # Changed from safe_args to safe_cmd_parts

    image = config["image"]
    env_vars = config.get("env", {})
    working_dir = config.get("working_dir", "/work")
    volumes = config.get("volumes", {})

    try:
        import docker

        client = docker.from_env()
    except Exception as e:
        print("ERROR: Docker setup failed: %s" % e, file=sys.stderr)
        sys.exit(1)

    # Pull if missing
    try:
        client.images.get(image)
    except docker.errors.ImageNotFound:
        print("[SANDBOX] Pulling %s..." % image, flush=True)
        client.images.pull(image)

    full_cmd = "pip install -q metaflow==%s && %s" % (str_version, step_cmd)

    try:
        # FIX: Single clean run call with auto_remove
        container = client.containers.run(
            image=image,
            command=["bash", "-c", full_cmd],
            environment=env_vars,
            volumes=volumes,
            working_dir=working_dir,
            user=config.get("user", ""),
            remove=True,  # Using remove=True for automatic cleanup
            detach=True,
            stdout=True,
            stderr=True,
        )

        for chunk in container.logs(stream=True, follow=True):
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()

        result = container.wait()
        sys.exit(result.get("StatusCode", 1))

    except Exception as e:
        print("Error during sandbox execution: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()