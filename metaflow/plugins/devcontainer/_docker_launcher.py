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

    # Load configuration from the decorator
    config = json.loads(sys.argv[1])
    metaflow_cli_args = sys.argv[2:]

    # Step 1: Secure Command Reconstruction
    # We use shlex.quote to ensure spaces/semicolons in paths or args
    # are treated as literal text, preventing shell injection.
    entrypoint_args = config.get("entrypoint_args", [])
    cmd_parts = ["python3"] + entrypoint_args + metaflow_cli_args
    safe_cmd_parts = [shlex.quote(part) for part in cmd_parts]
    step_cmd = " ".join(safe_args)

    image = config["image"]
    env_vars = config.get("env", {})
    working_dir = config.get("working_dir", "/work")
    volumes = config.get("volumes", {})

    try:
        import docker
    except ImportError:
        print(
            "ERROR: docker-py not found. Please run: pip install docker",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        client = docker.from_env()
    except Exception as e:
        print("ERROR: Docker daemon not reachable: %s" % e, file=sys.stderr)
        sys.exit(1)

    # Pull image if not present locally
    try:
        client.images.get(image)
    except docker.errors.ImageNotFound:
        print("[SANDBOX] Pulling %s..." % image, flush=True)
        client.images.pull(image)

    # Step 2: Version Pinning
    # We install the EXACT version of Metaflow running on the host
    # to avoid serialization errors or behavior drift.
    full_cmd = "pip install -q metaflow==%s && %s" % (str_version, step_cmd)

    print("[SANDBOX] image: %s" % image, flush=True)
    print("[SANDBOX] Metaflow version pinned to: %s" % str_version, flush=True)
    print("[SANDBOX] cmd: %s" % step_cmd, flush=True)

    try:
        # Step 3: Reliable Execution & Orphan Prevention
        # auto_remove=True ensures the container is deleted by the Docker Engine
        # even if this launcher script is killed unexpectedly.
        container = client.containers.run(
            image=image,
            command=["bash", "-c", full_cmd],
            environment=env_vars,
            volumes=volumes,
            working_dir=working_dir,
            user=config.get("user", ""),
            auto_remove=True,  # Replaces manual container.remove()
            detach=True,
            stdout=True,
            stderr=True,
        )

        # Stream logs in real-time
        for chunk in container.logs(stream=True, follow=True):
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()

        result = container.wait()
        exit_code = result.get("StatusCode", 1)
        sys.exit(exit_code)

    except Exception as e:
        print("Error during sandbox execution: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()