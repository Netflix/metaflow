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


def main():
    if len(sys.argv) < 3:
        print("Usage: _docker_launcher.py <config_json> <args...>", file=sys.stderr)
        sys.exit(1)

    config = json.loads(sys.argv[1])
    metaflow_cli_args = sys.argv[2:]

    # Reconstruct the command: python3 <flow_file> <metaflow_args...>
    entrypoint_args = config.get("entrypoint_args", [])
    cmd_parts = ["python3"] + entrypoint_args + metaflow_cli_args

    image = config["image"]
    env_vars = config.get("env", {})
    working_dir = config.get("working_dir", "/work")
    volumes = config.get("volumes", {})

    try:
        import docker
    except ImportError:
        print("ERROR: pip install docker", file=sys.stderr)
        sys.exit(1)

    try:
        client = docker.from_env()
    except docker.errors.DockerException as e:
        print("ERROR: Docker daemon not reachable: %s" % e, file=sys.stderr)
        sys.exit(1)

    # Pull if missing
    try:
        client.images.get(image)
    except docker.errors.ImageNotFound:
        print("[SANDBOX] Pulling %s..." % image, flush=True)
        client.images.pull(image)

    # Install metaflow from PyPI inside the container so the step can run.
    # We don't need the local source (with @devcontainer) — that plugin
    # already did its job on the host side. The container just runs the step.
    step_cmd = " ".join(cmd_parts)
    full_cmd = "pip install -q metaflow && %s" % step_cmd

    print("[SANDBOX] image: %s" % image, flush=True)
    print("[SANDBOX] env keys: %s" % list(env_vars.keys()), flush=True)
    print("[SANDBOX] cmd: %s" % step_cmd, flush=True)

    try:
        # Docker SDK call — environment= takes ONLY our clean dict.
        # This is the whole point: no host env leaks in.
        container = client.containers.run(
            image=image,
            command=["bash", "-c", full_cmd],
            environment=env_vars,
            volumes=volumes,
            working_dir=working_dir,
            user=config.get("user", ""),
            remove=False,
            detach=True,
            stdout=True,
            stderr=True,
        )

        # Stream logs so Metaflow can capture them
        for chunk in container.logs(stream=True, follow=True):
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()

        result = container.wait()
        exit_code = result.get("StatusCode", 1)

        try:
            container.remove()
        except Exception:
            pass

        sys.exit(exit_code)

    except docker.errors.ContainerError as e:
        print("Container error: %s" % e, file=sys.stderr)
        sys.exit(1)
    except docker.errors.APIError as e:
        print("Docker API error: %s" % e, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print("Error: %s" % e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
