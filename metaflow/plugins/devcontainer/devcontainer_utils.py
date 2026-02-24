import shlex
from typing import Dict, Optional, Any


def generate_devcontainer_json(
    base_image: str,
    python_dependencies: Optional[Dict[str, str]] = None,
    features: Optional[Dict[str, Any]] = None,
    post_create_command: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generates a devcontainer.json configuration dictionary bridging Metaflow decorator dependencies
    and Devcontainer specs.

    Args:
        base_image: The base Docker image to use (e.g., 'mcr.microsoft.com/devcontainers/python:1-3.11-bullseye').
        python_dependencies: A dictionary of Python packages to install (e.g., {'pandas': '==2.0.0', 'numpy': ''}).
        features: A dictionary of devcontainer features to include.
        post_create_command: A custom command to run after the container is created.

    Returns:
        A dictionary representing the devcontainer.json configuration.
    """
    config: Dict[str, Any] = {
        "image": base_image,
    }

    if features:
        config["features"] = features

    commands = []

    if post_create_command:
        commands.append(post_create_command)

    if python_dependencies:
        # Construct the pip install command based on the provided dependencies
        deps = [
            shlex.quote(
                f"{pkg}{version}"
                if not version or version[0] in ("<", ">", "!", "~", "@", "=")
                else f"{pkg}=={version}"
            )
            for pkg, version in python_dependencies.items()
        ]
        if deps:
            deps_str = " ".join(deps)
            commands.append(f"pip install {deps_str}")

    if commands:
        config["postCreateCommand"] = " && ".join(commands)

    return config
