from metaflow.plugins.devcontainer.devcontainer_utils import generate_devcontainer_json


def test_generate_basic_devcontainer():
    config = generate_devcontainer_json("python:3.11")
    assert config["image"] == "python:3.11"
    assert "postCreateCommand" not in config


def test_generate_with_python_dependencies():
    deps = {"pandas": "==2.0.0", "numpy": "", "requests": "2.31.0"}
    config = generate_devcontainer_json("python:3.11", python_dependencies=deps)
    assert config["image"] == "python:3.11"
    assert "postCreateCommand" in config
    assert (
        "pip install pandas==2.0.0 numpy requests==2.31.0"
        in config["postCreateCommand"]
    )


def test_generate_with_features():
    features = {"ghcr.io/devcontainers/features/docker-in-docker:2": {}}
    config = generate_devcontainer_json("python:3.11", features=features)
    assert "features" in config
    assert config["features"] == features


def test_generate_with_multiple_commands():
    deps = {"pytest": ""}
    config = generate_devcontainer_json(
        "python:3.11",
        python_dependencies=deps,
        post_create_command="echo 'hello world'",
    )
    assert config["postCreateCommand"] == "echo 'hello world' && pip install pytest"


def test_generate_with_empty_dependencies():
    config = generate_devcontainer_json("python:3.11", python_dependencies={})
    assert config["image"] == "python:3.11"
    assert "postCreateCommand" not in config


def test_generate_with_post_create_command_only():
    config = generate_devcontainer_json("python:3.11", post_create_command="echo 'hi'")
    assert config["image"] == "python:3.11"
    assert config["postCreateCommand"] == "echo 'hi'"
