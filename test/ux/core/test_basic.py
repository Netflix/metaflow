import pytest

pytestmark = pytest.mark.basic
from .test_utils import execute_test_flow


def test_hello_world(exec_mode, decospecs, compute_env, tag, scheduler_config):
    run = execute_test_flow(
        flow_name="basic/helloworld.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="hello_world",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"
    assert (
        run["hello"].task.data.message == "Metaflow says: Hi!"
    ), "Hello world message didn't match"


def test_hello_project(exec_mode, decospecs, compute_env, tag, scheduler_config):
    run = execute_test_flow(
        flow_name="basic/helloproject.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="hello_project",
        tl_args_extra={"env": compute_env},
    )

    assert run.successful, "Run was not successful"


@pytest.mark.conda
def test_hello_conda(exec_mode, decospecs, compute_env, tag, scheduler_config):
    run = execute_test_flow(
        flow_name="basic/helloconda.py",
        exec_mode=exec_mode,
        decospecs=decospecs,
        tag=tag,
        scheduler_config=scheduler_config,
        test_name="hello_conda",
        tl_args_extra={
            "environment": "conda",
            "env": compute_env,
        },
    )

    assert run.successful, "Run was not successful"
    assert run["v1"].task.data.lib_version == "2.5.148", "v1 version incorrect"
    assert run["v2"].task.data.lib_version == "2.5.147", "v2 version incorrect"
    assert run["combo"].task.data.lib_version == "2.5.148", "combo version incorrect"
    assert (
        run["combo"].task.data.itsdangerous_version == "2.2.0"
    ), "itsdangerous version incorrect"
