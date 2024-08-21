import sys
import json
import tempfile
from typing import Optional, ClassVar, List

from metaflow.plugins.aws.step_functions.step_functions import StepFunctions
from metaflow.runner.deployer import (
    DeployerImpl,
    DeployedFlow,
    TriggeredRun,
    get_lower_level_group,
    handle_timeout,
)


def terminate(instance: TriggeredRun, **kwargs):
    """
    Terminate the running workflow.

    Parameters
    ----------
    **kwargs : Any
        Additional arguments to pass to the terminate command.

    Returns
    -------
    bool
        True if the command was successful, False otherwise.
    """
    _, run_id = instance.pathspec.split("/")

    # every subclass needs to have `self.deployer_kwargs`
    command = get_lower_level_group(
        instance.deployer.api,
        instance.deployer.top_level_kwargs,
        instance.deployer.TYPE,
        instance.deployer.deployer_kwargs,
    ).terminate(run_id=run_id, **kwargs)

    pid = instance.deployer.spm.run_command(
        [sys.executable, *command],
        env=instance.deployer.env_vars,
        cwd=instance.deployer.cwd,
        show_output=instance.deployer.show_output,
    )

    command_obj = instance.deployer.spm.get(pid)
    return command_obj.process.returncode == 0


def production_token(instance: DeployedFlow):
    """
    Get the production token for the deployed flow.

    Returns
    -------
    str, optional
        The production token, None if it cannot be retrieved.
    """
    try:
        _, production_token = StepFunctions.get_existing_deployment(
            instance.deployer.name
        )
        return production_token
    except TypeError:
        return None


def list_runs(instance: DeployedFlow, states: Optional[List[str]] = None):
    """
    List runs of the deployed flow.

    Parameters
    ----------
    states : Optional[List[str]], optional
        A list of states to filter the runs by. Allowed values are:
        RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED.
        If not provided, all states will be considered.

    Returns
    -------
    List[TriggeredRun]
        A list of TriggeredRun objects representing the runs of the deployed flow.

    Raises
    ------
    ValueError
        If any of the provided states are invalid or if there are duplicate states.
    """
    VALID_STATES = {"RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}

    if states is None:
        states = []

    unique_states = set(states)
    if not unique_states.issubset(VALID_STATES):
        invalid_states = unique_states - VALID_STATES
        raise ValueError(
            f"Invalid states found: {invalid_states}. Valid states are: {VALID_STATES}"
        )

    if len(states) != len(unique_states):
        raise ValueError("Duplicate states are not allowed")

    triggered_runs = []
    executions = StepFunctions.list(instance.deployer.name, states)

    for e in executions:
        run_id = "sfn-%s" % e["name"]
        tr = TriggeredRun(
            deployer=instance.deployer,
            content=json.dumps(
                {
                    "metadata": instance.deployer.metadata,
                    "pathspec": "/".join((instance.deployer.flow_name, run_id)),
                    "name": run_id,
                }
            ),
        )
        tr._enrich_object({"terminate": terminate})
        triggered_runs.append(tr)

    return triggered_runs


def delete(instance: DeployedFlow, **kwargs):
    """
    Delete the deployed flow.

    Parameters
    ----------
    **kwargs : Any
        Additional arguments to pass to the delete command.

    Returns
    -------
    bool
        True if the command was successful, False otherwise.
    """
    command = get_lower_level_group(
        instance.deployer.api,
        instance.deployer.top_level_kwargs,
        instance.deployer.TYPE,
        instance.deployer.deployer_kwargs,
    ).delete(**kwargs)

    pid = instance.deployer.spm.run_command(
        [sys.executable, *command],
        env=instance.deployer.env_vars,
        cwd=instance.deployer.cwd,
        show_output=instance.deployer.show_output,
    )

    command_obj = instance.deployer.spm.get(pid)
    return command_obj.process.returncode == 0


def trigger(instance: DeployedFlow, **kwargs):
    """
    Trigger a new run for the deployed flow.

    Parameters
    ----------
    **kwargs : Any
        Additional arguments to pass to the trigger command, `Parameters` in particular

    Returns
    -------
    StepFunctionsTriggeredRun
        The triggered run instance.

    Raises
    ------
    Exception
        If there is an error during the trigger process.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        tfp_runner_attribute = tempfile.NamedTemporaryFile(dir=temp_dir, delete=False)

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            instance.deployer.api,
            instance.deployer.top_level_kwargs,
            instance.deployer.TYPE,
            instance.deployer.deployer_kwargs,
        ).trigger(deployer_attribute_file=tfp_runner_attribute.name, **kwargs)

        pid = instance.deployer.spm.run_command(
            [sys.executable, *command],
            env=instance.deployer.env_vars,
            cwd=instance.deployer.cwd,
            show_output=instance.deployer.show_output,
        )

        command_obj = instance.deployer.spm.get(pid)
        content = handle_timeout(
            tfp_runner_attribute, command_obj, instance.deployer.file_read_timeout
        )

        if command_obj.process.returncode == 0:
            triggered_run = TriggeredRun(deployer=instance.deployer, content=content)
            triggered_run._enrich_object({"terminate": terminate})
            return triggered_run

    raise Exception(
        "Error triggering %s on %s for %s"
        % (instance.deployer.name, instance.deployer.TYPE, instance.deployer.flow_file)
    )


class StepFunctionsDeployer(DeployerImpl):
    """
    Deployer implementation for AWS Step Functions.

    Attributes
    ----------
    TYPE : ClassVar[Optional[str]]
        The type of the deployer, which is "step-functions".
    """

    TYPE: ClassVar[Optional[str]] = "step-functions"

    def __init__(self, deployer_kwargs, **kwargs):
        """
        Initialize the StepFunctionsDeployer.

        Parameters
        ----------
        deployer_kwargs : dict
            The deployer-specific keyword arguments.
        **kwargs : Any
            Additional arguments to pass to the superclass constructor.
        """
        self.deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    def _enrich_deployed_flow(self, deployed_flow: DeployedFlow):
        """
        Enrich the DeployedFlow object with additional properties and methods.

        Parameters
        ----------
        deployed_flow : DeployedFlow
            The deployed flow object to enrich.
        """
        deployed_flow._enrich_object(
            {
                "production_token": property(production_token),
                "trigger": trigger,
                "delete": delete,
                "list_runs": list_runs,
            }
        )
