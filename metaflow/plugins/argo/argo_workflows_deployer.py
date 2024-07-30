import sys
import tempfile
from typing import Optional, ClassVar

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.runner.deployer import (
    DeployerImpl,
    DeployedFlow,
    TriggeredRun,
    get_lower_level_group,
    handle_timeout,
)


def suspend(instance: TriggeredRun, **kwargs):
    """
    Suspend the running workflow.

    Parameters
    ----------
    **kwargs : Any
        Additional arguments to pass to the suspend command.

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
    ).suspend(run_id=run_id, **kwargs)

    pid = instance.deployer.spm.run_command(
        [sys.executable, *command],
        env=instance.deployer.env_vars,
        cwd=instance.deployer.cwd,
        show_output=instance.deployer.show_output,
    )

    command_obj = instance.deployer.spm.get(pid)
    return command_obj.process.returncode == 0


def unsuspend(instance: TriggeredRun, **kwargs):
    """
    Unsuspend the suspended workflow.

    Parameters
    ----------
    **kwargs : Any
        Additional arguments to pass to the unsuspend command.

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
    ).unsuspend(run_id=run_id, **kwargs)

    pid = instance.deployer.spm.run_command(
        [sys.executable, *command],
        env=instance.deployer.env_vars,
        cwd=instance.deployer.cwd,
        show_output=instance.deployer.show_output,
    )

    command_obj = instance.deployer.spm.get(pid)
    return command_obj.process.returncode == 0


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


def status(instance: TriggeredRun):
    """
    Get the status of the triggered run.

    Returns
    -------
    str, optional
        The status of the workflow considering the run object, or None if the status could not be retrieved.
    """
    from metaflow.plugins.argo.argo_workflows_cli import (
        get_status_considering_run_object,
    )

    flow_name, run_id = instance.pathspec.split("/")
    name = run_id[5:]
    status = ArgoWorkflows.get_workflow_status(flow_name, name)
    if status is not None:
        return get_status_considering_run_object(status, instance.run)
    return None


def production_token(instance: DeployedFlow):
    """
    Get the production token for the deployed flow.

    Returns
    -------
    str, optional
        The production token, None if it cannot be retrieved.
    """
    try:
        _, production_token = ArgoWorkflows.get_existing_deployment(
            instance.deployer.name
        )
        return production_token
    except TypeError:
        return None


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
    ArgoWorkflowsTriggeredRun
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
        content = handle_timeout(tfp_runner_attribute, command_obj)

        if command_obj.process.returncode == 0:
            triggered_run = TriggeredRun(deployer=instance.deployer, content=content)
            triggered_run._enrich_object(
                {
                    "status": property(status),
                    "terminate": terminate,
                    "suspend": suspend,
                    "unsuspend": unsuspend,
                }
            )
            return triggered_run

    raise Exception(
        "Error triggering %s on %s for %s"
        % (instance.deployer.name, instance.deployer.TYPE, instance.deployer.flow_file)
    )


class ArgoWorkflowsDeployer(DeployerImpl):
    """
    Deployer implementation for Argo Workflows.

    Attributes
    ----------
    TYPE : ClassVar[Optional[str]]
        The type of the deployer, which is "argo-workflows".
    """

    TYPE: ClassVar[Optional[str]] = "argo-workflows"

    def __init__(self, deployer_kwargs, **kwargs):
        """
        Initialize the ArgoWorkflowsDeployer.

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
            }
        )
