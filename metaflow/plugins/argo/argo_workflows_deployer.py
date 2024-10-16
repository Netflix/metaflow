import sys
import tempfile
from typing import Dict, Optional, ClassVar

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.runner.deployer import (
    DeployerImpl,
    DeployedFlow,
    TriggeredRun,
    get_lower_level_group,
    handle_timeout,
)


class ArgoWorkflowsTriggeredRun(TriggeredRun):
    def suspend(self, **kwargs) -> bool:
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
        _, run_id = self.pathspec.split("/")

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).suspend(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0

    def unsuspend(self, **kwargs) -> bool:
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
        _, run_id = self.pathspec.split("/")

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).unsuspend(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0

    def terminate(self, **kwargs) -> bool:
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
        _, run_id = self.pathspec.split("/")

        # every subclass needs to have `self.deployer_kwargs`
        command = get_lower_level_group(
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).terminate(run_id=run_id, **kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0

    @property
    def status(self) -> Optional[str]:
        """
        Get the status of the triggered run.

        Returns
        -------
        str, optional
            The status of the workflow considering the run object, or None if
            the status could not be retrieved.
        """
        from metaflow.plugins.argo.argo_workflows_cli import (
            get_status_considering_run_object,
        )

        flow_name, run_id = self.pathspec.split("/")
        name = run_id[5:]
        status = ArgoWorkflows.get_workflow_status(flow_name, name)
        if status is not None:
            return get_status_considering_run_object(status, self.run)
        return None


class ArgoWorkflowsDeployedFlow(DeployedFlow):

    @property
    def production_token(self) -> Optional[str]:
        """
        Get the production token for the deployed flow.

        Returns
        -------
        str, optional
            The production token, None if it cannot be retrieved.
        """
        try:
            _, production_token = ArgoWorkflows.get_existing_deployment(
                self.deployer.name
            )
            return production_token
        except TypeError:
            return None

    def delete(self, **kwargs) -> bool:
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
            self.deployer.api,
            self.deployer.top_level_kwargs,
            self.deployer.TYPE,
            self.deployer.deployer_kwargs,
        ).delete(**kwargs)

        pid = self.deployer.spm.run_command(
            [sys.executable, *command],
            env=self.deployer.env_vars,
            cwd=self.deployer.cwd,
            show_output=self.deployer.show_output,
        )

        command_obj = self.deployer.spm.get(pid)
        return command_obj.process.returncode == 0

    def trigger(self, **kwargs) -> ArgoWorkflowsTriggeredRun:
        """
        Trigger a new run for the deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments to pass to the trigger command, `Parameters` in
            particular

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
            tfp_runner_attribute = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False
            )

            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(deployer_attribute_file=tfp_runner_attribute.name, **kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                tfp_runner_attribute, command_obj, self.deployer.file_read_timeout
            )

            if command_obj.process.returncode == 0:
                return ArgoWorkflowsTriggeredRun(
                    deployer=self.deployer, content=content
                )

        raise Exception(
            "Error triggering %s on %s for %s"
            % (
                self.deployer.name,
                self.deployer.TYPE,
                self.deployer.flow_file,
            )
        )


class ArgoWorkflowsDeployer(DeployerImpl):
    """
    Deployer implementation for Argo Workflows.
    """

    TYPE: ClassVar[Optional[str]] = "argo-workflows"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs):
        """
        Initialize the ArgoWorkflowsDeployer.

        Parameters
        ----------
        deployer_kwargs : Dict[str, str]
            The deployer-specific keyword arguments.
        **kwargs : Any
            Additional arguments to pass to the superclass constructor.
        """
        self.deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    def create(self, **kwargs) -> ArgoWorkflowsDeployedFlow:
        return self._create(ArgoWorkflowsDeployedFlow, **kwargs)
