import os
import sys
import json
import tempfile
from typing import ClassVar, Optional, List

from metaflow.client.core import get_metadata
from metaflow.exception import MetaflowException
from metaflow.plugins.aws.step_functions.step_functions import StepFunctions
from metaflow.runner.deployer import (
    Deployer,
    DeployedFlow,
    TriggeredRun,
    generate_fake_flow_file_contents,
)

from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo


class StepFunctionsTriggeredRun(TriggeredRun):
    """
    A class representing a triggered AWS Step Functions state machine execution.
    """

    def terminate(self, **kwargs) -> bool:
        """
        Terminate the running state machine execution.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the termination with a production token.

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
        command_obj.sync_wait()
        return command_obj.process.returncode == 0


class StepFunctionsDeployedFlow(DeployedFlow):
    """
    A class representing a deployed AWS Step Functions state machine.
    """

    TYPE: ClassVar[Optional[str]] = "step-functions"

    @classmethod
    def list_deployed_flows(cls, flow_name: Optional[str] = None):
        """
        List all deployed AWS Step Functions state machines.

        Parameters
        ----------
        flow_name : str, optional, default None
            If specified, only list deployed flows for this specific flow name.
            If None, list all deployed flows.

        Yields
        ------
        StepFunctionsDeployedFlow
            `StepFunctionsDeployedFlow` objects representing deployed
            state machines on AWS Step Functions.
        """
        for template_name in StepFunctions.list_templates(flow_name=flow_name):
            try:
                deployed_flow = cls.from_deployment(template_name)
                yield deployed_flow
            except Exception:
                # Skip state machines that can't be converted to DeployedFlow objects
                continue

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None):
        """
        Retrieves a `StepFunctionsDeployedFlow` object from an identifier and optional
        metadata.

        Parameters
        ----------
        identifier : str
            The state machine name for the deployment to retrieve.
        metadata : str, optional, default None
            Optional deployer specific metadata.

        Returns
        -------
        StepFunctionsDeployedFlow
            A `StepFunctionsDeployedFlow` object representing the
            deployed flow on AWS Step Functions.
        """
        deployment_metadata = StepFunctions.get_deployment_metadata(identifier)

        if deployment_metadata is None:
            raise MetaflowException("No deployed flow found for: %s" % identifier)

        flow_name, username, _ = deployment_metadata

        fake_flow_file_contents = generate_fake_flow_file_contents(
            flow_name=flow_name, param_info={}
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as fake_flow_file:
            tmp_path = fake_flow_file.name

        try:
            with open(tmp_path, "w") as fp:
                fp.write(fake_flow_file_contents)

            d = Deployer(
                tmp_path, env={"METAFLOW_USER": username}
            ).step_functions(name=identifier)

            d.name = identifier
            d.flow_name = flow_name
            if metadata is None:
                d.metadata = get_metadata()
            else:
                d.metadata = metadata
        finally:
            os.unlink(tmp_path)

        return cls(deployer=d)

    @classmethod
    def get_triggered_run(
        cls, identifier: str, run_id: str, metadata: Optional[str] = None
    ):
        """
        Retrieves a `StepFunctionsTriggeredRun` object from an identifier, a run id and
        optional metadata.

        Parameters
        ----------
        identifier : str
            The state machine name for the deployment to retrieve.
        run_id : str
            Run ID for which to fetch the triggered run object.
        metadata : str, optional, default None
            Optional deployer specific metadata.

        Returns
        -------
        StepFunctionsTriggeredRun
            A `StepFunctionsTriggeredRun` object representing the
            triggered run on AWS Step Functions.
        """
        deployed_flow_obj = cls.from_deployment(identifier, metadata)
        return StepFunctionsTriggeredRun(
            deployer=deployed_flow_obj.deployer,
            content=json.dumps(
                {
                    "metadata": deployed_flow_obj.deployer.metadata,
                    "pathspec": "/".join(
                        (deployed_flow_obj.deployer.flow_name, run_id)
                    ),
                    "name": run_id,
                }
            ),
        )

    @property
    def production_token(self: DeployedFlow) -> Optional[str]:
        """
        Get the production token for the deployed flow.

        Returns
        -------
        str, optional
            The production token, None if it cannot be retrieved.
        """
        try:
            _, production_token = StepFunctions.get_existing_deployment(
                self.deployer.name
            )
            return production_token
        except TypeError:
            return None

    def list_runs(
        self, states: Optional[List[str]] = None
    ) -> List[StepFunctionsTriggeredRun]:
        """
        List runs of the deployed flow.

        Parameters
        ----------
        states : List[str], optional, default None
            A list of states to filter the runs by. Allowed values are:
            RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED.
            If not provided, all states will be considered.

        Returns
        -------
        List[StepFunctionsTriggeredRun]
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
        executions = StepFunctions.list(self.deployer.name, states)

        for e in executions:
            run_id = "sfn-%s" % e["name"]
            tr = StepFunctionsTriggeredRun(
                deployer=self.deployer,
                content=json.dumps(
                    {
                        "metadata": self.deployer.metadata,
                        "pathspec": "/".join((self.deployer.flow_name, run_id)),
                        "name": run_id,
                    }
                ),
            )
            triggered_runs.append(tr)

        return triggered_runs

    def delete(self, **kwargs) -> bool:
        """
        Delete the deployed state machine.

        Parameters
        ----------
        authorize : str, optional, default None
            Authorize the deletion with a production token.

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
        command_obj.sync_wait()
        return command_obj.process.returncode == 0

    def trigger(self, **kwargs) -> StepFunctionsTriggeredRun:
        """
        Trigger a new run for the deployed flow.

        Parameters
        ----------
        **kwargs : Any
            Additional arguments to pass to the trigger command,
            `Parameters` in particular

        Returns
        -------
        StepFunctionsTriggeredRun
            The triggered run instance.

        Raises
        ------
        Exception
            If there is an error during the trigger process.
        """
        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            # every subclass needs to have `self.deployer_kwargs`
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(deployer_attribute_file=attribute_file_path, **kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )

            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return StepFunctionsTriggeredRun(
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
