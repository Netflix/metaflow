import sys
import json
from typing import ClassVar, Optional, List

from metaflow.plugins.aws.step_functions.step_functions import StepFunctions
from metaflow.runner.deployer import DeployedFlow, TriggeredRun

from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo


class StepFunctionsTriggeredRun(TriggeredRun):
    """
    A class representing a triggered AWS Step Functions state machine execution.
    """

    @property
    def status(self) -> Optional[str]:
        """
        Get the status of the triggered execution.

        Returns
        -------
        str, optional
            One of RUNNING, SUCCEEDED, FAILED, TIMED_OUT, ABORTED, or None.
        """
        try:
            from metaflow.plugins.aws.step_functions.step_functions import (
                StepFunctions,
            )
            from metaflow.plugins.aws.step_functions.step_functions_client import (
                StepFunctionsClient,
            )

            _, run_id = self.pathspec.split("/")
            execution_name = run_id[4:]  # strip "sfn-"
            state_machine_name = self.deployer.name

            state_machine = StepFunctionsClient().get(state_machine_name)
            if state_machine is None:
                return None
            sm_arn = state_machine["stateMachineArn"]
            # Execution ARN: replace :stateMachine: with :execution: and append name
            execution_arn = (
                sm_arn.replace(":stateMachine:", ":execution:") + ":" + execution_name
            )
            result = StepFunctionsClient().describe_execution(execution_arn)
            if result is None:
                return None
            return result.get("status")
        except Exception:
            return None

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

    def _run_deployer_command(self, method, return_content=False, **kwargs):
        """Run a deployer CLI command and return the result.

        Parameters
        ----------
        method : str
            CLI subcommand name (e.g. "trigger", "resume", "delete").
        return_content : bool
            If True, read content from the attribute FIFO and return a
            StepFunctionsTriggeredRun on success. If False, return bool.
        **kwargs
            Passed to the CLI subcommand.
        """
        if return_content:
            with temporary_fifo() as (attribute_file_path, attribute_file_fd):
                kwargs["deployer_attribute_file"] = attribute_file_path
                command = getattr(
                    get_lower_level_group(
                        self.deployer.api,
                        self.deployer.top_level_kwargs,
                        self.deployer.TYPE,
                        self.deployer.deployer_kwargs,
                    ),
                    method,
                )(**kwargs)

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
                "Error running %s for %s on %s"
                % (method, self.deployer.flow_file, self.deployer.TYPE)
            )
        else:
            command = getattr(
                get_lower_level_group(
                    self.deployer.api,
                    self.deployer.top_level_kwargs,
                    self.deployer.TYPE,
                    self.deployer.deployer_kwargs,
                ),
                method,
            )(**kwargs)
            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )
            command_obj = self.deployer.spm.get(pid)
            command_obj.sync_wait()
            return command_obj.process.returncode == 0

    @classmethod
    def list_deployed_flows(cls, flow_name: Optional[str] = None):
        """
        This method is not currently implemented for Step Functions.

        Raises
        ------
        NotImplementedError
            This method is not implemented for Step Functions.
        """
        raise NotImplementedError(
            "list_deployed_flows is not implemented for StepFunctions"
        )

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None):
        """
        Retrieves a `StepFunctionsDeployedFlow` object from an identifier and optional
        metadata.

        Parameters
        ----------
        identifier : str
            State machine name for the workflow to retrieve.
        metadata : str, optional, default None
            Optional deployer specific metadata.

        Returns
        -------
        StepFunctionsDeployedFlow
            A `StepFunctionsDeployedFlow` object representing the deployed flow.

        Raises
        ------
        MetaflowException
            If no deployed flow is found for the given identifier.
        """
        import tempfile
        from metaflow.exception import MetaflowException
        from metaflow.runner.deployer import Deployer, generate_fake_flow_file_contents
        from metaflow.client.core import get_metadata
        from metaflow.plugins.aws.step_functions.step_functions_client import (
            StepFunctionsClient,
        )

        workflow = StepFunctionsClient().get(identifier)
        if workflow is None:
            raise MetaflowException("No deployed flow found for: %s" % identifier)

        # Extract flow metadata stored in the start state's Parameters.
        try:
            definition = json.loads(workflow["definition"])
            start = definition["States"]["start"]
            batch_params = start["Parameters"]["Parameters"]
            flow_name = batch_params.get("metaflow.flow_name", "")
            username = batch_params.get("metaflow.owner", "")
        except (KeyError, json.JSONDecodeError):
            raise MetaflowException(
                "Could not extract flow metadata from state machine: %s" % identifier
            )

        # Extract parameter info from the start state's environment variables.
        # METAFLOW_DEFAULT_PARAMETERS is a JSON dict of {param_name: default_value}.
        param_info = {}
        try:
            env_vars = (
                start.get("Parameters", {})
                .get("ContainerOverrides", {})
                .get("Environment", [])
            )
            env_dict = {item.get("Name"): item.get("Value") for item in env_vars}
            default_params_str = env_dict.get("METAFLOW_DEFAULT_PARAMETERS")
            if default_params_str:
                default_params = json.loads(default_params_str)
                for pname, pvalue in default_params.items():
                    # Infer type from the default value
                    if isinstance(pvalue, bool):
                        ptype = "bool"
                    elif isinstance(pvalue, int):
                        ptype = "int"
                    elif isinstance(pvalue, float):
                        ptype = "float"
                    else:
                        ptype = "str"
                    param_info[pname] = {
                        "name": pname,
                        "python_var_name": pname,
                        "type": ptype,
                        "description": "",
                        "is_required": False,
                    }
            # If METAFLOW_PARAMETERS env var is present, there are parameters
            # even if they don't have defaults.  We already captured those with
            # defaults above; required params without defaults would not appear
            # in METAFLOW_DEFAULT_PARAMETERS but the flow still has them.
        except (KeyError, json.JSONDecodeError, TypeError):
            pass  # best-effort extraction; proceed with empty param_info

        fake_flow_file_contents = generate_fake_flow_file_contents(
            flow_name=flow_name, param_info=param_info, project_name=None
        )

        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as fake_flow_file:
            with open(fake_flow_file.name, "w") as fp:
                fp.write(fake_flow_file_contents)

            d = Deployer(
                fake_flow_file.name,
                env={"METAFLOW_USER": username},
            ).step_functions(name=identifier)

            d.name = identifier
            d.flow_name = flow_name
            if metadata is None:
                d.metadata = get_metadata()
            else:
                d.metadata = metadata

        return cls(deployer=d)

    @classmethod
    def get_triggered_run(
        cls, identifier: str, run_id: str, metadata: Optional[str] = None
    ):
        """
        Retrieves a `StepFunctionsTriggeredRun` object from an identifier and run id.

        Parameters
        ----------
        identifier : str
            State machine name for the workflow.
        run_id : str
            Run ID for the triggered run.
        metadata : str, optional, default None
            Optional deployer specific metadata.

        Returns
        -------
        StepFunctionsTriggeredRun
            A `StepFunctionsTriggeredRun` object representing the triggered run.
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
        return self._run_deployer_command("delete", **kwargs)

    def resume(
        self,
        origin_run_id: str,
        step_to_rerun: Optional[str] = None,
        **kwargs,
    ) -> StepFunctionsTriggeredRun:
        """
        Resume a failed or stopped run on AWS Step Functions.

        Successful steps from the origin run will be cloned rather than
        re-executed, unless they are downstream of *step_to_rerun*.

        Parameters
        ----------
        origin_run_id : str
            Run ID of the run to resume (e.g., ``"sfn-<execution-name>"``).
        step_to_rerun : str, optional
            Name of a specific step from which to rerun. All downstream
            steps will also be rerun. If not specified, only steps whose
            origin task was not successful will be rerun.
        **kwargs : Any
            Additional arguments to pass to the resume command,
            `Parameters` in particular.

        Returns
        -------
        StepFunctionsTriggeredRun
            The triggered run instance.
        """
        resume_kwargs = dict(origin_run_id=origin_run_id, **kwargs)
        if step_to_rerun is not None:
            resume_kwargs["step_to_rerun"] = step_to_rerun
        return self._run_deployer_command(
            "resume", return_content=True, **resume_kwargs
        )

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
        """
        return self._run_deployer_command("trigger", return_content=True, **kwargs)
