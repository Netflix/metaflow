from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
from metaflow.exception import MetaflowNotFound, MetaflowException
import time


class ArgoLiveRun:  # TODO: make child class of LiveRun
    """
    This class takes in information to identify and trigger a run of a
    Metaflow flow using the Argo plugin. The run is initialized without
    triggering a run, but a run should be triggered immediately after
    initialization. This class should be initialized only with the trigger_run
    function. The flow to trigger is identified based on the parameter for the
    Metaflow flow name. Users may use additional parameters to alter the run,
    and wait for the run to finish.

    The object allows users to access information relating to the triggered
    run, including booleans for whether the run has been triggered, is running,
    and was successful, as well as for failed steps, and exceptions.
    """

    def __init__(
        self,
        flow_name: str = None,
        alt_flow_id_info: dict = None,
    ):
        """
        Initialize an ArgoLiveRun object without triggering flow.

        Parameters
        ----------
        flow_name: str
            Name of Metaflow flow to trigger run
        alt_flow_id_info: dict
            Alternative identifying information of flow to trigger run
        """
        # confirm that a flow_name or template_name exists w/o conflict
        if alt_flow_id_info is None:
            alt_flow_id_info = {}

        template_name = None
        if 'template_name' in alt_flow_id_info:
            template_name = alt_flow_id_info['template_name']

        if flow_name is None and template_name is None:
            raise ValueError("no Metaflow flow or Argo template specified")

        if flow_name and template_name and flow_name.lower() != template_name:
            raise ValueError("mismatching Metaflow flow and Argo template names")

        # initialize instance variables
        self._flow_name = flow_name
        self._template_name = template_name
        if template_name is None:
            self._template_name = flow_name.lower()
        self._argo_client = ArgoClient(KUBERNETES_NAMESPACE)
        self._cached_status = None
        self._has_triggered = False
        self._metaflow_run = None
        self._argo_run_id = None
        self._metaflow_run_id = None
        self._time_triggered = None
        self._error_message = None

    def trigger(
        self,
        parameters: dict = None,
        wait: bool = True,
        wait_timeout: int = 30,  # in minutes TODO: determine proper timeout
    ) -> None:
        """
        Trigger flow.

        Parameters
        ----------
        parameters: dict
            The information passed in to affect how run is triggered
        wait: bool
            whether function waits for triggered run to finish before returning
        wait_timeout: int
            time (in minutes) to wait for run to finish
        """

        if parameters is None:
            parameters = {}

        print(f"template name: {self._template_name}")

        # trigger run and retrieve id info
        flow_information = self._argo_client.trigger_workflow_template(
            self._template_name,
            parameters=parameters,
        )

        self._time_triggered = time.time()
        self._argo_run_id = flow_information["metadata"]["name"]
        self._metaflow_run_id = f"argo-{self._argo_run_id}"
        run_kubernetes_namespace = flow_information["metadata"]["namespace"]

        # Waiting for Argo workflow to trigger run is not optional.
        # It is necessary to determine Flow name if not given as parameter

        print(
            f"Attempting to trigger a run of a Metaflow flow:\n"
            f"    - Metaflow run id: {self._metaflow_run_id}\n"
            f"    - k8s namespace: {run_kubernetes_namespace}"
        )

        wait_to_trigger = 20  # wait time is 20 seconds
        start_time = time.time()
        while wait_to_trigger > time.time() - start_time:
            if self.has_triggered:
                print(
                    f"Run triggered successfully.\n"
                    f"    - Metaflow flow:   {self.flow_name}\n"
                    f"    - Metaflow run id: {self._metaflow_run_id}\n"
                )
                break
            elif self._error:
                raise MetaflowException(
                    "ArgoClient returned 'Error' status. Potentially caused if"
                    " Metaflow flow does not exist or if Argo workflow"
                    " template has not yet been created."
                    f"\nError message: {self._error_message}"
                )
            time.sleep(1)
        else:
            raise TimeoutError(f"Failed to trigger Argo workflow within {wait_to_trigger} seconds")

        # optional wait for argo workflow to finish running
        if wait:
            print("Now we will wait for the flow to finish")

            start_time = time.time()
            loop_counter = 0
            while wait_timeout * 60 > time.time() - start_time:
                if not self.is_running:
                    break
                if loop_counter % 12 == 0:
                    print(
                        f"Time waited: {int((time.time() - start_time)/60)}"
                        f" minutes out of a possible {wait_timeout}"
                    )
                loop_counter += 1
                time.sleep(5)
            else:
                raise TimeoutError(f"Failed to begin running within {wait_timeout} minutes")

            success_statement = (
                "successfully!!!" if self.successful else "unsuccessfully."
            )
            print(f"\nRun completed {success_statement}")

        else:
            print("\nNot waiting for run to finish")

    @property
    def flow_name(self) -> str:
        if self._flow_name is None:
            workflow = self._argo_client.get_workflow(self._argo_run_id)
            self._flow_name = workflow['metadata']['annotations']['metaflow/flow_name']
        return self._flow_name

    @property
    def _status(self) -> str:
        if self._cached_status in ["Error", "Failed", "Succeeded"]:
            return self._cached_status

        workflow = self._argo_client.get_workflow(self._argo_run_id)
        if workflow.get("status"):
            self._cached_status = workflow["status"].get("phase")
            if self._cached_status == 'Error':
                self._error_message = workflow["status"].get("message")

        return self._cached_status

    def _print_status(self):
        print(f"""Current run properties:
        Has Triggered:    {self.has_triggered}
        Is Running:       {self.is_running}
        Successful:       {self.successful}
        Failed Steps:     {self.failed_steps}
        Exceptions:       {self.exceptions}
        """)

    @property
    def has_triggered(self) -> bool:
        if self._has_triggered:
            return True

        elif self._status and self._status != "Error":
            self._has_triggered = True

        return self._has_triggered

    @property
    def is_running(self) -> bool:
        return self._status == "Running"

    @property
    def _error(self) -> bool:
        return self._status == "Error"

    @property
    def successful(self) -> bool:
        return self._status == "Succeeded"

    @property
    def failed_steps(self) -> list:
        if self.is_running or self.successful:
            return []

        failed_steps = []

        workflow = self._argo_client.get_workflow(self._argo_run_id)
        nodes_info = workflow["status"]["nodes"]
        for node in nodes_info:
            if (
                nodes_info[node]["phase"] == "Failed"
                and nodes_info[node]["type"] == "Pod"
            ):
                failed_steps.append(nodes_info[node]["templateName"])

        return failed_steps

    def _find_metaflow_run(self) -> None:
        from metaflow import Run, namespace

        namespace(None)
        metaflow_run_location = self.flow_name + "/" + self._metaflow_run_id
        print(f"Trying to find Metaflow run @ location: {metaflow_run_location}")
        try:
            metaflow_run = Run(metaflow_run_location)
            self._metaflow_run = metaflow_run
            print("Found Metaflow run!")
        except MetaflowNotFound:
            print("Failed to find Metaflow run")
            if time.time() - self._time_triggered < 60:
                print("Not raising exception because within 60 seconds of triggering")
            else:
                raise MetaflowNotFound(
                    f"Unable to find Metaflow run at location: {metaflow_run_location}"
                )

    @property
    def exceptions(self) -> dict:

        if self._metaflow_run is None:
            self._find_metaflow_run()
            if self._metaflow_run is None:
                return {}  # only if Flow not found within 60s of triggering

        exceptions = {}

        for step in self._metaflow_run.steps():
            for task in step.tasks():
                if task.exception is not None:
                    if str(step) not in exceptions:
                        exceptions[str(step)] = [
                            {
                                "step": step,
                                "exceptions": [
                                    {"task": task, "exception": task.exception}
                                ],
                            }
                        ]
                    else:
                        exceptions[str(step)]["exceptions"].append(
                            {"task": task, "exception": task.exception}
                        )

        return exceptions
