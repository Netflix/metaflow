from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE
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
        if alt_flow_id_info is None:
            alt_flow_id_info = {}

        # initialize instance variables
        self._flow_name = flow_name
        self._alt_flow_id_info = alt_flow_id_info
        self._template_name = self._flow_name.lower()  # TODO: add logic when adding alt flow IDs
        self._argo_client = ArgoClient(KUBERNETES_NAMESPACE)
        self._cached_status = None
        self._has_triggered = False
        self._metaflow_run = None
        self._argo_run_id = None
        self._metaflow_run_id = None

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

        self._argo_run_id = flow_information["metadata"]["name"]
        self._metaflow_run_id = f"argo-{self._argo_run_id}"
        run_kubernetes_namespace = flow_information["metadata"]["namespace"]

        # Waiting for Argo workflow to trigger run is not optional.
        # It is necessary to determine Flow name if not given as parameter

        print(
            f"Attempting to trigger a run of Metaflow flow {self._flow_name}.\n"
            f"    - Metaflow run id: {self._metaflow_run_id}\n"
            f"    - k8s namespace: {run_kubernetes_namespace}"
        )

        wait_to_trigger = 20  # wait time is 20 seconds
        start_time = time.time()
        while wait_to_trigger > time.time() - start_time:
            if self.has_triggered:
                print(
                    f"Run triggered successfully. Metaflow run id: "
                    f"{self._metaflow_run_id} "
                )
                break
            elif self._error:
                # TODO: add specificity to exceptions (see AIP-6470)
                raise Exception("Error - Unable to trigger run")
            time.sleep(1)
        else:
            # TODO: add specificity to exceptions (see AIP-6470).
            # MetaflowException (or a child exception in file)
            # or TimeoutError would be more specific.
            raise Exception("Failed to begin running")

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
                print("Run timed out.")
                # TODO: add specificity to exceptions (see AIP-6470)
                raise Exception("Wait Timeout")

            success_statement = (
                "successfully!!!" if self.successful else "unsuccessfully."
            )
            print(f"\nRun completed {success_statement}")

        else:
            print("\nNot waiting for run to finish")

    @property
    def _status(self) -> str:
        if self._cached_status in ["Error", "Failed", "Succeeded"]:
            return self._cached_status

        workflow = self._argo_client.get_workflow(self._argo_run_id)
        if workflow.get("status"):
            self._cached_status = workflow["status"].get("phase")

        return self._cached_status

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
        metaflow_run_location = self._flow_name + "/" + self._metaflow_run_id
        seconds_to_find_metaflow_run = 5  # gives 5 seconds to find MF run
        start_time = time.time()
        print("Trying to find metaflow run")
        print(f"metaflow_run_location: {metaflow_run_location}")
        while seconds_to_find_metaflow_run > time.time() - start_time:
            print("Trying to find Metaflow Run")
            try:
                metaflow_run = Run(metaflow_run_location)
                self._metaflow_run = metaflow_run
                print("Found it!")
                break
            except:
                print("could not find metaflow Run")
                time.sleep(1)
        else:
            # TODO: add specificity to exceptions (see AIP-6470).
            # MetaflowException (or a child exception in file)
            # or TimeoutError would be more specific.
            raise Exception("Unable to find Metaflow Run")

    @property
    def exceptions(self) -> dict:
        # if self.is_running:
        #     return None

        if self._metaflow_run is None:
            self._find_metaflow_run()
            if self._metaflow_run is None:
                # TODO: add specificity to exceptions (see AIP-6470)
                raise Exception("Could not find Metaflow run")

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
