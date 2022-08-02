from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.metaflow_config import KUBERNETES_NAMESPACE


class TriggeredRun:
    def __init__(
        self,
        flow_name: str = None,
        parameters: dict = None,
        wait_to_trigger: int = 100,  # In minutes (REMEMBER TO CHANGE IT TO MINUTES FOR ACTUAL VERSION b/c testing uses shorter times)
    ):
        """Initializes and triggers a run.
        
        Keyword arguments:
        flow_name -- Metaflow flow name to trigger
        parameters -- information passed in to affect how run is triggered
        wait_to_trigger -- length of time to wait for run to be triggered
        """

        self._flow_name = flow_name
        self._template_name = flow_name.lower()

        if parameters is None:
            parameters = {}
        self._parameters = parameters

        self._argo_client = ArgoClient(KUBERNETES_NAMESPACE)

        self._flow_information = self._argo_client.trigger_workflow_template(
            self._template_name,
            parameters=self._parameters,
        )

        self._argo_run_id = self._flow_information["metadata"]["name"]
        self._metaflow_run_id = f"argo-{self._argo_run_id}"
        self._kubernetes_namespace = self._flow_information["metadata"]["namespace"]
        self._exception = None
        self._status = None

        print(f"Inner flow has started w/ Argo id: {self._argo_run_id}")
