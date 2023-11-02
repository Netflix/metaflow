import os
from typing import Optional

import requests
from metaflow.decorators import StepDecorator
from metaflow.metadata.metadata import MetaDatum


def _get_ec2_metadata(path: str) -> Optional[str]:
    response = requests.get(f"http://169.254.169.254/latest/meta-data/{path}")
    return response.text


class interruptibleDecorator(StepDecorator):
    """
    For AIP orchestrator plugin only.

    Step decorator to specify that the pod be can be interrupted (ex: Spot, pod consolidation, etc)

    To use, follow the example below.
    ```
    @interruptible()
    @step
    def train(self):
        self.rank = self.input
        # code running on interruptible instance
        ...
    ```

    Parameters
    ----------
    """

    name = "interruptible"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        # Determine whether this is running on Argo, and hence on AWS
        if os.environ.get("MF_ARGO_WORKFLOW_NAME"):
            meta = {
                "capacity-type": _get_ec2_metadata(
                    "instance-life-cycle"
                ),  # returns: {on-demand, spot}
                "hostname": _get_ec2_metadata("hostname"),
                "instance-type": _get_ec2_metadata("instance-type"),
            }

            entries = [
                MetaDatum(field=k, value=v, type=k, tags=[]) for k, v in meta.items()
            ]
            metadata.register_metadata(run_id, step_name, task_id, entries)
