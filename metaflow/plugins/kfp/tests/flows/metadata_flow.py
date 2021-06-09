from metaflow import FlowSpec, step, current, Step

import os
from random import random


class MetadataFlow(FlowSpec):
    start_message = "MetadataFlow is starting."

    @step
    def start(self):
        print(self.start_message)
        self.x = random()
        print("self.x", self.x)
        self.next(self.end)

    @step
    def end(self):
        start_step: Step = Step(f"{current.flow_name}/{current.run_id}/start")
        print("start_step", start_step)
        x = start_step.task.data.x
        print("self.x=", self.x)
        print("Metaflow client start_step.task.data.x=", x)
        assert x == self.x

        logs = start_step.task.stdout
        print()
        print(">>>> start step logs")
        print(logs)
        print("<<<<")
        print()
        assert self.start_message in logs

        start_step_tags: frozenset = start_step.tags
        print("start_step_tags", start_step_tags)
        assert "test_t1" in start_step_tags
        assert "metaflow_test" in start_step_tags
        assert f"zodiac_service:{os.environ['ZODIAC_SERVICE']}" in start_step_tags
        assert f"zodiac_team:{os.environ['ZODIAC_TEAM']}" in start_step_tags
        assert f"argo_workflow:{os.environ['MF_ARGO_WORKFLOW_NAME']}" in start_step_tags
        assert f"pod_namespace:{os.environ['MF_POD_NAMESPACE']}" in start_step_tags

        print("MetadataFlow is all done.")


if __name__ == "__main__":
    MetadataFlow()
